package sync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wangdong58/FastSync/internal/config"
	"github.com/wangdong58/FastSync/internal/database"
	"github.com/wangdong58/FastSync/internal/logger"
	"github.com/wangdong58/FastSync/internal/monitor"
	"github.com/wangdong58/FastSync/internal/schema"
)

// Engine 同步引擎
type Engine struct {
	config   *config.Config
	sourceDB *database.Connection
	targetDB *database.Connection
	stats    *monitor.SyncStats
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewEngine 创建同步引擎
func NewEngine(cfg *config.Config, sourceDB, targetDB *database.Connection) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		config:   cfg,
		sourceDB: sourceDB,
		targetDB: targetDB,
		stats:    monitor.NewSyncStats(),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Run 运行同步
func (e *Engine) Run() error {
	e.stats.Start()

	// 启动监控报告
	if e.config.Monitor.ReportInterval != "" {
		go e.stats.StartReporting(e.config.GetReportInterval())
	}

	// 获取要同步的表列表
	tables := e.config.Tables
	if len(tables) == 0 {
		// 自动发现所有表
		discovery := schema.NewSchemaDiscovery(e.sourceDB.DB)
		allTables, err := discovery.DiscoverAllTables()
		if err != nil {
			return fmt.Errorf("discover tables failed: %w", err)
		}
		for _, t := range allTables {
			tables = append(tables, config.TableConfig{
				Source:             t,
				Target:             strings.ToLower(strings.ReplaceAll(t, "dbo.", "")),
				TruncateBeforeSync: true,
			})
		}
	}

	e.stats.TotalTables = len(tables)

	// 判断是否使用多表并发
	tableWorkers := e.config.Sync.TableWorkers
	if tableWorkers <= 1 {
		// 串行同步
		return e.runSequential(tables)
	}

	// 并发同步多张表
	return e.runConcurrent(tables, tableWorkers)
}

// runSequential 串行同步多张表
func (e *Engine) runSequential(tables []config.TableConfig) error {
	for i, table := range tables {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		default:
		}

		if err := e.syncTable(table, i+1, len(tables)); err != nil {
			return fmt.Errorf("sync table %s failed: %w", table.Source, err)
		}

		atomic.AddInt64(&e.stats.CompletedTables, 1)
	}

	e.stats.Stop()
	return nil
}

// runConcurrent 并发同步多张表
func (e *Engine) runConcurrent(tables []config.TableConfig, tableWorkers int) error {
	logger.Info("\nStarting concurrent sync with %d table workers", tableWorkers)

	// 预计算所有表的总行数
	logger.Info("Calculating total rows for all tables...")
	var totalRowsAll int64
	for _, table := range tables {
		rows, err := database.GetTableRowCount(e.sourceDB.DB, table.Source)
		if err != nil {
			logger.Warn("Failed to get row count for %s: %v", table.Source, err)
			continue
		}
		totalRowsAll += rows
	}
	atomic.StoreInt64(&e.stats.TotalRows, totalRowsAll)
	logger.Info("Total rows to sync: %s", monitor.FormatNumber(totalRowsAll))

	// 创建工作池
	var wg sync.WaitGroup
	tableChan := make(chan config.TableConfig, len(tables))
	errors := make(chan error, len(tables))

	// 使用原子计数器跟踪表索引
	tableIndex := int32(0)
	totalTables := len(tables)

	// 启动表同步workers
	for i := 0; i < tableWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("Table worker %d panic: %v", workerID, r)
				}
				wg.Done()
			}()
			for table := range tableChan {
				select {
				case <-e.ctx.Done():
					return
				default:
				}

				// 原子递增表索引
				idx := atomic.AddInt32(&tableIndex, 1)

				logger.Info("[%d/%d] Syncing table: %s -> %s", idx, totalTables, table.Source, table.Target)

				if err := e.syncTable(table, int(idx), totalTables); err != nil {
					errors <- fmt.Errorf("worker %d: sync table %s failed: %w", workerID, table.Source, err)
				} else {
					completed := atomic.LoadInt64(&e.stats.CompletedTables)
					logger.Info("✓ Table %s completed (%d/%d)", table.Source, completed, totalTables)
				}
			}
		}(i)
	}

	// 分发表任务
	go func() {
		for _, table := range tables {
			tableChan <- table
		}
		close(tableChan)
	}()

	// 等待所有worker完成
	wg.Wait()
	close(errors)

	// 收集错误
	var errList []error
	for err := range errors {
		errList = append(errList, err)
	}

	e.stats.Stop()

	if len(errList) > 0 {
		return fmt.Errorf("%d tables failed to sync: %v", len(errList), errList[0])
	}
	return nil
}

// Stop 停止同步
func (e *Engine) Stop() {
	e.cancel()
}

// GetStats 获取统计信息
func (e *Engine) GetStats() *monitor.SyncStats {
	return e.stats
}

// syncTable 同步单个表
func (e *Engine) syncTable(tableConfig config.TableConfig, tableIndex int, totalTables int) error {
	// 获取表结构
	discovery := schema.NewSchemaDiscovery(e.sourceDB.DB)
	schemaParts := strings.Split(tableConfig.Source, ".")
	var schemaName, tableName string
	if len(schemaParts) == 2 {
		schemaName = schemaParts[0]
		tableName = schemaParts[1]
	} else {
		schemaName = "dbo"
		tableName = schemaParts[0]
	}

	ts, err := discovery.DiscoverTable(schemaName, tableName)
	if err != nil {
		return fmt.Errorf("discover table schema failed: %w", err)
	}

	// 获取总行数
	totalRows, err := database.GetTableRowCount(e.sourceDB.DB, tableConfig.Source)
	if err != nil {
		return fmt.Errorf("get row count failed: %w", err)
	}

	// 注册表到统计中
	tableStats := e.stats.StartTable(tableConfig.Source, totalRows)

	// 显示主键信息（包括复合主键）
	pkInfo := ts.PrimaryKey
	if len(ts.PrimaryKeys) > 1 {
		pkInfo = fmt.Sprintf("%s (composite: %s)", ts.PrimaryKey, strings.Join(ts.PrimaryKeys, ", "))
	}
	logger.Info("[%d/%d] [%s] Estimated rows: %d, Primary key: %s", tableIndex, totalTables, tableConfig.Source, totalRows, pkInfo)

	// 检测大字段并警告
	e.checkLargeColumns(tableConfig.Source, ts)

	// 如果需要，清空目标表
	if tableConfig.TruncateBeforeSync {
		if err := database.TruncateTable(e.targetDB.DB, tableConfig.Target); err != nil {
			return fmt.Errorf("truncate target table failed: %w", err)
		}
		logger.Info("[%s] Target table truncated", tableConfig.Source)
	}

	// 确定分段字段
	splitColumn := tableConfig.SplitColumn
	if splitColumn == "" {
		splitColumn = ts.PrimaryKey
	}

	// 创建表级进度更新器
	var syncErr error
	if splitColumn != "" && totalRows > int64(e.config.Sync.BatchSize) {
		// 获取主键范围判断数据分布
		minID, maxID, err := database.GetPrimaryKeyRange(e.sourceDB.DB, tableConfig.Source, splitColumn)
		if err == nil && minID > 0 && maxID > 0 {
			keyRange := maxID - minID + 1
			// 如果主键范围比行数大100倍以上，说明数据分布极不均匀，使用顺序同步
			if keyRange/int64(totalRows) > 100 {
				logger.Info("  [%s] Data distribution uneven (range/rows=%d), using sequential mode", tableConfig.Source, keyRange/int64(totalRows))
				syncErr = e.syncTableSequential(tableConfig, ts, tableStats)
			} else {
				// 使用主键范围分段并发同步
				syncErr = e.syncTableWithSplit(tableConfig, ts, splitColumn, totalRows, minID, maxID, tableStats)
			}
		} else {
			syncErr = e.syncTableSequential(tableConfig, ts, tableStats)
		}
	} else {
		// 单线程同步
		syncErr = e.syncTableSequential(tableConfig, ts, tableStats)
	}

	// 标记表完成
	if syncErr == nil {
		e.stats.CompleteTable(tableConfig.Source)
	}
	return syncErr
}

// syncTableWithSplit 使用分段并发同步表
func (e *Engine) syncTableWithSplit(tableConfig config.TableConfig, ts *schema.TableSchema, splitColumn string, totalRows int64, minID, maxID int64, tableStats *monitor.TableStats) error {
	// 计算合理的channel缓冲区大小：最多积压2倍的writer数量
	channelBuffer := e.config.Sync.Workers
	if channelBuffer > 100 {
		channelBuffer = 100
	}
	logger.Info("  [%s] Using channel buffer size: %d (workers: %d)", tableConfig.Source, channelBuffer, e.config.Sync.Workers)

	// 创建数据通道（限制缓冲区防止内存无限增长）
	dataChan := make(chan []map[string]interface{}, channelBuffer)

	// 启动写入Workers
	var wg sync.WaitGroup
	writerErrors := make(chan error, e.config.Sync.Workers)

	for i := 0; i < e.config.Sync.Workers/2; i++ {
		wg.Add(1)
		go e.tableWriter(tableConfig, ts, dataChan, &wg, writerErrors, tableStats)
	}

	// 创建读取任务
	tasks := e.createSplitTasks(minID, maxID, e.config.Sync.Workers)

	// 启动读取Workers
	readWg := sync.WaitGroup{}
	readerErrors := make(chan error, len(tasks))

	// 记录失败的任务，用于后续补偿
	var failedTasks []SplitTask
	var failedMu sync.Mutex

	for _, task := range tasks {
		readWg.Add(1)
		go func(t SplitTask) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("  [%s] Reader panic: %v", tableConfig.Source, r)
				}
				readWg.Done()
			}()
			if err := e.tableReader(tableConfig, ts, splitColumn, t, dataChan); err != nil {
				logger.Error("  [%s] Reader error: %v", tableConfig.Source, err)
				failedMu.Lock()
				failedTasks = append(failedTasks, t)
				failedMu.Unlock()
				readerErrors <- err
			}
		}(task)
	}

	// 等待读取完成，然后关闭通道
	go func() {
		readWg.Wait()
		close(dataChan)
		close(readerErrors)
	}()

	// 启动 watchdog 监控，防止 reader 死锁
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-time.After(30 * time.Minute): // 30分钟超时
			logger.Error("  [%s] Table sync timeout (30min), forcing close", tableConfig.Source)
			close(dataChan)
		}
	}()

	// 等待写入完成
	wg.Wait()
	close(done) // 取消 watchdog
	close(writerErrors)

	// 检查错误
	var readerErr error
	for err := range readerErrors {
		if err != nil && readerErr == nil {
			readerErr = err
		}
	}

	var writerErr error
	for err := range writerErrors {
		if err != nil && writerErr == nil {
			writerErr = err
		}
	}

	// 如果有失败的 ranges，使用顺序模式作为补偿机制
	if len(failedTasks) > 0 {
		logger.Warn("  [%s] %d range(s) failed in parallel mode, compensating with sequential mode", tableConfig.Source, len(failedTasks))
		for _, task := range failedTasks {
			logger.Info("  [%s] Compensating range %d-%d", tableConfig.Source, task.Start, task.End)
			if err := e.tableReaderSequential(tableConfig, ts, splitColumn, task, tableStats); err != nil {
				logger.Error("  [%s] Compensation failed for range %d-%d: %v", tableConfig.Source, task.Start, task.End, err)
				return fmt.Errorf("reader error (compensation failed): %w", err)
			}
		}
	}

	if writerErr != nil {
		return fmt.Errorf("writer error: %w", writerErr)
	}

	return nil
}

// syncTableSequential 单线程顺序同步表
func (e *Engine) syncTableSequential(tableConfig config.TableConfig, ts *schema.TableSchema, tableStats *monitor.TableStats) error {
	// 获取列名
	columns := make([]string, 0, len(ts.Columns))
	for _, col := range ts.Columns {
		columns = append(columns, col.Name)
	}

	// 构建查询
	query := fmt.Sprintf("SELECT %s FROM %s WITH (NOLOCK)",
		strings.Join(columns, ", "), tableConfig.Source)

	if tableConfig.Where != "" {
		query += " WHERE " + tableConfig.Where
	}

	// 执行查询
	rows, err := e.sourceDB.DB.Query(query)
	if err != nil {
		return fmt.Errorf("query source table failed: %w", err)
	}
	defer rows.Close()

	// 获取列信息
	rowColumns, err := rows.Columns()
	if err != nil {
		return err
	}

	// 准备批量插入
	batch := make([][]interface{}, 0, e.config.Sync.BatchSize)
	rowCount := int64(0)

	converter := schema.NewValueConverter()

	for rows.Next() {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		default:
		}

		// 扫描行
		values := make([]interface{}, len(rowColumns))
		valuePtrs := make([]interface{}, len(rowColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		// 转换值
		convertedValues := make([]interface{}, len(values))
		for i, v := range values {
			convertedValues[i], _ = converter.Convert(v, ts.Columns[i].SourceType)
		}

		batch = append(batch, convertedValues)

		// 批量插入
		if len(batch) >= e.config.Sync.BatchSize {
			if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
				return fmt.Errorf("insert batch failed: %w", err)
			}
			rowCount += int64(len(batch))
			e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
			// 释放batch内存
			batch = nil
			batch = make([][]interface{}, 0, e.config.Sync.BatchSize)
		}
	}

	// 插入剩余数据
	if len(batch) > 0 {
		if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
			return fmt.Errorf("insert final batch failed: %w", err)
		}
		rowCount += int64(len(batch))
		e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
	}

	return rows.Err()
}

// tableReader 表数据读取器
func (e *Engine) tableReader(tableConfig config.TableConfig, ts *schema.TableSchema, splitColumn string, task SplitTask, dataChan chan<- []map[string]interface{}) error {
	// 获取专用连接（每个reader使用独立连接，超时时可以单独关闭）
	conn, err := e.sourceDB.DB.Conn(e.ctx)
	if err != nil {
		return fmt.Errorf("get dedicated connection failed: %w", err)
	}
	defer conn.Close()

	// 使用 goroutine + channel 实现超时控制
	readerDone := make(chan error, 1)

	go func() {
		readerDone <- e.tableReaderWithConn(conn, tableConfig, ts, splitColumn, task, dataChan)
	}()

	select {
	case err := <-readerDone:
		return err
	case <-time.After(15 * time.Minute):
		// 超时后强制关闭专用连接，以解除 rows.Next() 的阻塞
		logger.Error("  [%s] Reader timeout (15min) for range %d-%d, closing connection", tableConfig.Source, task.Start, task.End)
		conn.Close()
		return fmt.Errorf("tableReader timeout (15min) for range %d-%d", task.Start, task.End)
	}
}

// tableReaderWithConn 使用专用连接读取数据
func (e *Engine) tableReaderWithConn(conn *sql.Conn, tableConfig config.TableConfig, ts *schema.TableSchema, splitColumn string, task SplitTask, dataChan chan<- []map[string]interface{}) error {
	// 获取列名
	columns := make([]string, 0, len(ts.Columns))
	for _, col := range ts.Columns {
		columns = append(columns, col.Name)
	}

	// 构建查询 (SQL Server 使用 @p1, @p2 作为参数占位符)
	query := fmt.Sprintf("SELECT %s FROM %s WITH (NOLOCK) WHERE %s >= @p1 AND %s <= @p2",
		strings.Join(columns, ", "), tableConfig.Source, splitColumn, splitColumn)

	if tableConfig.Where != "" {
		query += " AND " + tableConfig.Where
	}

	query += fmt.Sprintf(" ORDER BY %s", splitColumn)

	// 使用带超时的上下文查询数据库（15分钟超时）
	queryCtx, cancel := context.WithTimeout(e.ctx, 15*time.Minute)
	defer cancel()

	// 执行查询
	rows, err := conn.QueryContext(queryCtx, query, task.Start, task.End)
	if err != nil {
		return fmt.Errorf("query failed (range %d-%d): %w", task.Start, task.End, err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return err
	}

	// 批量读取并发送
	batch := make([]map[string]interface{}, 0, e.config.Sync.ReadBuffer)
	converter := schema.NewValueConverter()

	for rows.Next() {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-queryCtx.Done():
			return fmt.Errorf("query timeout (range %d-%d)", task.Start, task.End)
		default:
		}

		// 扫描行
		values := make([]interface{}, len(rowColumns))
		valuePtrs := make([]interface{}, len(rowColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		// 构建行数据
		rowData := make(map[string]interface{})
		for i, col := range columns {
			convertedVal, _ := converter.Convert(values[i], ts.Columns[i].SourceType)
			rowData[col] = convertedVal
		}

		batch = append(batch, rowData)

		// 发送批次
		if len(batch) >= e.config.Sync.ReadBuffer {
			// 估算batch内存大小并更新统计
			batchMemory := e.estimateBatchMemory(batch, ts)
			e.stats.UpdateTableBuffer(tableConfig.Source, len(batch), batchMemory)

			select {
			case dataChan <- batch:
			case <-e.ctx.Done():
				return e.ctx.Err()
			case <-queryCtx.Done():
				return fmt.Errorf("send timeout (range %d-%d)", task.Start, task.End)
			}
			// 释放引用，让GC回收内存
			batch = nil
			batch = make([]map[string]interface{}, 0, e.config.Sync.ReadBuffer)
		}
	}

	// 发送剩余数据
	if len(batch) > 0 {
		// 估算batch内存大小并更新统计
		batchMemory := e.estimateBatchMemory(batch, ts)
		e.stats.UpdateTableBuffer(tableConfig.Source, len(batch), batchMemory)

		select {
		case dataChan <- batch:
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-queryCtx.Done():
			return fmt.Errorf("send timeout (range %d-%d)", task.Start, task.End)
		}
	}

	return rows.Err()
}

// tableReaderWithTimeout 已废弃，保留用于兼容
func (e *Engine) tableReaderWithTimeout(tableConfig config.TableConfig, ts *schema.TableSchema, splitColumn string, task SplitTask, dataChan chan<- []map[string]interface{}) error {
	return nil
}

// tableReaderSequential 顺序读取失败的range（作为补偿机制）
func (e *Engine) tableReaderSequential(tableConfig config.TableConfig, ts *schema.TableSchema, splitColumn string, task SplitTask, tableStats *monitor.TableStats) error {
	// 获取列名
	columns := make([]string, 0, len(ts.Columns))
	for _, col := range ts.Columns {
		columns = append(columns, col.Name)
	}

	// 构建查询 (SQL Server 使用 @p1, @p2 作为参数占位符)
	query := fmt.Sprintf("SELECT %s FROM %s WITH (NOLOCK) WHERE %s >= @p1 AND %s <= @p2",
		strings.Join(columns, ", "), tableConfig.Source, splitColumn, splitColumn)

	if tableConfig.Where != "" {
		query += " AND " + tableConfig.Where
	}

	query += fmt.Sprintf(" ORDER BY %s", splitColumn)

	// 补偿模式使用更长的超时（20分钟）
	queryCtx, cancel := context.WithTimeout(e.ctx, 20*time.Minute)
	defer cancel()

	// 获取专用连接
	conn, err := e.sourceDB.DB.Conn(queryCtx)
	if err != nil {
		return fmt.Errorf("compensation get connection failed: %w", err)
	}
	defer conn.Close()

	// 执行查询
	rows, err := conn.QueryContext(queryCtx, query, task.Start, task.End)
	if err != nil {
		return fmt.Errorf("compensation query failed (range %d-%d): %w", task.Start, task.End, err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return err
	}

	// 批量读取并直接插入（不经过channel，避免channel已关闭的问题）
	batch := make([][]interface{}, 0, e.config.Sync.BatchSize)
	converter := schema.NewValueConverter()

	for rows.Next() {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-queryCtx.Done():
			return fmt.Errorf("compensation timeout (range %d-%d)", task.Start, task.End)
		default:
		}

		// 扫描行
		values := make([]interface{}, len(rowColumns))
		valuePtrs := make([]interface{}, len(rowColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("compensation scan row failed: %w", err)
		}

		// 转换值
		convertedValues := make([]interface{}, len(values))
		for i, v := range values {
			convertedValues[i], _ = converter.Convert(v, ts.Columns[i].SourceType)
		}

		batch = append(batch, convertedValues)

		// 批量插入
		if len(batch) >= e.config.Sync.BatchSize {
			if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
				return fmt.Errorf("compensation insert batch failed: %w", err)
			}
			e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
			batch = nil
			batch = make([][]interface{}, 0, e.config.Sync.BatchSize)
		}
	}

	// 插入剩余数据
	if len(batch) > 0 {
		if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
			return fmt.Errorf("compensation insert final batch failed: %w", err)
		}
		e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
	}

	logger.Info("  [%s] Compensation completed for range %d-%d", tableConfig.Source, task.Start, task.End)
	return rows.Err()
}

// tableWriter 表数据写入器
func (e *Engine) tableWriter(tableConfig config.TableConfig, ts *schema.TableSchema, dataChan <-chan []map[string]interface{}, wg *sync.WaitGroup, errors chan<- error, tableStats *monitor.TableStats) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("  [%s] Writer panic: %v", tableConfig.Source, r)
			errors <- fmt.Errorf("writer panic: %v", r)
		}
		wg.Done()
	}()

	// 获取列名
	columns := make([]string, 0, len(ts.Columns))
	for _, col := range ts.Columns {
		columns = append(columns, col.Name)
	}

	for batch := range dataChan {
		select {
		case <-e.ctx.Done():
			return
		default:
		}

		// 估算batch内存大小，用于后续减少统计
		batchMemory := e.estimateBatchMemory(batch, ts)
		batchRowCount := int64(len(batch))

		// 转换批次为插入格式
		rows := make([][]interface{}, 0, len(batch))
		for _, rowData := range batch {
			row := make([]interface{}, len(columns))
			for i, col := range columns {
				row[i] = rowData[col]
			}
			rows = append(rows, row)
		}

		// 释放batch引用，让GC回收
		batch = nil

		// 减少缓冲区统计（batch已被消费）
		e.stats.ResetTableBuffer(tableConfig.Source, batchRowCount, batchMemory)

		// 批量插入，带重试
		err := database.RetryOperation(e.config.Sync.MaxRetries, e.config.GetRetryInterval(), func() error {
			return e.insertBatch(tableConfig.Target, columns, rows)
		})

		// 插入完成后释放rows引用
		rows = nil

		if err != nil {
			errors <- fmt.Errorf("insert batch failed: %w", err)
			return
		}

		// 更新表级进度
		e.stats.UpdateTableProgress(tableConfig.Source, batchRowCount)
	}
}

// insertBatch 批量插入数据
func (e *Engine) insertBatch(table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// MySQL 限制：最多 65535 个参数
	// 计算每批最大行数：65535 / 列数
	maxParams := 60000 // 留一些余量
	colCount := len(columns)
	maxRowsPerBatch := maxParams / colCount
	if maxRowsPerBatch < 1 {
		maxRowsPerBatch = 1
	}

	// 分批插入
	for start := 0; start < len(rows); start += maxRowsPerBatch {
		end := start + maxRowsPerBatch
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]

		// 构建批量INSERT语句
		query, args, err := e.buildBatchInsert(table, columns, batch)
		if err != nil {
			return err
		}

		// 执行插入
		_, err = e.targetDB.DB.Exec(query, args...)
		if err != nil {
			return err
		}
	}

	return nil
}

// buildBatchInsert 构建批量INSERT语句
func (e *Engine) buildBatchInsert(table string, columns []string, rows [][]interface{}) (string, []interface{}, error) {
	var sb strings.Builder

	// 估算需要的容量：每行大约 (列数*20) 字节
	// 限制预分配最多 256KB，避免内存浪费
	estimatedSize := len(rows) * len(columns) * 20
	if estimatedSize > 256*1024 {
		estimatedSize = 256 * 1024
	}
	if estimatedSize < 1024 {
		estimatedSize = 1024
	}
	sb.Grow(estimatedSize)

	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES ")

	args := make([]interface{}, 0, len(rows)*len(columns))
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := "(" + strings.Join(placeholders, ", ") + ")"

	valueStrs := make([]string, 0, len(rows))
	for _, row := range rows {
		valueStrs = append(valueStrs, placeholderStr)
		args = append(args, row...)
	}
	sb.WriteString(strings.Join(valueStrs, ", "))

	return sb.String(), args, nil
}

// estimateBatchMemory 估算batch的内存大小（字节）
func (e *Engine) estimateBatchMemory(batch []map[string]interface{}, ts *schema.TableSchema) int64 {
	if len(batch) == 0 {
		return 0
	}

	// 估算每行的基础开销：map开销 + 字段数 * (key指针 + value指针)
	// map 本身约 48 字节 + 每个字段约 16 字节（key+value指针）
	rowOverhead := int64(48 + len(ts.Columns)*16)

	var totalMemory int64
	for _, row := range batch {
		// 基础行开销
		totalMemory += rowOverhead

		// 估算字段值的大小
		for _, col := range ts.Columns {
			val := row[col.Name]
			totalMemory += e.estimateValueSize(val, col.SourceType)
		}
	}

	return totalMemory
}

// estimateValueSize 估算单个值的内存大小
func (e *Engine) estimateValueSize(val interface{}, sourceType string) int64 {
	if val == nil {
		return 8 // nil 指针开销
	}

	switch v := val.(type) {
	case string:
		return int64(24 + len(v)) // string 头开销 + 内容
	case []byte:
		return int64(24 + len(v)) // slice 头开销 + 内容
	case int64, int, uint64, uint:
		return 8
	case int32, int16, int8, uint32, uint16, uint8:
		return 4
	case float64:
		return 8
	case float32:
		return 4
	case bool:
		return 1
	case time.Time:
		return 24 // time.Time 结构体大小
	default:
		// 根据 SQL 类型估算
		typeName := strings.ToUpper(sourceType)
		switch typeName {
		case "VARCHAR", "NVARCHAR":
			return 100 // 平均估算
		case "TEXT", "NTEXT", "XML":
			return 1024 // 大文本平均估算
		case "IMAGE", "VARBINARY":
			return 1024
		default:
			return 32 // 默认估算
		}
	}
}

// checkLargeColumns 检测大字段并发出警告
func (e *Engine) checkLargeColumns(tableName string, ts *schema.TableSchema) {
	var largeCols []string
	for _, col := range ts.Columns {
		typeName := strings.ToUpper(col.SourceType)
		// 检测大字段类型
		switch typeName {
		case "TEXT", "NTEXT", "IMAGE", "XML", "VARBINARY(MAX)", "VARCHAR(MAX)", "NVARCHAR(MAX)":
			largeCols = append(largeCols, fmt.Sprintf("%s(%s)", col.Name, typeName))
		default:
			// 检测超长的VARCHAR/NVARCHAR
			if (strings.HasPrefix(typeName, "VARCHAR") || strings.HasPrefix(typeName, "NVARCHAR")) && col.MaxLength > 8000 {
				largeCols = append(largeCols, fmt.Sprintf("%s(%s,%d)", col.Name, typeName, col.MaxLength))
			}
		}
	}
	if len(largeCols) > 0 {
		logger.Warn("  [%s] WARNING: Table has %d large column(s) that may cause high memory usage:", tableName, len(largeCols))
		for _, col := range largeCols {
			logger.Warn("    - %s", col)
		}
		logger.Warn("  [%s] Consider reducing 'read_buffer' and 'batch_size' in config", tableName)
	}
}

// SplitTask 分段任务
type SplitTask struct {
	Start int64
	End   int64
}

// createSplitTasks 创建分段任务
func (e *Engine) createSplitTasks(min, max int64, workerCount int) []SplitTask {
	if workerCount <= 0 {
		workerCount = 1
	}

	total := max - min + 1
	perWorker := total / int64(workerCount)

	if perWorker == 0 {
		perWorker = 1
	}

	tasks := make([]SplitTask, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		start := min + int64(i)*perWorker
		end := start + perWorker - 1
		if i == workerCount-1 {
			end = max
		}
		if start > max {
			break
		}
		tasks = append(tasks, SplitTask{Start: start, End: end})
	}

	return tasks
}
