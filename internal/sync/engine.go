package sync

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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
			defer wg.Done()
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
				logger.Info("  Data distribution uneven (range/rows=%d), using sequential mode", keyRange/int64(totalRows))
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

	// 创建数据通道
	dataChan := make(chan []map[string]interface{}, e.config.Sync.ChannelBuffer)

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
	for _, task := range tasks {
		readWg.Add(1)
		go func(t SplitTask) {
			defer readWg.Done()
			if err := e.tableReader(tableConfig, ts, splitColumn, t, dataChan); err != nil {
				fmt.Printf("  Reader error: %v\n", err)
			}
		}(task)
	}

	// 等待读取完成，然后关闭通道
	go func() {
		readWg.Wait()
		close(dataChan)
	}()

	// 等待写入完成
	wg.Wait()
	close(writerErrors)

	// 检查错误
	for err := range writerErrors {
		if err != nil {
			return err
		}
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
			batch = batch[:0]
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

	// 执行查询
	rows, err := e.sourceDB.DB.Query(query, task.Start, task.End)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
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
			select {
			case dataChan <- batch:
			case <-e.ctx.Done():
				return e.ctx.Err()
			}
			batch = make([]map[string]interface{}, 0, e.config.Sync.ReadBuffer)
		}
	}

	// 发送剩余数据
	if len(batch) > 0 {
		select {
		case dataChan <- batch:
		case <-e.ctx.Done():
			return e.ctx.Err()
		}
	}

	return rows.Err()
}

// tableWriter 表数据写入器
func (e *Engine) tableWriter(tableConfig config.TableConfig, ts *schema.TableSchema, dataChan <-chan []map[string]interface{}, wg *sync.WaitGroup, errors chan<- error, tableStats *monitor.TableStats) {
	defer wg.Done()

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

		// 转换批次为插入格式
		rows := make([][]interface{}, 0, len(batch))
		for _, rowData := range batch {
			row := make([]interface{}, len(columns))
			for i, col := range columns {
				row[i] = rowData[col]
			}
			rows = append(rows, row)
		}

		// 批量插入，带重试
		err := database.RetryOperation(e.config.Sync.MaxRetries, e.config.GetRetryInterval(), func() error {
			return e.insertBatch(tableConfig.Target, columns, rows)
		})

		if err != nil {
			errors <- fmt.Errorf("insert batch failed: %w", err)
			return
		}

		// 更新表级进度
		e.stats.UpdateTableProgress(tableConfig.Source, int64(len(rows)))
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

	// 预分配容量
	sb.Grow(1024 * 1024)

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
