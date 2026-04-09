package compare

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wangdong58/FastSync/internal/database"
	"github.com/wangdong58/FastSync/internal/logger"
)

// Comparator 数据对比器
type Comparator struct {
	sourceDB *database.Connection
	targetDB *database.Connection
	workers  int
}

// NewComparator 创建数据对比器
func NewComparator(sourceDB, targetDB *database.Connection, workers int) *Comparator {
	if workers <= 0 {
		workers = 10
	}
	return &Comparator{
		sourceDB: sourceDB,
		targetDB: targetDB,
		workers:  workers,
	}
}

// ColumnMinMax 列的Min/Max值
type ColumnMinMax struct {
	ColumnName   string
	SourceMin    interface{}
	SourceMax    interface{}
	TargetMin    interface{}
	TargetMax    interface{}
	MinMatch     bool
	MaxMatch     bool
}

// CompareResult 对比结果
type CompareResult struct {
	SourceTable      string
	TargetTable      string
	SourceCount      int64
	TargetCount      int64
	CountMatch       bool
	SampledRows      int64
	MatchedRows      int64
	MismatchedRows   int64
	SourceOnlyRows   int64
	TargetOnlyRows   int64
	Errors           []string
	SampleMismatches []MismatchDetail // 样本差异详情
	NoPKUK           bool             // 无主键和唯一键标识
	ColumnStats      []ColumnMinMax   // 列Min/Max统计
}

// MismatchDetail 差异详情
type MismatchDetail struct {
	Key        string                 // 主键或唯一键值（多列用逗号分隔）
	KeyColumns []string               // 键字段名称列表
	KeyType    string                 // 键类型：PK 或 UK
	SourceData map[string]interface{} // 源端数据
	TargetData map[string]interface{} // 目标端数据
	DiffCols   []string               // 差异列名
}

// CompareTables 对比多个表
func (c *Comparator) CompareTables(tables []TablePair) ([]CompareResult, error) {
	results := make([]CompareResult, 0, len(tables))
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 使用信号量限制并发
	sem := make(chan struct{}, c.workers)

	for _, table := range tables {
		wg.Add(1)
		go func(t TablePair) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			result, err := c.CompareTable(t.Source, t.Target, t.SampleRate)
			if err != nil {
				result.Errors = append(result.Errors, err.Error())
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(table)
	}

	wg.Wait()
	return results, nil
}

// TablePair 表对
type TablePair struct {
	Source     string
	Target     string
	SampleRate float64
}

// CompareTable 对比单个表
func (c *Comparator) CompareTable(sourceTable, targetTable string, sampleRate float64) (CompareResult, error) {
	result := CompareResult{
		SourceTable: sourceTable,
		TargetTable: targetTable,
	}

	logger.Info("\nComparing table: %s <-> %s", sourceTable, targetTable)

	// 1. 对比行数
	sourceCount, targetCount, countMatch, err := c.compareRowCount(sourceTable, targetTable)
	if err != nil {
		return result, err
	}

	result.SourceCount = sourceCount
	result.TargetCount = targetCount
	result.CountMatch = countMatch

	logger.Info("  [%s] Source count: %d, Target count: %d, Match: %v",
		sourceTable, sourceCount, targetCount, countMatch)

	if !countMatch {
		logger.Warn("  WARNING: Row count mismatch!")
	}

	// 2. 抽样对比数据内容
	if sampleRate > 0 && sourceCount > 0 {
		sampleResult, err := c.sampleCompare(sourceTable, targetTable, sampleRate)
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
		} else {
			result.SampledRows = sampleResult.SampledRows
			result.MatchedRows = sampleResult.MatchedRows
			result.MismatchedRows = sampleResult.MismatchedRows
			result.SourceOnlyRows = sampleResult.SourceOnlyRows
			result.TargetOnlyRows = sampleResult.TargetOnlyRows
			result.SampleMismatches = sampleResult.SampleMismatches
			result.NoPKUK = sampleResult.NoPKUK
			result.ColumnStats = sampleResult.ColumnStats

			if sampleResult.NoPKUK {
				logger.Info("  [%s] [NO_PK_UK] No primary/unique key - comparing row counts and column min/max only", sourceTable)
			} else {
				logger.Info("  [%s] Sampled: %d, Matched: %d, Mismatched: %d",
					sourceTable, sampleResult.SampledRows, sampleResult.MatchedRows, sampleResult.MismatchedRows)
			}
		}
	}

	return result, nil
}

// compareRowCount 对比行数
func (c *Comparator) compareRowCount(sourceTable, targetTable string) (int64, int64, bool, error) {
	sourceCount, err := database.GetTableRowCount(c.sourceDB.DB, sourceTable)
	if err != nil {
		return 0, 0, false, fmt.Errorf("get source row count failed: %w", err)
	}

	targetCount, err := database.GetTableRowCount(c.targetDB.DB, targetTable)
	if err != nil {
		return sourceCount, 0, false, fmt.Errorf("get target row count failed: %w", err)
	}

	return sourceCount, targetCount, sourceCount == targetCount, nil
}

// SampleResult 抽样对比结果
type SampleResult struct {
	SampledRows      int64
	MatchedRows      int64
	MismatchedRows   int64
	SourceOnlyRows   int64
	TargetOnlyRows   int64
	SampleMismatches []MismatchDetail // 样本差异详情
	NoPKUK           bool             // 无主键和唯一键标识
	ColumnStats      []ColumnMinMax   // 列Min/Max统计
}

// sampleCompare 抽样对比
func (c *Comparator) sampleCompare(sourceTable, targetTable string, sampleRate float64) (SampleResult, error) {
	result := SampleResult{}

	// 获取主键或唯一键信息（支持复合键）
	keyColumns, keyType, err := c.getUniqueKey(sourceTable)
	if err != nil {
		// 没有主键和唯一键，使用简化对比（行数 + Min/Max）
		return c.noPKUKCompare(sourceTable, targetTable)
	}

	// 获取抽样键列表（复合键值）
	sampleKeys, err := c.getSampleKeys(sourceTable, keyColumns, sampleRate)
	if err != nil {
		return result, err
	}

	result.SampledRows = int64(len(sampleKeys))

	if len(sampleKeys) == 0 {
		return result, nil
	}

	// 并发对比抽样数据
	var matched, mismatched, sourceOnly, targetOnly int64
	var mismatchesMu sync.Mutex
	var sampleMismatches []MismatchDetail

	// 使用worker pool
	tasks := make(chan []interface{}, len(sampleKeys))
	var wg sync.WaitGroup

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for keyValues := range tasks {
				match, srcOnly, tgtOnly, diffCols, err := c.compareRowByKey(
					sourceTable, targetTable, keyColumns, keyValues,
				)
				if err != nil {
					continue
				}

				if match {
					atomic.AddInt64(&matched, 1)
				} else {
					atomic.AddInt64(&mismatched, 1)
					// 收集差异详情（限制样本数量）
					if len(sampleMismatches) < 5 {
						mismatchesMu.Lock()
						if len(sampleMismatches) < 5 {
							// 获取完整数据用于展示
							srcData, tgtData := c.fetchRowDataForDisplay(sourceTable, targetTable, keyColumns, keyValues)
							// 构建键值字符串
							keyStr := buildKeyString(keyColumns, keyValues)
							sampleMismatches = append(sampleMismatches, MismatchDetail{
								Key:        keyStr,
								KeyColumns: keyColumns,
								KeyType:    keyType,
								SourceData: srcData,
								TargetData: tgtData,
								DiffCols:   diffCols,
							})
						}
						mismatchesMu.Unlock()
					}
				}
				if srcOnly {
					atomic.AddInt64(&sourceOnly, 1)
				}
				if tgtOnly {
					atomic.AddInt64(&targetOnly, 1)
				}
			}
		}()
	}

	// 发送任务
	for _, key := range sampleKeys {
		tasks <- key
	}
	close(tasks)

	wg.Wait()

	result.MatchedRows = matched
	result.MismatchedRows = mismatched
	result.SourceOnlyRows = sourceOnly
	result.TargetOnlyRows = targetOnly
	result.SampleMismatches = sampleMismatches

	return result, nil
}

// getPrimaryKey 获取主键列名
func (c *Comparator) getPrimaryKey(table string) (string, error) {
	// 查询主键信息
	query := fmt.Sprintf(`
		SELECT c.name
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID('%s')
	`, table)

	var pkName string
	err := c.sourceDB.DB.QueryRow(query).Scan(&pkName)
	if err != nil {
		return "", err
	}

	return pkName, nil
}

// getUniqueKey 获取唯一键列名（优先主键，其次唯一键）
// 返回：键列名列表、键类型（PK/UK）、错误
func (c *Comparator) getUniqueKey(table string) ([]string, string, error) {
	// 先尝试获取主键
	pkColumn, err := c.getPrimaryKey(table)
	if err == nil && pkColumn != "" {
		return []string{pkColumn}, "PK", nil
	}

	// 如果没有主键，获取第一个唯一索引的所有列
	query := fmt.Sprintf(`
		SELECT c.name
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		WHERE i.is_unique = 1 AND i.is_primary_key = 0 AND i.object_id = OBJECT_ID('%s')
		ORDER BY i.index_id, ic.key_ordinal
	`, table)

	rows, err := c.sourceDB.DB.Query(query)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var uniqueKeys []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			continue
		}
		uniqueKeys = append(uniqueKeys, col)
	}

	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	if len(uniqueKeys) == 0 {
		return nil, "", fmt.Errorf("no unique key found")
	}

	return uniqueKeys, "UK", nil
}

// getSampleKeys 获取抽样键（支持复合键）
func (c *Comparator) getSampleKeys(table string, keyColumns []string, sampleRate float64) ([][]interface{}, error) {
	// 首先估算总行数（使用系统表快速获取）
	var totalRows int64
	countQuery := fmt.Sprintf(`
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		WHERE t.name = '%s' AND p.index_id IN (0, 1)
	`, table[strings.LastIndex(table, ".")+1:])
	_ = c.sourceDB.DB.QueryRow(countQuery).Scan(&totalRows)
	if totalRows == 0 {
		// 如果系统表没有数据，使用近似值
		totalRows = 100000
	}

	// 计算抽样数量，限制最大10000条
	sampleSize := int(float64(totalRows) * sampleRate)
	if sampleSize < 1 {
		sampleSize = 1
	}
	if sampleSize > 10000 {
		sampleSize = 10000
	}

	// 构建列列表
	colList := strings.Join(keyColumns, ", ")

	// 使用 TABLESAMPLE 快速抽样（SQL Server 2005+）
	query := fmt.Sprintf(`
		SELECT %s FROM %s WITH (NOLOCK)
		TABLESAMPLE(%d ROWS)
	`, colList, table, sampleSize)

	rows, err := c.sourceDB.DB.Query(query)
	if err != nil {
		// TABLESAMPLE 失败，回退到传统方式
		return c.getSampleKeysFallback(table, keyColumns, sampleRate)
	}
	defer rows.Close()

	sampleKeys := make([][]interface{}, 0, sampleSize)
	for rows.Next() {
		// 为每个键列创建一个值
		keyValues := make([]interface{}, len(keyColumns))
		valuePtrs := make([]interface{}, len(keyColumns))
		for i := range keyValues {
			valuePtrs[i] = &keyValues[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}
		sampleKeys = append(sampleKeys, keyValues)
	}

	return sampleKeys, rows.Err()
}

// getSampleKeysFallback 传统抽样方式（回退方案）
func (c *Comparator) getSampleKeysFallback(table string, keyColumns []string, sampleRate float64) ([][]interface{}, error) {
	// 构建列列表
	colList := strings.Join(keyColumns, ", ")

	// 获取所有键（限制最多50000条）
	query := fmt.Sprintf(`
		SELECT TOP 50000 %s FROM %s WITH (NOLOCK)
		ORDER BY NEWID()
	`, colList, table)
	rows, err := c.sourceDB.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allKeys := make([][]interface{}, 0, 50000)
	for rows.Next() {
		// 为每个键列创建一个值
		keyValues := make([]interface{}, len(keyColumns))
		valuePtrs := make([]interface{}, len(keyColumns))
		for i := range keyValues {
			valuePtrs[i] = &keyValues[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}
		allKeys = append(allKeys, keyValues)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 随机抽样，限制最大10000条
	sampleSize := int(float64(len(allKeys)) * sampleRate)
	if sampleSize < 1 {
		sampleSize = 1
	}
	if sampleSize > 10000 {
		sampleSize = 10000
	}

	// 洗牌算法抽样
	rand.Shuffle(len(allKeys), func(i, j int) {
		allKeys[i], allKeys[j] = allKeys[j], allKeys[i]
	})

	return allKeys[:sampleSize], nil
}

// compareRowByKey 根据键对比单行数据（支持复合键）
// 返回: match-是否匹配, sourceOnly-仅源端存在, targetOnly-仅目标端存在, diffCols-差异列, err-错误
func (c *Comparator) compareRowByKey(sourceTable, targetTable string, keyColumns []string, keyValues []interface{}) (match, sourceOnly, targetOnly bool, diffCols []string, err error) {
	// 构建 WHERE 条件（多列）
	var whereConditions []string
	for i, col := range keyColumns {
		_ = i // 用于参数占位符索引
		whereConditions = append(whereConditions, fmt.Sprintf("%s = @p%d", col, i+1))
	}
	whereClause := strings.Join(whereConditions, " AND ")

	// 获取源端数据 (SQL Server 使用 @p1, @p2... 作为参数占位符)
	sourceQuery := fmt.Sprintf("SELECT * FROM %s WITH (NOLOCK) WHERE %s",
		sourceTable, whereClause)
	sourceRow, err := c.fetchRow(c.sourceDB.DB, sourceQuery, keyValues...)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, false, true, nil, nil // 只在目标端存在
		}
		return false, false, false, nil, err
	}

	// 获取目标端数据 (MySQL 使用 ? 作为参数占位符)
	// 构建相同数量的 ? 占位符
	placeholders := make([]string, len(keyColumns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	whereClauseMySQL := strings.Join(whereConditions, " AND ")
	// 替换 @pN 为 ?
	for i := 1; i <= len(keyColumns); i++ {
		whereClauseMySQL = strings.ReplaceAll(whereClauseMySQL, fmt.Sprintf("@p%d", i), "?")
	}
	targetQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		targetTable, whereClauseMySQL)
	targetRow, err := c.fetchRow(c.targetDB.DB, targetQuery, keyValues...)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, true, false, nil, nil // 只在源端存在
		}
		return false, false, false, nil, err
	}

	// 对比行数据并获取差异列
	match, diffCols = c.compareRowDataWithDiff(sourceRow, targetRow)
	return match, false, false, diffCols, nil
}

// fetchRow 获取单行数据
func (c *Comparator) fetchRow(db *sql.DB, query string, args ...interface{}) (map[string]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, sql.ErrNoRows
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i, col := range columns {
		result[col] = values[i]
	}

	return result, nil
}

// compareRowDataWithDiff 对比两行数据并返回差异列
func (c *Comparator) compareRowDataWithDiff(source, target map[string]interface{}) (bool, []string) {
	diffCols := make([]string, 0)

	// 检查所有源端列
	for key, srcVal := range source {
		tgtVal, exists := target[key]
		if !exists {
			diffCols = append(diffCols, fmt.Sprintf("%s(missing in target)", key))
			continue
		}

		// 处理NULL值
		if srcVal == nil && tgtVal == nil {
			continue
		}
		if srcVal == nil || tgtVal == nil {
			diffCols = append(diffCols, key)
			continue
		}

		// 对比值
		if !valuesEqual(srcVal, tgtVal) {
			diffCols = append(diffCols, key)
		}
	}

	// 检查目标端独有的列
	for key := range target {
		if _, exists := source[key]; !exists {
			diffCols = append(diffCols, fmt.Sprintf("%s(missing in source)", key))
		}
	}

	return len(diffCols) == 0, diffCols
}

// compareRowData 对比两行数据
func (c *Comparator) compareRowData(source, target map[string]interface{}) bool {
	match, _ := c.compareRowDataWithDiff(source, target)
	return match
}

// valuesEqual 判断两个值是否相等
func valuesEqual(a, b interface{}) bool {
	// 处理 []byte 类型（SQL Server 驱动返回的字节数组）
	if aBytes, ok := a.([]byte); ok {
		a = string(aBytes)
	}
	if bBytes, ok := b.([]byte); ok {
		b = string(bBytes)
	}

	// 转换为字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr == bStr
}

// fullTableHashCompare 全表哈希对比
func (c *Comparator) fullTableHashCompare(sourceTable, targetTable string, sampleRate float64) (SampleResult, error) {
	result := SampleResult{}

	// 无主键表对比：计算整体哈希摘要，并收集样本差异详情

	// 获取源端抽样哈希和样本数据
	sourceHashes, sourceSampleData, err := c.getTableHashesWithData(c.sourceDB.DB, sourceTable, sampleRate)
	if err != nil {
		return result, err
	}

	// 获取目标端抽样哈希和样本数据
	targetHashes, targetSampleData, err := c.getTableHashesWithData(c.targetDB.DB, targetTable, sampleRate)
	if err != nil {
		return result, err
	}

	// 使用哈希值本身作为对比依据（而不是行号）
	// 统计两边相同的哈希值数量
	sourceHashSet := make(map[string]int)
	for _, hash := range sourceHashes {
		sourceHashSet[hash]++
	}

	targetHashSet := make(map[string]int)
	for _, hash := range targetHashes {
		targetHashSet[hash]++
	}

	// 对比哈希集合
	matchedHashes := 0
	var sampleMismatches []MismatchDetail

	for hash, sourceCount := range sourceHashSet {
		targetCount := targetHashSet[hash]
		if targetCount > 0 {
			// 取较小的计数作为匹配数
			if sourceCount <= targetCount {
				matchedHashes += sourceCount
			} else {
				matchedHashes += targetCount
			}
		}
	}

	result.SampledRows = int64(len(sourceHashes))
	result.MatchedRows = int64(matchedHashes)
	result.MismatchedRows = result.SampledRows - result.MatchedRows
	result.SourceOnlyRows = int64(len(sourceHashes) - matchedHashes)
	result.TargetOnlyRows = int64(len(targetHashes) - matchedHashes)

	// 收集样本差异详情（选取几个源端独有的哈希值对应的行数据）
	for hash, srcData := range sourceSampleData {
		if targetHashSet[hash] == 0 && len(sampleMismatches) < 5 {
			// 查找目标端的一个样本数据进行对比
			var tgtData map[string]interface{}
			for _, data := range targetSampleData {
				tgtData = data
				break
			}

			// 计算差异列
			diffCols := make([]string, 0)
			for col, srcVal := range srcData {
				if tgtVal, ok := tgtData[col]; ok {
					if !valuesEqual(srcVal, tgtVal) {
						diffCols = append(diffCols, col)
					}
				} else {
					diffCols = append(diffCols, col+"(missing in target)")
				}
			}
			// 添加目标端独有的列
			for col := range tgtData {
				if _, ok := srcData[col]; !ok {
					diffCols = append(diffCols, col+"(missing in source)")
				}
			}

			sampleMismatches = append(sampleMismatches, MismatchDetail{
				Key:        fmt.Sprintf("hash_%d", len(sampleMismatches)+1),
				SourceData: srcData,
				TargetData: tgtData,
				DiffCols:   diffCols,
			})
		}
	}

	result.SampleMismatches = sampleMismatches
	return result, nil
}

// noPKUKCompare 无主键和唯一键表的简化对比（对比行数和每列Min/Max）
func (c *Comparator) noPKUKCompare(sourceTable, targetTable string) (SampleResult, error) {
	result := SampleResult{
		NoPKUK:      true,
		ColumnStats: make([]ColumnMinMax, 0),
	}

	// 获取源端列信息
	sourceCols, err := c.getTableColumns(c.sourceDB.DB, sourceTable)
	if err != nil {
		return result, fmt.Errorf("get source table columns failed: %w", err)
	}

	// 获取目标端列信息
	targetCols, err := c.getTableColumns(c.targetDB.DB, targetTable)
	if err != nil {
		return result, fmt.Errorf("get target table columns failed: %w", err)
	}

	// 找出共同列
	commonCols := make([]string, 0)
	targetColSet := make(map[string]bool)
	for _, col := range targetCols {
		targetColSet[col] = true
	}
	for _, col := range sourceCols {
		if targetColSet[col] {
			commonCols = append(commonCols, col)
		}
	}

	// 对比每列的Min/Max
	for _, col := range commonCols {
		stat, err := c.compareColumnMinMax(sourceTable, targetTable, col)
		if err != nil {
			// 某些列可能不支持MIN/MAX（如TEXT/BLOB），跳过
			continue
		}
		result.ColumnStats = append(result.ColumnStats, stat)
	}

	return result, nil
}

// getTableColumns 获取表的列名列表
func (c *Comparator) getTableColumns(db *sql.DB, table string) ([]string, error) {
	// 尝试SQL Server方式
	query := fmt.Sprintf(`
		SELECT c.name
		FROM sys.columns c
		WHERE c.object_id = OBJECT_ID('%s')
		ORDER BY c.column_id
	`, table)
	rows, err := db.Query(query)
	if err == nil {
		defer rows.Close()
		columns := make([]string, 0)
		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err == nil {
				columns = append(columns, col)
			}
		}
		return columns, rows.Err()
	}

	// MySQL方式
	query = fmt.Sprintf(`
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = '%s' AND table_schema = DATABASE()
		ORDER BY ordinal_position
	`, table)
	rows, err = db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err == nil {
			columns = append(columns, col)
		}
	}
	return columns, rows.Err()
}

// compareColumnMinMax 对比单列的Min/Max值
func (c *Comparator) compareColumnMinMax(sourceTable, targetTable, column string) (ColumnMinMax, error) {
	stat := ColumnMinMax{
		ColumnName: column,
	}

	// 获取源端Min/Max
	sourceQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WITH (NOLOCK)",
		column, column, sourceTable)
	var sourceMin, sourceMax interface{}
	err := c.sourceDB.DB.QueryRow(sourceQuery).Scan(&sourceMin, &sourceMax)
	if err != nil {
		return stat, err
	}
	stat.SourceMin = sourceMin
	stat.SourceMax = sourceMax

	// 获取目标端Min/Max
	targetQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		column, column, targetTable)
	var targetMin, targetMax interface{}
	err = c.targetDB.DB.QueryRow(targetQuery).Scan(&targetMin, &targetMax)
	if err != nil {
		return stat, err
	}
	stat.TargetMin = targetMin
	stat.TargetMax = targetMax

	// 比较Min值
	stat.MinMatch = valuesEqual(sourceMin, targetMin)

	// 比较Max值
	stat.MaxMatch = valuesEqual(sourceMax, targetMax)

	return stat, nil
}

// getTableHashes 获取表的行哈希
func (c *Comparator) getTableHashes(db *sql.DB, table string, sampleRate float64) (map[string]string, error) {
	hashes, _, err := c.getTableHashesWithData(db, table, sampleRate)
	return hashes, err
}

// getTableHashesWithData 获取表的行哈希和样本数据
// 返回：哈希列表、样本数据（哈希->行数据映射）、错误
func (c *Comparator) getTableHashesWithData(db *sql.DB, table string, sampleRate float64) (map[string]string, map[string]map[string]interface{}, error) {
	// 判断是SQL Server还是MySQL
	var columns []string
	var isSQLServer bool

	// 尝试SQL Server方式获取列名
	columnsQuery := fmt.Sprintf(`
		SELECT c.name
		FROM sys.columns c
		WHERE c.object_id = OBJECT_ID('%s')
		ORDER BY c.column_id
	`, table)
	rows, err := db.Query(columnsQuery)
	if err != nil {
		// 可能是MySQL，使用不同的查询
		columns, err = c.getMySQLColumns(db, table)
		if err != nil {
			return nil, nil, err
		}
		isSQLServer = false
	} else {
		defer rows.Close()
		columns = make([]string, 0)
		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err != nil {
				continue
			}
			columns = append(columns, col)
		}
		isSQLServer = true
	}

	// 获取列名列表字符串
	colList := strings.Join(columns, ", ")

	// 构建哈希查询
	var query string
	if isSQLServer {
		hashExpr := c.buildHashExpression(columns)
		query = fmt.Sprintf("SELECT %s, %s FROM %s WITH (NOLOCK)", hashExpr, colList, table)
	} else {
		exprs := make([]string, len(columns))
		for i, col := range columns {
			exprs[i] = fmt.Sprintf("IFNULL(CAST(%s AS CHAR), '')", col)
		}
		concatExpr := strings.Join(exprs, ", '|', ")
		query = fmt.Sprintf("SELECT MD5(CONCAT(%s)), %s FROM %s", concatExpr, colList, table)
	}

	return c.fetchHashesWithData(db, query, columns, sampleRate, isSQLServer)
}

// getMySQLColumns 获取MySQL表的列名
func (c *Comparator) getMySQLColumns(db *sql.DB, table string) ([]string, error) {
	columnsQuery := fmt.Sprintf(`
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = '%s' AND table_schema = DATABASE()
		ORDER BY ordinal_position
	`, table)
	rows, err := db.Query(columnsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var field string
		if err := rows.Scan(&field); err != nil {
			continue
		}
		columns = append(columns, field)
	}

	return columns, rows.Err()
}

// fetchHashesWithData 获取哈希列表和对应的行数据
func (c *Comparator) fetchHashesWithData(db *sql.DB, query string, columns []string, sampleRate float64, isSQLServer bool) (map[string]string, map[string]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	hashes := make(map[string]string)
	sampleData := make(map[string]map[string]interface{})
	idx := 0

	for rows.Next() {
		// 准备扫描：第一列是哈希，后面是数据列
		values := make([]interface{}, len(columns)+1)
		valuePtrs := make([]interface{}, len(columns)+1)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		hash := ""
		if values[0] != nil {
			if h, ok := values[0].([]byte); ok {
				hash = string(h)
			} else {
				hash = fmt.Sprintf("%v", values[0])
			}
		}

		// 构建行数据映射
		rowData := make(map[string]interface{})
		for i, col := range columns {
			rowData[col] = values[i+1]
		}

		// 抽样：使用哈希值进行确定性抽样
		hashPrefix := hash
		if len(hash) >= 8 {
			hashPrefix = hash[:8]
		}
		var hashNum uint64
		fmt.Sscanf(hashPrefix, "%x", &hashNum)

		if float64(hashNum%1000000)/1000000.0 < sampleRate {
			key := fmt.Sprintf("row_%d", idx)
			hashes[key] = hash
			sampleData[hash] = rowData
		}
		idx++
	}

	return hashes, sampleData, rows.Err()
}

// buildHashExpression 构建哈希表达式
func (c *Comparator) buildHashExpression(columns []string) string {
	// 使用 HASHBYTES('MD5', ...) 生成与 MySQL MD5 兼容的哈希
	// 将各列拼接后计算 MD5，并转换为小写十六进制字符串
	exprs := make([]string, len(columns))
	for i, col := range columns {
		exprs[i] = fmt.Sprintf("ISNULL(CAST(%s AS VARCHAR(MAX)), '')", col)
	}
	concatExpr := strings.Join(exprs, " + '|' + ")
	// CONVERT(..., 2) 将二进制 MD5 转为十六进制字符串
	return fmt.Sprintf("LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', %s), 2))", concatExpr)
}

// fetchRowDataForDisplay 获取用于展示的行数据（支持复合键）
func (c *Comparator) fetchRowDataForDisplay(sourceTable, targetTable string, keyColumns []string, keyValues []interface{}) (map[string]interface{}, map[string]interface{}) {
	// 构建 WHERE 条件（多列）
	var whereConditions []string
	for i, col := range keyColumns {
		whereConditions = append(whereConditions, fmt.Sprintf("%s = @p%d", col, i+1))
	}
	whereClause := strings.Join(whereConditions, " AND ")

	// 获取源端数据
	sourceQuery := fmt.Sprintf("SELECT * FROM %s WITH (NOLOCK) WHERE %s", sourceTable, whereClause)
	sourceRow, _ := c.fetchRow(c.sourceDB.DB, sourceQuery, keyValues...)

	// 获取目标端数据
	placeholders := make([]string, len(keyColumns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	whereClauseMySQL := strings.Join(whereConditions, " AND ")
	for i := 1; i <= len(keyColumns); i++ {
		whereClauseMySQL = strings.ReplaceAll(whereClauseMySQL, fmt.Sprintf("@p%d", i), "?")
	}
	targetQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s", targetTable, whereClauseMySQL)
	targetRow, _ := c.fetchRow(c.targetDB.DB, targetQuery, keyValues...)

	return sourceRow, targetRow
}

// buildKeyString 构建键值字符串
func buildKeyString(keyColumns []string, keyValues []interface{}) string {
	if len(keyColumns) == 1 {
		return fmt.Sprintf("%v", keyValues[0])
	}
	// 多列键：col1=val1, col2=val2, ...
	parts := make([]string, len(keyColumns))
	for i, col := range keyColumns {
		parts[i] = fmt.Sprintf("%s=%v", col, keyValues[i])
	}
	return strings.Join(parts, ", ")
}
func PrintCompareSummary(results []CompareResult) {
	logger.Info("\n========================================")
	logger.Info("         COMPARE SUMMARY")
	logger.Info("========================================")

	allMatch := true
	for _, r := range results {
		status := "OK"
		if !r.CountMatch || r.MismatchedRows > 0 || r.SourceOnlyRows > 0 || r.TargetOnlyRows > 0 {
			status = "MISMATCH"
			allMatch = false
		}

		// 标记无主键和唯一键表
		tag := ""
		if r.NoPKUK {
			tag = " [NO_PK_UK]"
		}

		logger.Info("\nTable: %s -> %s [%s]%s", r.SourceTable, r.TargetTable, status, tag)
		logger.Info("  Row Count: %d vs %d", r.SourceCount, r.TargetCount)

		// 无主键和唯一键表的显示
		if r.NoPKUK {
			if len(r.ColumnStats) > 0 {
				logger.Info("  Column Min/Max Comparison:")
				for _, stat := range r.ColumnStats {
					minStatus := "OK"
					if !stat.MinMatch {
						minStatus = "DIFF"
						allMatch = false
					}
					maxStatus := "OK"
					if !stat.MaxMatch {
						maxStatus = "DIFF"
						allMatch = false
					}
					logger.Info("    %s: Min[%s] Max[%s]", stat.ColumnName, minStatus, maxStatus)
					if !stat.MinMatch {
						logger.Error("      Source Min: %v | Target Min: %v",
							formatValue(stat.SourceMin), formatValue(stat.TargetMin))
					}
					if !stat.MaxMatch {
						logger.Error("      Source Max: %v | Target Max: %v",
							formatValue(stat.SourceMax), formatValue(stat.TargetMax))
					}
				}
			}
		} else if r.SampledRows > 0 {
			logger.Info("  Sampled: %d", r.SampledRows)
			logger.Info("  Matched: %d, Mismatched: %d", r.MatchedRows, r.MismatchedRows)
			logger.Info("  Source Only: %d, Target Only: %d", r.SourceOnlyRows, r.TargetOnlyRows)
		}
		if len(r.Errors) > 0 {
			logger.Error("  Errors: %d", len(r.Errors))
		}
		if len(r.SampleMismatches) > 0 {
			logger.Error("\n  Sample Mismatch Records:")
			for i, m := range r.SampleMismatches {
				keyType := m.KeyType
				if keyType == "" {
					keyType = "Key"
				}
				// 构建键显示字符串
				keyDisplay := m.Key
				if len(m.KeyColumns) > 1 {
					keyDisplay = fmt.Sprintf("(%s)", m.Key)
				}
				logger.Error("  --- Record %d (%s: %s) ---", i+1, keyType, keyDisplay)
				logger.Error("    Diff Columns: %v", m.DiffCols)
				for _, col := range m.DiffCols {
					srcVal := "NULL"
					tgtVal := "NULL"
					if v, ok := m.SourceData[col]; ok && v != nil {
						// 处理 []byte 类型
						if vBytes, ok := v.([]byte); ok {
							srcVal = string(vBytes)
						} else {
							srcVal = fmt.Sprintf("%v", v)
						}
						if len(srcVal) > 100 {
							srcVal = srcVal[:100] + "..."
						}
					}
					if v, ok := m.TargetData[col]; ok && v != nil {
						// 处理 []byte 类型
						if vBytes, ok := v.([]byte); ok {
							tgtVal = string(vBytes)
						} else {
							tgtVal = fmt.Sprintf("%v", v)
						}
						if len(tgtVal) > 100 {
							tgtVal = tgtVal[:100] + "..."
						}
					}
					logger.Error("    %s: Source=%s | Target=%s", col, srcVal, tgtVal)
				}
			}
		}
	}

	logger.Info("\n========================================")
	if allMatch {
		logger.Info("Result: ALL TABLES MATCH")
	} else {
		logger.Error("Result: SOME TABLES HAVE MISMATCHES")
	}
	logger.Info("========================================")
}

// formatValue 格式化值用于显示
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	// 处理 []byte 类型
	if vBytes, ok := v.([]byte); ok {
		return string(vBytes)
	}
	return fmt.Sprintf("%v", v)
}
