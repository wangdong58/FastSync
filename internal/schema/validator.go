package schema

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/wangdong58/FastSync/internal/config"
)

// Validator 结构校验器
type Validator struct {
	sourceDB *sql.DB
	targetDB *sql.DB
}

// NewValidator 创建新的结构校验器
func NewValidator(sourceDB, targetDB *sql.DB) *Validator {
	return &Validator{
		sourceDB: sourceDB,
		targetDB: targetDB,
	}
}

// ColumnDiff 列差异
type ColumnDiff struct {
	ColumnName string
	SourceType string
	TargetType string
	DiffType   string // MISSING_IN_TARGET, TYPE_MISMATCH, NULL_MISMATCH, DEFAULT_MISMATCH
}

// IndexDiff 索引差异
type IndexDiff struct {
	IndexName string
	DiffType  string // MISSING_IN_TARGET, TYPE_MISMATCH
}

// ConstraintDiff 约束差异
type ConstraintDiff struct {
	ConstraintName string
	DiffType       string // MISSING_IN_TARGET, TYPE_MISMATCH
}

// ValidationResult 校验结果
type ValidationResult struct {
	TableName         string
	MissingInTarget   bool
	ColumnDiffs       []ColumnDiff
	IndexDiffs        []IndexDiff
	ConstraintDiffs   []ConstraintDiff
	ColumnCommentDiff map[string]string // 源端有注释但目标端没有
}

// IsValid 检查是否完全匹配
func (vr *ValidationResult) IsValid() bool {
	return !vr.MissingInTarget &&
		len(vr.ColumnDiffs) == 0 &&
		len(vr.IndexDiffs) == 0 &&
		len(vr.ConstraintDiffs) == 0
}

// ValidateTable 对比单个表结构
func (v *Validator) ValidateTable(sourceSchema *ExtendedTableSchema, targetTable string) (*ValidationResult, error) {
	result := &ValidationResult{
		TableName:         targetTable,
		ColumnDiffs:       make([]ColumnDiff, 0),
		IndexDiffs:        make([]IndexDiff, 0),
		ConstraintDiffs:   make([]ConstraintDiff, 0),
		ColumnCommentDiff: make(map[string]string),
	}

	// 检查目标表是否存在
	exists, err := v.checkTableExists(targetTable)
	if err != nil {
		return nil, fmt.Errorf("check table existence failed: %w", err)
	}
	if !exists {
		result.MissingInTarget = true
		return result, nil
	}

	// 获取目标表结构
	targetSchema, err := v.getTargetTableSchema(targetTable)
	if err != nil {
		return nil, fmt.Errorf("get target schema failed: %w", err)
	}

	// 对比列
	v.compareColumns(sourceSchema, targetSchema, result)

	// 对比索引
	v.compareIndexes(sourceSchema, targetSchema, result)

	// 对比约束
	v.compareConstraints(sourceSchema, targetSchema, result)

	return result, nil
}

// ValidateAllTables 对比所有配置的表
func (v *Validator) ValidateAllTables(tables []config.TableConfig) ([]ValidationResult, error) {
	results := make([]ValidationResult, 0, len(tables))

	for _, table := range tables {
		fmt.Printf("Validating table: %s -> %s\n", table.Source, table.Target)

		// 获取源端表结构
		schemaParts := strings.Split(table.Source, ".")
		var schemaName, tableName string
		if len(schemaParts) == 2 {
			schemaName = schemaParts[0]
			tableName = schemaParts[1]
		} else {
			schemaName = "dbo"
			tableName = schemaParts[0]
		}

		sd := NewSchemaDiscovery(v.sourceDB)
		sourceSchema, err := sd.DiscoverExtendedTable(schemaName, tableName)
		if err != nil {
			fmt.Printf("  Error: failed to discover source schema: %v\n", err)
			continue
		}

		// 校验
		result, err := v.ValidateTable(sourceSchema, table.Target)
		if err != nil {
			fmt.Printf("  Error: validation failed: %v\n", err)
			continue
		}

		results = append(results, *result)
		v.printValidationResult(result)
	}

	return results, nil
}

// checkTableExists 检查目标表是否存在
func (v *Validator) checkTableExists(tableName string) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_name = ? AND table_schema = DATABASE()
	`
	var count int
	err := v.targetDB.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// getTargetTableSchema 获取目标表结构
func (v *Validator) getTargetTableSchema(tableName string) (*ExtendedTableSchema, error) {
	schema := &ExtendedTableSchema{
		TableSchema: TableSchema{
			TableName: tableName,
			Columns:   make([]ColumnSchema, 0),
			Indexes:   make([]IndexSchema, 0),
		},
		Constraints:    make([]ConstraintSchema, 0),
		ColumnComments: make(map[string]string),
	}

	// 获取列信息
	columnsQuery := `
		SELECT
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_default,
			column_comment
		FROM information_schema.columns
		WHERE table_name = ? AND table_schema = DATABASE()
		ORDER BY ordinal_position
	`
	rows, err := v.targetDB.Query(columnsQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnSchema
		var maxLen, precision, scale sql.NullInt64
		var isNullable string
		var colDefault, colComment sql.NullString

		err := rows.Scan(&col.Name, &col.SourceType, &maxLen, &precision, &scale, &isNullable, &colDefault, &colComment)
		if err != nil {
			continue
		}

		col.TargetType = col.SourceType
		if maxLen.Valid && maxLen.Int64 > 0 {
			col.MaxLength = maxLen.Int64
		}
		if precision.Valid {
			col.Precision = int(precision.Int64)
		}
		if scale.Valid {
			col.Scale = int(scale.Int64)
		}
		col.IsNullable = isNullable == "YES"
		if colDefault.Valid {
			col.DefaultValue = colDefault.String
		}
		if colComment.Valid && colComment.String != "" {
			schema.ColumnComments[col.Name] = colComment.String
		}

		schema.Columns = append(schema.Columns, col)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	// 获取索引信息
	indexQuery := `
		SELECT
			index_name,
			non_unique = 0 as is_unique,
			column_name
		FROM information_schema.statistics
		WHERE table_name = ? AND table_schema = DATABASE()
		ORDER BY index_name, seq_in_index
	`
	idxRows, err := v.targetDB.Query(indexQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer idxRows.Close()

	indexMap := make(map[string]*IndexSchema)
	for idxRows.Next() {
		var idxName, colName string
		var isUnique bool
		if err := idxRows.Scan(&idxName, &isUnique, &colName); err != nil {
			continue
		}

		if idx, exists := indexMap[idxName]; exists {
			idx.Columns = append(idx.Columns, colName)
		} else {
			indexMap[idxName] = &IndexSchema{
				Name:      idxName,
				IsUnique:  isUnique,
				IsPrimary: idxName == "PRIMARY",
				Columns:   []string{colName},
			}
		}
	}

	for _, idx := range indexMap {
		schema.Indexes = append(schema.Indexes, *idx)
		if idx.IsPrimary && len(idx.Columns) > 0 {
			schema.PrimaryKey = idx.Columns[0]
		}
		// 将唯一索引（非主键）也作为约束添加，以便与SQL Server的约束比较
		if idx.IsUnique && !idx.IsPrimary {
			schema.Constraints = append(schema.Constraints, ConstraintSchema{
				Name:    idx.Name,
				Type:    "UNIQUE",
				Columns: idx.Columns,
			})
		}
	}

	return schema, nil
}

// compareColumns 对比列差异
func (v *Validator) compareColumns(source, target *ExtendedTableSchema, result *ValidationResult) {
	targetColMap := make(map[string]ColumnSchema)
	for _, col := range target.Columns {
		// MySQL返回小写列名，统一转为小写作为key
		targetColMap[strings.ToLower(col.Name)] = col
	}

	for _, srcCol := range source.Columns {
		// SQL Server返回大写列名，统一转为小写进行匹配
		tgtCol, exists := targetColMap[strings.ToLower(srcCol.Name)]
		if !exists {
			result.ColumnDiffs = append(result.ColumnDiffs, ColumnDiff{
				ColumnName: srcCol.Name,
				SourceType: srcCol.TargetType,
				DiffType:   "MISSING_IN_TARGET",
			})
			continue
		}

		// 对比类型
		if !compareTypes(srcCol.TargetType, tgtCol.TargetType) {
			result.ColumnDiffs = append(result.ColumnDiffs, ColumnDiff{
				ColumnName: srcCol.Name,
				SourceType: srcCol.TargetType,
				TargetType: tgtCol.TargetType,
				DiffType:   "TYPE_MISMATCH",
			})
			continue
		}

		// 对比NULL约束
		if srcCol.IsNullable != tgtCol.IsNullable {
			result.ColumnDiffs = append(result.ColumnDiffs, ColumnDiff{
				ColumnName: srcCol.Name,
				SourceType: fmt.Sprintf("NULL=%v", srcCol.IsNullable),
				TargetType: fmt.Sprintf("NULL=%v", tgtCol.IsNullable),
				DiffType:   "NULL_MISMATCH",
			})
		}
	}
}

// compareIndexes 对比索引差异
func (v *Validator) compareIndexes(source, target *ExtendedTableSchema, result *ValidationResult) {
	targetIdxMap := make(map[string]IndexSchema)
	for _, idx := range target.Indexes {
		targetIdxMap[strings.ToLower(idx.Name)] = idx
	}

	for _, srcIdx := range source.Indexes {
		// 跳过主键索引
		if srcIdx.IsPrimary {
			continue
		}

		tgtIdx, exists := targetIdxMap[strings.ToLower(srcIdx.Name)]
		if !exists {
			result.IndexDiffs = append(result.IndexDiffs, IndexDiff{
				IndexName: srcIdx.Name,
				DiffType:  "MISSING_IN_TARGET",
			})
			continue
		}

		// 对比索引类型
		if srcIdx.IsUnique != tgtIdx.IsUnique {
			result.IndexDiffs = append(result.IndexDiffs, IndexDiff{
				IndexName: srcIdx.Name,
				DiffType:  "TYPE_MISMATCH",
			})
		}
	}
}

// compareConstraints 对比约束差异
func (v *Validator) compareConstraints(source, target *ExtendedTableSchema, result *ValidationResult) {
	targetConsMap := make(map[string]ConstraintSchema)
	for _, cons := range target.Constraints {
		targetConsMap[strings.ToLower(cons.Name)] = cons
	}

	for _, srcCons := range source.Constraints {
		_, exists := targetConsMap[strings.ToLower(srcCons.Name)]
		if !exists {
			result.ConstraintDiffs = append(result.ConstraintDiffs, ConstraintDiff{
				ConstraintName: srcCons.Name,
				DiffType:       "MISSING_IN_TARGET",
			})
		}
	}
}

// compareTypes 对比类型是否匹配（简化比较）
func compareTypes(type1, type2 string) bool {
	// 标准化类型名称
	t1 := strings.ToUpper(type1)
	t2 := strings.ToUpper(type2)

	// 移除长度信息进行比较
	t1 = strings.Split(t1, "(")[0]
	t2 = strings.Split(t2, "(")[0]

	// 常见别名映射
	typeMapping := map[string]string{
		"INT":       "INT",
		"INTEGER":   "INT",
		"BIGINT":    "BIGINT",
		"VARCHAR":   "VARCHAR",
		"CHAR":      "CHAR",
		"TEXT":      "TEXT",
		"DATETIME":  "DATETIME",
		"TIMESTAMP": "TIMESTAMP",
		"DECIMAL":   "DECIMAL",
		"NUMERIC":   "DECIMAL",
		"FLOAT":     "FLOAT",
		"DOUBLE":    "DOUBLE",
	}

	m1, ok1 := typeMapping[t1]
	m2, ok2 := typeMapping[t2]

	if !ok1 || !ok2 {
		return t1 == t2
	}

	return m1 == m2
}

// printValidationResult 打印校验结果
func (v *Validator) printValidationResult(result *ValidationResult) {
	if result.MissingInTarget {
		fmt.Printf("  ✗ Table does not exist in target\n")
		return
	}

	if result.IsValid() {
		fmt.Printf("  ✓ Schema matches\n")
		return
	}

	if len(result.ColumnDiffs) > 0 {
		fmt.Printf("  ✗ Column differences: %d\n", len(result.ColumnDiffs))
		for _, diff := range result.ColumnDiffs {
			fmt.Printf("    - %s: %s\n", diff.ColumnName, diff.DiffType)
		}
	}

	if len(result.IndexDiffs) > 0 {
		fmt.Printf("  ✗ Index differences: %d\n", len(result.IndexDiffs))
		for _, diff := range result.IndexDiffs {
			fmt.Printf("    - %s: %s\n", diff.IndexName, diff.DiffType)
		}
	}

	if len(result.ConstraintDiffs) > 0 {
		fmt.Printf("  ✗ Constraint differences: %d\n", len(result.ConstraintDiffs))
		for _, diff := range result.ConstraintDiffs {
			fmt.Printf("    - %s: %s\n", diff.ConstraintName, diff.DiffType)
		}
	}
}

// PrintValidationSummary 打印校验摘要
func PrintValidationSummary(results []ValidationResult) {
	fmt.Println("\n========================================")
	fmt.Println("         SCHEMA VALIDATION SUMMARY")
	fmt.Println("========================================")

	validCount := 0
	invalidCount := 0
	missingCount := 0

	for _, result := range results {
		if result.MissingInTarget {
			missingCount++
		} else if result.IsValid() {
			validCount++
		} else {
			invalidCount++
		}
	}

	fmt.Printf("Total Tables:    %d\n", len(results))
	fmt.Printf("Valid:           %d\n", validCount)
	fmt.Printf("Invalid:         %d\n", invalidCount)
	fmt.Printf("Missing:         %d\n", missingCount)

	if invalidCount > 0 || missingCount > 0 {
		fmt.Println("\nDetails:")
		for _, result := range results {
			if !result.IsValid() || result.MissingInTarget {
				fmt.Printf("  - %s: ", result.TableName)
				if result.MissingInTarget {
					fmt.Println("MISSING")
				} else {
					parts := []string{}
					if len(result.ColumnDiffs) > 0 {
						parts = append(parts, fmt.Sprintf("%d columns", len(result.ColumnDiffs)))
					}
					if len(result.IndexDiffs) > 0 {
						parts = append(parts, fmt.Sprintf("%d indexes", len(result.IndexDiffs)))
					}
					if len(result.ConstraintDiffs) > 0 {
						parts = append(parts, fmt.Sprintf("%d constraints", len(result.ConstraintDiffs)))
					}
					fmt.Println(strings.Join(parts, ", "))
				}
			}
		}
	}

	fmt.Println("========================================")
}
