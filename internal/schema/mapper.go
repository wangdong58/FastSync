package schema

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TypeMapper SQL Server 到 MySQL 类型映射器
type TypeMapper struct {
	mappings map[string]MySQLType
}

// MySQLType MySQL类型定义
type MySQLType struct {
	Type     string
	Size     int
	Signed   bool
	Nullable bool
	Default  string
}

// NewTypeMapper 创建类型映射器
func NewTypeMapper() *TypeMapper {
	mapper := &TypeMapper{
		mappings: make(map[string]MySQLType),
	}
	mapper.initMappings()
	return mapper
}

// initMappings 初始化类型映射表
func (m *TypeMapper) initMappings() {
	// 整数类型
	m.mappings["int"] = MySQLType{Type: "INT"}
	m.mappings["bigint"] = MySQLType{Type: "BIGINT"}
	m.mappings["smallint"] = MySQLType{Type: "SMALLINT"}
	m.mappings["tinyint"] = MySQLType{Type: "TINYINT"}

	// 精确数值类型
	m.mappings["decimal"] = MySQLType{Type: "DECIMAL"}
	m.mappings["numeric"] = MySQLType{Type: "DECIMAL"}
	m.mappings["money"] = MySQLType{Type: "DECIMAL", Size: 19}
	m.mappings["smallmoney"] = MySQLType{Type: "DECIMAL", Size: 10}

	// 近似数值类型
	m.mappings["float"] = MySQLType{Type: "DOUBLE"}
	m.mappings["real"] = MySQLType{Type: "FLOAT"}

	// 日期时间类型
	m.mappings["datetime"] = MySQLType{Type: "DATETIME", Size: 3}
	m.mappings["datetime2"] = MySQLType{Type: "DATETIME", Size: 6}
	m.mappings["smalldatetime"] = MySQLType{Type: "DATETIME"}
	m.mappings["date"] = MySQLType{Type: "DATE"}
	m.mappings["time"] = MySQLType{Type: "TIME"}
	m.mappings["datetimeoffset"] = MySQLType{Type: "TIMESTAMP"}

	// 字符串类型
	m.mappings["varchar"] = MySQLType{Type: "VARCHAR"}
	m.mappings["nvarchar"] = MySQLType{Type: "VARCHAR"}
	m.mappings["char"] = MySQLType{Type: "CHAR"}
	m.mappings["nchar"] = MySQLType{Type: "CHAR"}
	m.mappings["text"] = MySQLType{Type: "TEXT"}
	m.mappings["ntext"] = MySQLType{Type: "LONGTEXT"}

	// 二进制类型
	m.mappings["varbinary"] = MySQLType{Type: "VARBINARY"}
	m.mappings["binary"] = MySQLType{Type: "BINARY"}
	m.mappings["image"] = MySQLType{Type: "LONGBLOB"}

	// 其他类型
	m.mappings["bit"] = MySQLType{Type: "TINYINT", Size: 1}
	m.mappings["uniqueidentifier"] = MySQLType{Type: "CHAR", Size: 36}
	m.mappings["xml"] = MySQLType{Type: "LONGTEXT"}
	m.mappings["sql_variant"] = MySQLType{Type: "TEXT"}
}

// MapType 映射SQL Server类型到MySQL类型
func (m *TypeMapper) MapType(sqlServerType string, maxLength int, precision, scale int) MySQLType {
	sqlServerType = strings.ToLower(sqlServerType)

	mysqlType, exists := m.mappings[sqlServerType]
	if !exists {
		// 默认映射为TEXT
		return MySQLType{Type: "TEXT"}
	}

	// 根据具体情况调整
	switch sqlServerType {
	case "varchar", "nvarchar":
		if maxLength == -1 || maxLength > 65535 {
			mysqlType.Type = "TEXT"
		} else {
			mysqlType.Size = maxLength
		}
	case "varbinary":
		if maxLength == -1 || maxLength > 65535 {
			mysqlType.Type = "LONGBLOB"
		} else {
			mysqlType.Size = maxLength
		}
	case "decimal", "numeric":
		if precision > 0 {
			mysqlType.Type = fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
	case "char", "nchar":
		if maxLength > 0 {
			mysqlType.Size = maxLength
		}
	case "binary":
		if maxLength > 0 {
			mysqlType.Size = maxLength
		}
	}

	return mysqlType
}

// GetMySQLTypeString 获取MySQL类型字符串
func (m *TypeMapper) GetMySQLTypeString(sqlServerType string, maxLength int, precision, scale int) string {
	mysqlType := m.MapType(sqlServerType, maxLength, precision, scale)

	if mysqlType.Size > 0 {
		return fmt.Sprintf("%s(%d)", mysqlType.Type, mysqlType.Size)
	}

	return mysqlType.Type
}

// SchemaDiscovery Schema发现器
type SchemaDiscovery struct {
	db     *sql.DB
	mapper *TypeMapper
}

// NewSchemaDiscovery 创建Schema发现器
func NewSchemaDiscovery(db *sql.DB) *SchemaDiscovery {
	return &SchemaDiscovery{
		db:     db,
		mapper: NewTypeMapper(),
	}
}

// TableSchema 表结构
type TableSchema struct {
	SchemaName  string
	TableName   string
	Columns     []ColumnSchema
	Indexes     []IndexSchema
	PrimaryKey  string   // 主键字段（单个或复合主键的第一个字段，用于分段）
	PrimaryKeys []string // 复合主键的所有字段
}

// ColumnSchema 列结构
type ColumnSchema struct {
	Name         string
	SourceType   string
	TargetType   string
	MaxLength    int64
	Precision    int
	Scale        int
	IsNullable   bool
	IsPrimaryKey bool
	IsIdentity   bool // SQL Server IDENTITY 列
	DefaultValue string
}

// IndexSchema 索引结构
type IndexSchema struct {
	Name        string
	IsUnique    bool
	IsPrimary   bool
	IsClustered bool   // UNIQUE CLUSTERED INDEX 应被视为 MySQL 的主键
	Columns     []string
}

// ConstraintSchema 约束结构（唯一约束、外键等）
type ConstraintSchema struct {
	Name       string
	Type       string   // UNIQUE, FOREIGN KEY, CHECK
	Columns    []string
	RefTable   string   // 外键引用表
	RefColumns []string // 外键引用列
	Definition string   // CHECK 约束定义
}

// ExtendedTableSchema 扩展表结构（包含约束和注释）
type ExtendedTableSchema struct {
	TableSchema
	Constraints      []ConstraintSchema
	ColumnComments   map[string]string // 列注释
	TableComment     string            // 表注释
}

// DiscoverExtendedTable 发现完整表结构（含约束、注释）
func (sd *SchemaDiscovery) DiscoverExtendedTable(schema, table string) (*ExtendedTableSchema, error) {
	// 先获取基本表结构
	ts, err := sd.DiscoverTable(schema, table)
	if err != nil {
		return nil, err
	}

	ets := &ExtendedTableSchema{
		TableSchema:    *ts,
		Constraints:    make([]ConstraintSchema, 0),
		ColumnComments: make(map[string]string),
	}

	// 获取列注释
	if err := sd.discoverColumnComments(ets, schema, table); err != nil {
		// 注释获取失败不影响主流程
		fmt.Printf("Warning: failed to get column comments: %v\n", err)
	}

	// 获取表注释
	if err := sd.discoverTableComment(ets, schema, table); err != nil {
		fmt.Printf("Warning: failed to get table comment: %v\n", err)
	}

	// 获取唯一约束
	if err := sd.discoverUniqueConstraints(ets, schema, table); err != nil {
		fmt.Printf("Warning: failed to get unique constraints: %v\n", err)
	}

	// 获取外键约束
	if err := sd.discoverForeignKeys(ets, schema, table); err != nil {
		fmt.Printf("Warning: failed to get foreign keys: %v\n", err)
	}

	return ets, nil
}

// discoverColumnComments 获取列注释
func (sd *SchemaDiscovery) discoverColumnComments(ets *ExtendedTableSchema, schema, table string) error {
	query := fmt.Sprintf(`
		SELECT
			c.name AS column_name,
			CAST(ep.value AS NVARCHAR(MAX)) AS comment
		FROM sys.columns c
		LEFT JOIN sys.extended_properties ep ON
			c.object_id = ep.major_id
			AND c.column_id = ep.minor_id
			AND ep.name = 'MS_Description'
		WHERE c.object_id = OBJECT_ID('%s.%s')
	`, schema, table)

	rows, err := sd.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, comment string
		if err := rows.Scan(&colName, &comment); err != nil {
			continue
		}
		if comment != "" {
			ets.ColumnComments[colName] = comment
		}
	}

	return rows.Err()
}

// discoverTableComment 获取表注释
func (sd *SchemaDiscovery) discoverTableComment(ets *ExtendedTableSchema, schema, table string) error {
	query := fmt.Sprintf(`
		SELECT CAST(ep.value AS NVARCHAR(MAX)) AS comment
		FROM sys.tables t
		LEFT JOIN sys.extended_properties ep ON
			t.object_id = ep.major_id
			AND ep.minor_id = 0
			AND ep.name = 'MS_Description'
		WHERE t.object_id = OBJECT_ID('%s.%s')
	`, schema, table)

	var comment sql.NullString
	err := sd.db.QueryRow(query).Scan(&comment)
	if err != nil {
		return err
	}
	if comment.Valid {
		ets.TableComment = comment.String
	}
	return nil
}

// discoverUniqueConstraints 获取唯一约束（排除 CLUSTERED 索引，因为它们已被处理为主键）
func (sd *SchemaDiscovery) discoverUniqueConstraints(ets *ExtendedTableSchema, schema, table string) error {
	query := fmt.Sprintf(`
		SELECT
			i.name AS constraint_name,
			c.name AS column_name
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		WHERE i.object_id = OBJECT_ID('%s.%s')
			AND i.is_unique = 1
			AND i.is_primary_key = 0
			AND i.type_desc = 'NONCLUSTERED'
		ORDER BY i.name, ic.key_ordinal
	`, schema, table)

	rows, err := sd.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	constraintMap := make(map[string]*ConstraintSchema)
	for rows.Next() {
		var constraintName, columnName string
		if err := rows.Scan(&constraintName, &columnName); err != nil {
			continue
		}

		if cons, exists := constraintMap[constraintName]; exists {
			cons.Columns = append(cons.Columns, columnName)
		} else {
			constraintMap[constraintName] = &ConstraintSchema{
				Name:    constraintName,
				Type:    "UNIQUE",
				Columns: []string{columnName},
			}
		}
	}

	for _, cons := range constraintMap {
		// 额外检查：如果约束的列与主键列完全相同，则跳过（避免重复）
		if constraintMatchesPrimaryKey(cons.Columns, ets.PrimaryKeys) {
			continue
		}
		ets.Constraints = append(ets.Constraints, *cons)
	}

	return rows.Err()
}

// constraintMatchesPrimaryKey 检查约束列是否与主键列完全匹配
func constraintMatchesPrimaryKey(constraintCols, primaryKeyCols []string) bool {
	if len(constraintCols) != len(primaryKeyCols) {
		return false
	}
	// 创建集合进行比较
	constraintSet := make(map[string]bool)
	for _, col := range constraintCols {
		constraintSet[strings.ToLower(col)] = true
	}
	for _, col := range primaryKeyCols {
		if !constraintSet[strings.ToLower(col)] {
			return false
		}
	}
	return true
}

// discoverForeignKeys 获取外键约束
func (sd *SchemaDiscovery) discoverForeignKeys(ets *ExtendedTableSchema, schema, table string) error {
	query := fmt.Sprintf(`
		SELECT
			fk.name AS constraint_name,
			c.name AS column_name,
			rt.name AS ref_table,
			rc.name AS ref_column
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
		JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
		JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
		WHERE fk.parent_object_id = OBJECT_ID('%s.%s')
		ORDER BY fk.name, fkc.constraint_column_id
	`, schema, table)

	rows, err := sd.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	fkMap := make(map[string]*ConstraintSchema)
	for rows.Next() {
		var constraintName, columnName, refTable, refColumn string
		if err := rows.Scan(&constraintName, &columnName, &refTable, &refColumn); err != nil {
			continue
		}

		if fk, exists := fkMap[constraintName]; exists {
			fk.Columns = append(fk.Columns, columnName)
			fk.RefColumns = append(fk.RefColumns, refColumn)
		} else {
			fkMap[constraintName] = &ConstraintSchema{
				Name:       constraintName,
				Type:       "FOREIGN KEY",
				Columns:    []string{columnName},
				RefTable:   refTable,
				RefColumns: []string{refColumn},
			}
		}
	}

	for _, fk := range fkMap {
		ets.Constraints = append(ets.Constraints, *fk)
	}

	return rows.Err()
}

// GenerateFullCreateTableSQL 生成完整的MySQL建表语句（含索引、约束、注释）
func (ets *ExtendedTableSchema) GenerateFullCreateTableSQL() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", ets.TableName))

	// 列定义
	for i, col := range ets.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}

		// IDENTITY列特殊处理：BIGINT AUTO_INCREMENT 或 INT AUTO_INCREMENT
		colDef := col.TargetType
		if col.IsIdentity {
			// 移除可能的长度限制，添加AUTO_INCREMENT
			colDef = strings.ToUpper(col.TargetType)
			colDef = strings.Split(colDef, "(")[0] // 移除如 (20) 这样的长度限制
			colDef = colDef + " AUTO_INCREMENT"
		}

		// 检查是否有CURRENT_TIMESTAMP默认值
		mysqlDefault := ""
		if !col.IsIdentity && col.DefaultValue != "" {
			mysqlDefault = convertDefaultValue(col.DefaultValue)
		}

		// 如果默认值为CURRENT_TIMESTAMP，使用TIMESTAMP类型
		if mysqlDefault == "CURRENT_TIMESTAMP" && strings.Contains(colDef, "DATETIME") {
			// 提取精度，如 DATETIME(3) -> (3)
			precision := ""
			if strings.Contains(colDef, "(") {
				start := strings.Index(colDef, "(")
				end := strings.Index(colDef, ")")
				if start != -1 && end != -1 {
					precision = colDef[start:end+1]
				}
			}
			// 替换为TIMESTAMP类型，默认值为CURRENT_TIMESTAMP(精度)
			colDef = "TIMESTAMP" + precision
			mysqlDefault = "CURRENT_TIMESTAMP" + precision
		}

		sb.WriteString(fmt.Sprintf("    %s %s", col.Name, colDef))

		// MySQL 要求主键列必须是 NOT NULL
		if !col.IsNullable || col.IsPrimaryKey {
			sb.WriteString(" NOT NULL")
		}

		// 默认值（IDENTITY列不需要DEFAULT）
		if mysqlDefault != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", mysqlDefault))
		}

		// 字段注释
		if comment, ok := ets.ColumnComments[col.Name]; ok && comment != "" {
			sb.WriteString(fmt.Sprintf(" COMMENT '%s'", escapeString(comment)))
		}
	}

	// 主键约束（如果存在且未在列上定义）
	if len(ets.PrimaryKeys) > 0 {
		// 使用复合主键的所有字段
		sb.WriteString(fmt.Sprintf(",\n    PRIMARY KEY (%s)", strings.Join(ets.PrimaryKeys, ", ")))
	} else if ets.PrimaryKey != "" {
		// 单个主键字段（向后兼容）
		sb.WriteString(fmt.Sprintf(",\n    PRIMARY KEY (%s)", ets.PrimaryKey))
	}

	// 唯一约束（跳过与主键列完全相同的约束）
	for _, cons := range ets.Constraints {
		if cons.Type == "UNIQUE" {
			// 跳过与主键列完全相同的唯一约束
			if constraintMatchesPrimaryKey(cons.Columns, ets.PrimaryKeys) {
				continue
			}
			sb.WriteString(fmt.Sprintf(",\n    CONSTRAINT %s UNIQUE (%s)",
				cons.Name, strings.Join(cons.Columns, ", ")))
		}
	}

	sb.WriteString("\n)")

	// 表注释
	if ets.TableComment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT='%s'", escapeString(ets.TableComment)))
	}

	return sb.String()
}

// GenerateCreateIndexSQL 生成创建索引的SQL语句（不包括唯一索引和主键，已在建表时创建）
func (ets *ExtendedTableSchema) GenerateCreateIndexSQL() []string {
	var sqls []string

	for _, idx := range ets.Indexes {
		// 跳过主键索引（已在建表时创建）
		if idx.IsPrimary {
			continue
		}

		// 跳过唯一索引（已在建表时通过 CONSTRAINT 创建）
		if idx.IsUnique {
			continue
		}

		// 跳过 UNIQUE CLUSTERED INDEX（在 MySQL 中作为主键创建）
		if idx.IsUnique && idx.IsClustered {
			continue
		}

		// 只创建普通二级索引
		sql := fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
			idx.Name, ets.TableName, strings.Join(idx.Columns, ", "))
		sqls = append(sqls, sql)
	}

	return sqls
}

// convertDefaultValue 转换SQL Server默认值到MySQL格式
func convertDefaultValue(sqlServerDefault string) string {
	// 移除SQL Server的括号包裹，如 ('0') 或 ((0))
	defaultValue := strings.TrimSpace(sqlServerDefault)
	for strings.HasPrefix(defaultValue, "(") && strings.HasSuffix(defaultValue, ")") {
		defaultValue = strings.TrimPrefix(defaultValue, "(")
		defaultValue = strings.TrimSuffix(defaultValue, ")")
		defaultValue = strings.TrimSpace(defaultValue)
	}

	// 转换getdate()到CURRENT_TIMESTAMP
	if strings.EqualFold(defaultValue, "getdate") ||
		strings.EqualFold(defaultValue, "getdate()") {
		return "CURRENT_TIMESTAMP"
	}

	// 转换newid()到UUID（MySQL没有直接等价物，通常用空字符串或自定义）
	if strings.EqualFold(defaultValue, "newid") ||
		strings.EqualFold(defaultValue, "newid()") {
		return "''"
	}

	// 处理NULL - MySQL中省略DEFAULT即可表示NULL
	if strings.EqualFold(defaultValue, "NULL") {
		return ""
	}

	// 处理空字符串
	if defaultValue == "" {
		return ""
	}

	// 如果是纯数字，直接返回
	if _, err := strconv.ParseFloat(defaultValue, 64); err == nil {
		return defaultValue
	}

	// 如果是带引号的字符串，保留原样
	if (strings.HasPrefix(defaultValue, "'") && strings.HasSuffix(defaultValue, "'")) ||
		(strings.HasPrefix(defaultValue, "\"") && strings.HasSuffix(defaultValue, "\"")) {
		return defaultValue
	}

	// 其他情况，添加单引号作为字符串
	return "'" + defaultValue + "'"
}

// escapeString 转义SQL字符串
func escapeString(s string) string {
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	return s
}

// DiscoverTable 发现单个表结构
func (sd *SchemaDiscovery) DiscoverTable(schema, table string) (*TableSchema, error) {
	ts := &TableSchema{
		SchemaName: schema,
		TableName:  table,
		Columns:    make([]ColumnSchema, 0),
		Indexes:    make([]IndexSchema, 0),
	}

	// 查询列信息
	columnsQuery := fmt.Sprintf(`
		SELECT
			c.name as column_name,
			t.name as data_type,
			c.max_length,
			c.precision,
			c.scale,
			CAST(c.is_nullable AS INT) as is_nullable,
			ISNULL(dc.definition, '') as default_value,
			CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END as is_primary_key,
			CASE WHEN c.is_identity = 1 THEN 1 ELSE 0 END as is_identity
		FROM sys.columns c
		JOIN sys.types t ON c.user_type_id = t.user_type_id
		LEFT JOIN sys.default_constraints dc ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
		LEFT JOIN (
			SELECT ic.column_id, ic.object_id
			FROM sys.index_columns ic
			JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
			WHERE i.is_primary_key = 1
		) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
		WHERE c.object_id = OBJECT_ID('%s.%s')
		ORDER BY c.column_id
	`, schema, table)

	rows, err := sd.db.Query(columnsQuery)
	if err != nil {
		return nil, fmt.Errorf("query columns failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnSchema
		var isPK, isIdentity int
		var isNullable int  // SQL Server BIT 类型用 INT 接收
		err := rows.Scan(
			&col.Name,
			&col.SourceType,
			&col.MaxLength,
			&col.Precision,
			&col.Scale,
			&isNullable,
			&col.DefaultValue,
			&isPK,
			&isIdentity,
		)
		if err != nil {
			return nil, err
		}

		col.IsNullable = isNullable == 1  // 转换为 bool
		col.IsPrimaryKey = isPK == 1
		col.IsIdentity = isIdentity == 1
		if col.IsPrimaryKey {
			ts.PrimaryKey = col.Name
		}

		// 映射目标类型
		col.TargetType = sd.mapper.GetMySQLTypeString(col.SourceType, int(col.MaxLength), col.Precision, col.Scale)

		ts.Columns = append(ts.Columns, col)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	// 查询索引信息
	ts.Indexes, err = sd.DiscoverIndexes(schema, table)
	if err != nil {
		return nil, err
	}

	// 如果没有主键，查找 UNIQUE CLUSTERED INDEX 作为主键
	if ts.PrimaryKey == "" {
		for _, idx := range ts.Indexes {
			if idx.IsUnique && idx.IsClustered && len(idx.Columns) > 0 {
				// 设置复合主键的所有字段
				ts.PrimaryKeys = idx.Columns
				// 第一个列用于分段
				ts.PrimaryKey = idx.Columns[0]
				// 标记该索引为主键
				for i := range ts.Indexes {
					if ts.Indexes[i].Name == idx.Name {
						ts.Indexes[i].IsPrimary = true
						break
					}
				}
				// 同时更新列的 IsPrimaryKey 标记（所有主键列）
				for i := range ts.Columns {
					for _, pkCol := range idx.Columns {
						if ts.Columns[i].Name == pkCol {
							ts.Columns[i].IsPrimaryKey = true
							break
						}
					}
				}
				break
			}
		}
	} else {
		// 有主键时，也填充 PrimaryKeys 数组
		ts.PrimaryKeys = []string{ts.PrimaryKey}
	}

	return ts, nil
}

// DiscoverIndexes 发现表索引
func (sd *SchemaDiscovery) DiscoverIndexes(schema, table string) ([]IndexSchema, error) {
	indexesQuery := fmt.Sprintf(`
		SELECT
			i.name as index_name,
			i.is_unique,
			i.is_primary_key,
			i.type_desc,
			c.name as column_name
		FROM sys.indexes i
		JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		WHERE i.object_id = OBJECT_ID('%s.%s') AND i.name IS NOT NULL
		ORDER BY i.index_id, ic.key_ordinal
	`, schema, table)

	rows, err := sd.db.Query(indexesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexSchema)
	for rows.Next() {
		var indexName, columnName, typeDesc string
		var isUnique, isPrimary bool
		err := rows.Scan(&indexName, &isUnique, &isPrimary, &typeDesc, &columnName)
		if err != nil {
			return nil, err
		}

		if idx, exists := indexMap[indexName]; exists {
			idx.Columns = append(idx.Columns, columnName)
		} else {
			indexMap[indexName] = &IndexSchema{
				Name:        indexName,
				IsUnique:    isUnique,
				IsPrimary:   isPrimary,
				IsClustered: typeDesc == "CLUSTERED",
				Columns:     []string{columnName},
			}
		}
	}

	indexes := make([]IndexSchema, 0, len(indexMap))
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, rows.Err()
}

// DiscoverAllTables 发现所有用户表
func (sd *SchemaDiscovery) DiscoverAllTables() ([]string, error) {
	query := `
		SELECT SCHEMA_NAME(schema_id) + '.' + name
		FROM sys.tables
		WHERE is_ms_shipped = 0
		ORDER BY name
	`

	rows, err := sd.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, rows.Err()
}

// GenerateCreateTableSQL 生成MySQL建表语句
func (ts *TableSchema) GenerateCreateTableSQL() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", ts.TableName))

	for i, col := range ts.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}

		sb.WriteString(fmt.Sprintf("    %s %s", col.Name, col.TargetType))

		if !col.IsNullable {
			sb.WriteString(" NOT NULL")
		}

		if col.IsPrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}

		if col.DefaultValue != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultValue))
		}
	}

	sb.WriteString("\n) DEFAULT CHARSET=utf8mb4")

	return sb.String()
}

// ValueConverter 值转换器
type ValueConverter struct {
	mapper *TypeMapper
}

// NewValueConverter 创建值转换器
func NewValueConverter() *ValueConverter {
	return &ValueConverter{
		mapper: NewTypeMapper(),
	}
}

// Convert 转换值从SQL Server到MySQL格式
func (vc *ValueConverter) Convert(value interface{}, sourceType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	sourceType = strings.ToLower(sourceType)

	switch sourceType {
	case "uniqueidentifier":
		// UUID 转换为字符串
		return fmt.Sprintf("%v", value), nil
	case "bit":
		// BIT 转换为布尔或整数
		if b, ok := value.(bool); ok {
			if b {
				return 1, nil
			}
			return 0, nil
		}
		return value, nil
	case "money", "smallmoney":
		// Money 类型转换为DECIMAL
		return value, nil
	case "datetime", "datetime2", "smalldatetime":
		// 日期时间统一转换为UTC时间
		if t, ok := value.(time.Time); ok {
			return t.UTC(), nil
		}
		return value, nil
	case "datetimeoffset":
		// 带时区的日期时间转换为UTC
		if t, ok := value.(time.Time); ok {
			return t.UTC(), nil
		}
		return value, nil
	case "xml":
		// XML 转换为字符串
		return fmt.Sprintf("%v", value), nil
	default:
		return value, nil
	}
}
