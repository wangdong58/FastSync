package schema

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
)

// Creator 结构创建器
type Creator struct {
	db *sql.DB
}

// NewCreator 创建新的结构创建器
func NewCreator(db *sql.DB) *Creator {
	return &Creator{db: db}
}

// CreateTable 在目标库创建表
func (c *Creator) CreateTable(schema *ExtendedTableSchema) error {
	// 生成并执行建表语句
	sql := schema.GenerateFullCreateTableSQL()
	if _, err := c.db.Exec(sql); err != nil {
		return fmt.Errorf("create table failed: %w\nSQL: %s", err, sql)
	}
	return nil
}

// CreateIndexes 创建索引（二级索引）
func (c *Creator) CreateIndexes(schema *ExtendedTableSchema) error {
	// 获取索引创建SQL
	sqls := schema.GenerateCreateIndexSQL()
	for _, sql := range sqls {
		if _, err := c.db.Exec(sql); err != nil {
			// 忽略索引已存在的错误
			if !isDuplicateError(err) {
				return fmt.Errorf("create index failed: %w\nSQL: %s", err, sql)
			}
		}
	}
	return nil
}

// CreateConstraints 创建约束（外键）
func (c *Creator) CreateConstraints(schema *ExtendedTableSchema) error {
	// 创建外键约束
	for _, cons := range schema.Constraints {
		if cons.Type == "FOREIGN KEY" {
			sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				schema.TableName,
				cons.Name,
				strings.Join(cons.Columns, ", "),
				cons.RefTable,
				strings.Join(cons.RefColumns, ", "))

			if _, err := c.db.Exec(sql); err != nil {
				// 外键创建失败打印警告但不中断
				fmt.Printf("Warning: failed to create foreign key %s: %v\n", cons.Name, err)
			}
		}
	}
	return nil
}

// AddColumnComments 添加字段注释（MySQL 5.6+ 支持）
func (c *Creator) AddColumnComments(schema *ExtendedTableSchema) error {
	// 在 GenerateFullCreateTableSQL 中已经包含了 COMMENT
	// 这里提供一个单独的修改注释方法
	for colName, comment := range schema.ColumnComments {
		if comment == "" {
			continue
		}
		sql := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s COMMENT '%s'",
			schema.TableName,
			colName,
			getColumnType(schema, colName),
			escapeString(comment))

		if _, err := c.db.Exec(sql); err != nil {
			fmt.Printf("Warning: failed to add comment for column %s: %v\n", colName, err)
		}
	}
	return nil
}

// CreateSchemaForTables 为一组表创建结构
func (c *Creator) CreateSchemaForTables(schemas []*ExtendedTableSchema, dryRun bool) error {
	for _, schema := range schemas {
		fmt.Printf("Creating table: %s\n", schema.TableName)

		if dryRun {
			// 仅打印SQL
			fmt.Printf("  Table SQL:\n%s;\n\n", schema.GenerateFullCreateTableSQL())

			indexSQLs := schema.GenerateCreateIndexSQL()
			for _, sql := range indexSQLs {
				fmt.Printf("  Index SQL:\n%s;\n", sql)
			}
			fmt.Println()
			continue
		}

		// 创建表
		if err := c.CreateTable(schema); err != nil {
			return fmt.Errorf("create table %s failed: %w", schema.TableName, err)
		}
		fmt.Printf("  ✓ Table created\n")

		// 创建索引
		if err := c.CreateIndexes(schema); err != nil {
			return fmt.Errorf("create indexes for %s failed: %w", schema.TableName, err)
		}
		fmt.Printf("  ✓ Indexes created\n")

		// 创建外键约束
		if err := c.CreateConstraints(schema); err != nil {
			return fmt.Errorf("create constraints for %s failed: %w", schema.TableName, err)
		}
		fmt.Printf("  ✓ Constraints created\n")
	}

	return nil
}

// getColumnType 获取列的数据类型
func getColumnType(schema *ExtendedTableSchema, colName string) string {
	for _, col := range schema.Columns {
		if col.Name == colName {
			return col.TargetType
		}
	}
	return "VARCHAR(255)"
}

// isDuplicateError 检查是否是重复对象错误
func isDuplicateError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "duplicate") ||
		strings.Contains(errStr, "already exists") ||
		strings.Contains(errStr, "1061") // MySQL error code for duplicate key
}

// GenerateCreateTableScript 生成建表脚本（用于输出或保存）
func (c *Creator) GenerateCreateTableScript(schemas []*ExtendedTableSchema) string {
	var sb strings.Builder

	for _, schema := range schemas {
		sb.WriteString(fmt.Sprintf("-- Table: %s\n", schema.TableName))
		sb.WriteString(schema.GenerateFullCreateTableSQL())
		sb.WriteString(";\n\n")

		// 索引
		indexSQLs := schema.GenerateCreateIndexSQL()
		for _, sql := range indexSQLs {
			sb.WriteString(sql)
			sb.WriteString(";\n")
		}

		// 外键
		for _, cons := range schema.Constraints {
			if cons.Type == "FOREIGN KEY" {
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);\n",
					schema.TableName,
					cons.Name,
					strings.Join(cons.Columns, ", "),
					cons.RefTable,
					strings.Join(cons.RefColumns, ", ")))
			}
		}

		sb.WriteString("\n")
	}

	return sb.String()
}

// SaveCreateTableScript 保存建表脚本到文件
func (c *Creator) SaveCreateTableScript(schemas []*ExtendedTableSchema, filename string) error {
	script := c.GenerateCreateTableScript(schemas)
	return os.WriteFile(filename, []byte(script), 0644)
}
