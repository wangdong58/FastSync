package database

import (
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/wangdong58/FastSync/internal/config"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

// Connection 数据库连接封装
type Connection struct {
	DB     *sql.DB
	Config config.DBConfig
}

// NewSQLServerConnection 创建SQL Server连接
func NewSQLServerConnection(cfg config.DBConfig) (*Connection, error) {
	query := url.Values{}
	query.Add("database", cfg.Database)
	// 添加超时参数，防止查询挂起
	query.Add("connection+timeout", "60")  // 连接超时60秒
	query.Add("query+timeout", "180")      // 查询超时180秒
	query.Add("dial+timeout", "30")        // 拨号超时30秒
	query.Add("read+timeout", "60")        // 读取超时60秒
	query.Add("write+timeout", "60")       // 写入超时60秒

	// 添加额外参数
	for k, v := range cfg.Params {
		query.Add(k, v)
	}

	connStr := fmt.Sprintf("sqlserver://%s:%s@%s:%d?%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		query.Encode(),
	)

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, fmt.Errorf("open sqlserver connection failed: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.GetConnMaxLifetime())

	// 验证连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlserver failed: %w", err)
	}

	return &Connection{
		DB:     db,
		Config: cfg,
	}, nil
}

// NewMySQLConnection 创建MySQL/OceanBase连接
func NewMySQLConnection(cfg config.DBConfig) (*Connection, error) {
	params := make(map[string]string)

	// 默认参数
	params["charset"] = "utf8mb4"
	params["parseTime"] = "true"
	params["loc"] = "UTC"

	// 合并自定义参数
	for k, v := range cfg.Params {
		params[k] = v
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	// 添加参数
	if len(params) > 0 {
		dsn += "?"
		first := true
		for k, v := range params {
			if !first {
				dsn += "&"
			}
			dsn += fmt.Sprintf("%s=%s", k, url.QueryEscape(v))
			first = false
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql connection failed: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.GetConnMaxLifetime())

	// 验证连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping mysql failed: %w", err)
	}

	return &Connection{
		DB:     db,
		Config: cfg,
	}, nil
}

// Close 关闭连接
func (c *Connection) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}

// Ping 检查连接
func (c *Connection) Ping() error {
	return c.DB.Ping()
}

// GetStats 获取连接池统计
func (c *Connection) GetStats() sql.DBStats {
	return c.DB.Stats()
}

// TableInfo 表信息
type TableInfo struct {
	Schema     string
	Name       string
	Columns    []ColumnInfo
	PrimaryKey string
}

// ColumnInfo 列信息
type ColumnInfo struct {
	Name         string
	DataType     string
	MaxLength    int64
	IsNullable   bool
	IsPrimaryKey bool
}

// GetTableInfo 获取表信息 (SQL Server)
func GetSQLServerTableInfo(db *sql.DB, schema, table string) (*TableInfo, error) {
	info := &TableInfo{
		Schema:  schema,
		Name:    table,
		Columns: make([]ColumnInfo, 0),
	}

	// 查询列信息
	query := fmt.Sprintf(`
		SELECT
			c.name,
			t.name as type_name,
			c.max_length,
			c.is_nullable,
			CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END as is_primary_key
		FROM sys.columns c
		JOIN sys.types t ON c.user_type_id = t.user_type_id
		LEFT JOIN (
			SELECT ic.column_id, ic.object_id
			FROM sys.index_columns ic
			JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
			WHERE i.is_primary_key = 1
		) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
		WHERE c.object_id = OBJECT_ID('%s.%s')
		ORDER BY c.column_id
	`, schema, table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query table columns failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnInfo
		var isPK int
		err := rows.Scan(&col.Name, &col.DataType, &col.MaxLength, &col.IsNullable, &isPK)
		if err != nil {
			return nil, err
		}
		col.IsPrimaryKey = isPK == 1
		if col.IsPrimaryKey {
			info.PrimaryKey = col.Name
		}
		info.Columns = append(info.Columns, col)
	}

	return info, rows.Err()
}

// GetTableRowCount 获取表行数
func GetTableRowCount(db *sql.DB, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var count int64
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

// TruncateTable 清空表
func TruncateTable(db *sql.DB, table string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", table)
	_, err := db.Exec(query)
	return err
}

// GetPrimaryKeyRange 获取主键范围
func GetPrimaryKeyRange(db *sql.DB, table, pkColumn string) (min, max int64, err error) {
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WITH (NOLOCK)", pkColumn, pkColumn, table)
	row := db.QueryRow(query)
	var minVal, maxVal sql.NullInt64
	err = row.Scan(&minVal, &maxVal)
	if err != nil {
		return 0, 0, err
	}
	if minVal.Valid {
		min = minVal.Int64
	}
	if maxVal.Valid {
		max = maxVal.Int64
	}
	return min, max, nil
}

// ExecuteInTransaction 在事务中执行
func ExecuteInTransaction(db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}

// RetryOperation 带重试的操作
func RetryOperation(maxRetries int, interval time.Duration, operation func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = operation()
		if err == nil {
			return nil
		}
		if i < maxRetries-1 {
			time.Sleep(interval)
		}
	}
	return err
}
