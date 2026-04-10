package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 主配置结构
type Config struct {
	Version string    `yaml:"version"`
	JobName string    `yaml:"job_name"`
	Source  DBConfig  `yaml:"source"`
	Target  DBConfig  `yaml:"target"`
	Sync    SyncConfig    `yaml:"sync"`
	Tables  []TableConfig `yaml:"tables"`
	Monitor MonitorConfig `yaml:"monitor"`
	Compare CompareConfig `yaml:"compare"`
}

// DBConfig 数据库配置
type DBConfig struct {
	Type            string            `yaml:"type"`
	Host            string            `yaml:"host"`
	Port            int               `yaml:"port"`
	Database        string            `yaml:"database"`
	Username        string            `yaml:"username"`
	Password        string            `yaml:"password"`
	Params          map[string]string `yaml:"params"`
	MaxOpenConns    int               `yaml:"max_open_conns"`
	MaxIdleConns    int               `yaml:"max_idle_conns"`
	ConnMaxLifetime string            `yaml:"conn_max_lifetime"`
}

// SyncConfig 同步配置
type SyncConfig struct {
	Workers       int    `yaml:"workers"`        // 单表内的并发workers
	TableWorkers  int    `yaml:"table_workers"`  // 并发同步的表数量
	BatchSize     int    `yaml:"batch_size"`
	ReadBuffer    int    `yaml:"read_buffer"`
	ChannelBuffer int    `yaml:"channel_buffer"`
	MaxRetries    int    `yaml:"max_retries"`
	RetryInterval string `yaml:"retry_interval"`
}

// TableConfig 表配置
type TableConfig struct {
	Source             string          `yaml:"source"`
	Target             string          `yaml:"target"`
	Columns            []ColumnMapping `yaml:"columns"`
	SplitColumn        string          `yaml:"split_column"`
	Where              string          `yaml:"where"`
	TruncateBeforeSync bool            `yaml:"truncate_before_sync"`
}

// ColumnMapping 列映射
type ColumnMapping struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

// MonitorConfig 监控配置
type MonitorConfig struct {
	ReportInterval string `yaml:"report_interval"`
	EnableHTTP     bool   `yaml:"enable_http"`
	HTTPPort       int    `yaml:"http_port"`
	LogLevel       string `yaml:"log_level"`
}

// CompareConfig 对比配置
type CompareConfig struct {
	AutoCompare     bool    `yaml:"auto_compare"`
	SampleRate      float64 `yaml:"sample_rate"`
	CompareWorkers  int     `yaml:"compare_workers"`
	CountOnly       bool    `yaml:"count_only"` // 仅对比数据量（行数）
}

// LoadConfig 加载配置文件
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file failed: %w", err)
	}

	// 扩展环境变量
	data = expandEnvVars(data)

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse config failed: %w", err)
	}

	// 设置默认值
	config.setDefaults()

	// 验证配置
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &config, nil
}

// expandEnvVars 扩展环境变量 ${VAR} 或 $VAR
func expandEnvVars(data []byte) []byte {
	// 匹配 ${VAR} 或 $VAR
	re := regexp.MustCompile(`\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)
	return re.ReplaceAllFunc(data, func(match []byte) []byte {
		var varName string
		if match[1] == '{' {
			varName = string(match[2 : len(match)-1])
		} else {
			varName = string(match[1:])
		}
		return []byte(os.Getenv(varName))
	})
}

// setDefaults 设置默认值
func (c *Config) setDefaults() {
	if c.Sync.Workers == 0 {
		c.Sync.Workers = 100
	}
	if c.Sync.TableWorkers == 0 {
		c.Sync.TableWorkers = 1 // 默认串行同步表
	}
	if c.Sync.BatchSize == 0 {
		c.Sync.BatchSize = 2000
	}
	if c.Sync.ReadBuffer == 0 {
		c.Sync.ReadBuffer = 5000
	}
	if c.Sync.ChannelBuffer == 0 {
		c.Sync.ChannelBuffer = 10000
	}
	if c.Sync.MaxRetries == 0 {
		c.Sync.MaxRetries = 3
	}
	if c.Sync.RetryInterval == "" {
		c.Sync.RetryInterval = "5s"
	}
	if c.Monitor.ReportInterval == "" {
		c.Monitor.ReportInterval = "10s"
	}
	if c.Monitor.LogLevel == "" {
		c.Monitor.LogLevel = "info"
	}
	if c.Monitor.HTTPPort == 0 {
		c.Monitor.HTTPPort = 8080
	}
	if c.Compare.SampleRate == 0 {
		c.Compare.SampleRate = 0.01
	}
	if c.Compare.CompareWorkers == 0 {
		c.Compare.CompareWorkers = 10
	}
	if c.Source.MaxOpenConns == 0 {
		c.Source.MaxOpenConns = 20
	}
	if c.Source.MaxIdleConns == 0 {
		c.Source.MaxIdleConns = 5
	}
	if c.Target.MaxOpenConns == 0 {
		c.Target.MaxOpenConns = 50
	}
	if c.Target.MaxIdleConns == 0 {
		c.Target.MaxIdleConns = 10
	}
}

// validate 验证配置
func (c *Config) validate() error {
	if c.Source.Host == "" {
		return fmt.Errorf("source host is required")
	}
	if c.Target.Host == "" {
		return fmt.Errorf("target host is required")
	}
	if c.Source.Port == 0 {
		c.Source.Port = 1433 // SQL Server 默认端口
	}
	if c.Target.Port == 0 {
		c.Target.Port = 2881 // OceanBase MySQL 默认端口
	}
	return nil
}

// GetRetryInterval 获取重试间隔
func (c *Config) GetRetryInterval() time.Duration {
	d, _ := time.ParseDuration(c.Sync.RetryInterval)
	if d == 0 {
		return 5 * time.Second
	}
	return d
}

// GetReportInterval 获取报告间隔
func (c *Config) GetReportInterval() time.Duration {
	d, _ := time.ParseDuration(c.Monitor.ReportInterval)
	if d == 0 {
		return 10 * time.Second
	}
	return d
}

// GetConnMaxLifetime 获取连接最大生命周期
func (c *DBConfig) GetConnMaxLifetime() time.Duration {
	d, _ := time.ParseDuration(c.ConnMaxLifetime)
	if d == 0 {
		return time.Hour
	}
	return d
}
