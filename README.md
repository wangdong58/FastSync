# FastSync - SQL Server to OceanBase MySQL Sync Tool

高性能数据同步工具，支持从 SQL Server 2016 全量同步数据到 OceanBase MySQL 模式。

High-performance data synchronization tool for migrating data from SQL Server 2016 to OceanBase MySQL mode.

[中文](#功能特性) | [English](#features)

---

## 功能特性

- **高性能并发**：基于 Go 语言实现，支持 100+ Worker 并发同步
- **全量同步**：支持全量数据迁移，基于主键范围分段并行读取
- **多表并发**：支持同时同步多张表，提高整体效率
- **智能分段**：自动检测数据分布，智能选择分段策略
- **批量写入**：批量 INSERT 优化，自动处理 MySQL 参数限制
- **进度监控**：实时显示同步进度、速度、ETA 等统计信息
- **数据对比**：支持源端和目标端数据量对比、抽样校验，对无主键/唯一键表提供特殊对比模式
- **类型映射**：完整的 SQL Server 到 MySQL 类型映射
- **配置驱动**：YAML 配置文件，支持环境变量

## Features

- **High Performance**: Built with Go, supports 100+ concurrent workers
- **Full Migration**: Complete data migration with PK-based range segmentation
- **Multi-table Concurrency**: Sync multiple tables simultaneously for better efficiency
- **Smart Segmentation**: Auto-detects data distribution and chooses optimal strategy
- **Batch Writing**: Optimized batch INSERT with automatic MySQL parameter limit handling
- **Progress Monitoring**: Real-time progress, speed, ETA statistics
- **Data Comparison**: Row count and sample validation; special mode for tables without PK/UK
- **Type Mapping**: Complete SQL Server to MySQL type mapping
- **Configuration Driven**: YAML config with environment variable support

---

## 性能指标 | Performance Metrics

| 指标 | 预期值 |
|------|--------|
| 同步速度 | 100万+ 行/秒（取决于网络和硬件） |
| 并发度 | 100+ Workers 并行 |
| 内存占用 | 流式处理，可控内存使用 |
| 支持数据量 | 十亿级+ |

| Metric | Expected Value |
|--------|----------------|
| Sync Speed | 1M+ rows/sec (network/hardware dependent) |
| Concurrency | 100+ parallel workers |
| Memory Usage | Stream processing, controlled memory usage |
| Data Volume | Billions of rows+ |

---

## 快速开始 | Quick Start

### 1. 安装依赖 | Install Dependencies

```bash
# 下载 Go 依赖 | Download Go dependencies
go mod download
go mod tidy
```

### 2. 编译 | Build

```bash
# 标准编译 | Standard build
make build

# 静态编译（推荐用于部署）| Static build (recommended for deployment)
make build-static

# 跨平台编译 | Cross-platform builds
make build-linux    # Linux AMD64
make build-windows  # Windows AMD64
```

### 3. 配置 | Configuration

复制并编辑配置文件：
Copy and edit the configuration file:

```bash
cp config/example.yaml config.yaml
# 编辑 config.yaml，配置数据库连接信息
# Edit config.yaml and configure database connections
```

### 4. 运行 | Run

```bash
# 使用默认配置 | Use default config
./sync-tool

# 指定配置文件 | Specify config file
./sync-tool -c /path/to/config.yaml

# 只对比数据（跳过同步）| Compare only (skip sync)
./sync-tool --compare-only

# 指定同步特定表 | Sync specific tables
./sync-tool --tables Users,Orders

# 显示版本 | Show version
./sync-tool -v
```

---

## 配置说明 | Configuration Reference

### 完整配置示例 | Full Configuration Example

```yaml
version: "1.0"

job_name: "sqlserver_to_oceanbase_sync"

# 源端配置 (SQL Server) | Source configuration
source:
  type: "sqlserver"
  host: "192.168.1.100"
  port: 1433
  database: "SourceDB"
  username: "sync_user"
  password: "${DB_PASSWORD}"  # 支持环境变量 | Supports env vars
  max_open_conns: 20
  max_idle_conns: 5
  conn_max_lifetime: "1h"

# 目标端配置 (OceanBase MySQL) | Target configuration
target:
  type: "mysql"
  host: "192.168.1.200"
  port: 2881
  database: "TargetDB"
  username: "root"
  password: "${TARGET_DB_PASSWORD}"
  params:
    charset: "utf8mb4"
    parseTime: "true"
    loc: "UTC"
  max_open_conns: 50
  max_idle_conns: 10
  conn_max_lifetime: "1h"

# 同步配置 | Sync configuration
sync:
  workers: 100           # 单表内并发 workers
  table_workers: 4       # 并发同步表数量（多表并发）| Concurrent table workers
  batch_size: 3000       # 批量插入大小 | Batch insert size
  read_buffer: 10000     # 读取缓冲区 | Read buffer size
  channel_buffer: 20000  # 通道缓冲区 | Channel buffer size
  max_retries: 3         # 最大重试次数 | Max retries
  retry_interval: "5s"   # 重试间隔 | Retry interval

# 表映射配置 | Table mapping
tables:
  - source: "dbo.Users"
    target: "users"
    split_column: "UserID"      # 分段字段（可选，默认使用主键）
    truncate_before_sync: true  # 同步前清空目标表
    where: "IsActive = 1"       # 可选的 WHERE 条件
  - source: "dbo.Orders"
    target: "orders"
    truncate_before_sync: true

# 监控配置 | Monitor configuration
monitor:
  report_interval: "5s"   # 报告间隔
  enable_http: false
  log_level: "info"

# 对比配置 | Compare configuration
compare:
  auto_compare: true      # 同步后自动对比
  sample_rate: 0.01       # 抽样率（1%）
  compare_workers: 10     # 对比并发 workers
```

---

## 数据类型映射 | Data Type Mapping

| SQL Server | MySQL/OceanBase | 备注 | Notes |
|------------|-----------------|------|-------|
| INT | INT | | |
| BIGINT | BIGINT | | |
| SMALLINT | SMALLINT | | |
| TINYINT | TINYINT | | |
| DECIMAL(p,s) | DECIMAL(p,s) | | |
| NUMERIC | DECIMAL | | |
| MONEY | DECIMAL(19,4) | | |
| SMALLMONEY | DECIMAL(10,4) | | |
| FLOAT | DOUBLE | | |
| REAL | FLOAT | | |
| VARCHAR(n) | VARCHAR(n) | n>65535 时为 TEXT | TEXT if n>65535 |
| NVARCHAR(n) | VARCHAR(n) | Unicode 转换 | Unicode converted |
| CHAR(n) | CHAR(n) | | |
| NCHAR(n) | CHAR(n) | | |
| TEXT | TEXT | | |
| NTEXT | LONGTEXT | | |
| DATETIME | DATETIME(3) | | |
| DATETIME2 | DATETIME(6) | | |
| SMALLDATETIME | DATETIME | | |
| DATE | DATE | | |
| TIME | TIME | | |
| DATETIMEOFFSET | TIMESTAMP | 带时区 | With timezone |
| UNIQUEIDENTIFIER | CHAR(36) | UUID | UUID |
| BIT | TINYINT(1) | | |
| VARBINARY | VARBINARY | | |
| BINARY | BINARY | | |
| IMAGE | LONGBLOB | | |
| XML | LONGTEXT | | |
| SQL_VARIANT | TEXT | | |

---

## 数据对比 | Data Comparison

工具支持三种数据对比模式：
The tool supports three data comparison modes:

### 1. 行数对比 | Row Count Comparison

对比源端和目标端的记录数。
Compares source and target row counts.

### 2. 抽样对比（有主键/唯一键）| Sample Comparison (with PK/UK)

对表进行抽样，逐行对比数据内容。
Samples rows and compares data content.

```
Table: dbo.Users -> users [OK]
  Row Count: 100000000 vs 100000000
  Sampled: 1000000
  Matched: 999999, Mismatched: 1
  Source Only: 0, Target Only: 0
```

### 3. 简化对比（无主键/唯一键）| Simplified Comparison (no PK/UK)

对于无主键和唯一键的表，对比行数和每列的 Min/Max 值。
For tables without PK or UK, compares row counts and column Min/Max values.

```
Table: dbo.Logs -> logs [OK] [NO_PK_UK]
  Row Count: 5000000 vs 5000000
  Column Min/Max Comparison:
    CreateTime: Min[OK] Max[OK]
    Value: Min[DIFF] Max[OK]
      Source Min: 0.01 | Target Min: 0.02
```

---

## 监控输出 | Monitoring Output

同步过程中会实时输出进度：
Real-time progress during sync:

```
[1/15] Syncing table: Users -> users
  Estimated rows: 100,000,000, Primary key: UserID
  Target table truncated
  Progress: 25.3% | 25,300,000/100,000,000 rows | Speed: 1,480,000 rows/s | Elapsed: 17s | ETA: 50s
```

---

## 项目结构 | Project Structure

```
FastSync/
├── cmd/sync/
│   └── main.go              # 入口程序 | Entry point
├── internal/
│   ├── config/              # 配置解析 | Configuration parsing
│   ├── database/            # 数据库连接 | Database connections
│   ├── schema/              # Schema 发现和类型映射 | Schema discovery & type mapping
│   ├── sync/                # 同步引擎 | Sync engine
│   ├── monitor/             # 进度监控 | Progress monitoring
│   ├── compare/             # 数据对比 | Data comparison
│   └── logger/              # 日志记录 | Logging
├── config/
│   └── example.yaml         # 配置示例 | Example configuration
├── go.mod
├── Makefile
├── LICENSE
└── README.md
```

---

## 架构说明 | Architecture

### 同步流程 | Sync Flow

1. **配置加载** | Load YAML configuration (with env var expansion)
2. **连接建立** | Connect to source (SQL Server) and target (MySQL/OceanBase)
3. **表发现** | Discover tables (if not specified in config)
4. **数据同步** | For each table:
   - Discover schema and primary key
   - Get row count
   - Check data distribution (min/max PK range vs row count)
   - **If uneven** (range/rows > 100): Use sequential mode
   - **If even**: Use split mode with parallel workers
5. **批量写入** | Batch insert to target (respects MySQL 65535 parameter limit)
6. **数据对比** | Optional: Compare source/target data

### 并发模型 | Concurrency Model

- **表级并发**：`table_workers` 控制同时同步的表数量
- **表内并发**：`workers` 控制单表内的分段并发读取
- **智能分段**：基于主键范围自动分段，避免数据热点

- **Table-level**: `table_workers` controls concurrent table sync
- **Intra-table**: `workers` controls segmented parallel reads
- **Smart segmentation**: PK-based range segmentation avoids hotspots

---

## 常见问题 | FAQ

### Q: 支持增量同步吗？
**A:** 当前版本仅支持全量同步。增量同步可以通过配置 WHERE 条件实现部分数据同步。

### Q: 如何处理无主键表？
**A:** 无主键表可以使用 `split_column` 指定分段字段，或者使用单线程顺序同步。

### Q: 同步过程中断可以恢复吗？
**A:** 当前版本不支持断点续传，需要重新同步。建议根据业务低峰期执行同步。

### Q: Does it support incremental sync?
**A:** Current version only supports full sync. Partial sync can be done via WHERE clause.

### Q: How to handle tables without primary key?
**A:** Use `split_column` to specify a segmentation column, or use sequential mode.

### Q: Can sync resume after interruption?
**A:** Current version doesn't support resume. Need to restart. Schedule sync during low-traffic periods.

---

## 依赖 | Dependencies

- [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb) - SQL Server driver
- [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) - MySQL driver
- [gopkg.in/yaml.v3](https://gopkg.in/yaml.v3) - YAML parsing

---

## 贡献 | Contributing

欢迎提交 Issue 和 Pull Request！

Issues and Pull Requests are welcome!

### 开发环境 | Development Environment

```bash
# 克隆仓库 | Clone repo
git clone https://github.com/yourusername/FastSync.git
cd FastSync

# 安装依赖 | Install deps
go mod download

# 运行测试 | Run tests
make test

# 格式化代码 | Format code
make fmt

# 运行静态检查 | Run lint
make lint
```

---

## 许可证 | License

MIT License

Copyright (c) 2026 SQL Server to OceanBase Sync Tool

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.

---

## 联系方式 | Contact

如有问题或建议，欢迎通过以下方式联系：

For questions or suggestions:

- GitHub Issues: [https://github.com/yourusername/FastSync/issues](https://github.com/yourusername/FastSync/issues)
- Email: 8442161@qq.com

---

**Star ⭐ 这个项目如果它对你有帮助！**

**Star ⭐ this project if it helps you!**
