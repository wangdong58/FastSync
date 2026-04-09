package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wangdong58/FastSync/internal/compare"
	"github.com/wangdong58/FastSync/internal/config"
	"github.com/wangdong58/FastSync/internal/database"
	"github.com/wangdong58/FastSync/internal/logger"
	"github.com/wangdong58/FastSync/internal/schema"
	"github.com/wangdong58/FastSync/internal/sync"
)

var (
	version   = "1.0.0"
	buildTime = "unknown"
)

func main() {
	var (
		configFile     = flag.String("c", "config.yaml", "配置文件路径")
		compareOnly    = flag.Bool("compare-only", false, "仅执行数据对比，跳过同步")
		tables         = flag.String("tables", "", "指定要同步/对比的表，逗号分隔")
		showVersion    = flag.Bool("v", false, "显示版本信息")
		createSchema   = flag.Bool("create-schema", false, "仅创建表结构，不同步数据")
		validateSchema = flag.Bool("validate-schema", false, "校验源端和目标端表结构")
		dryRun         = flag.Bool("dry-run", false, "仅生成建表脚本，不执行")
		genScript      = flag.String("gen-script", "", "生成建表脚本到指定文件")
		logDir         = flag.String("log-dir", "", "日志目录（默认为当前目录）")
		tableWorkers   = flag.Int("table-workers", 0, "并发同步表数量（配置文件中为sync.table_workers）")
	)

	flag.Parse()

	if *showVersion {
		fmt.Printf("SQL Server to OceanBase Sync Tool v%s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	// 初始化日志
	exePath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get executable path: %v\n", err)
		os.Exit(1)
	}
	logPath := filepath.Dir(exePath)
	if *logDir != "" {
		logPath = *logDir
	}
	if err := logger.InitWithDir(logPath); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	// 加载配置
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		logger.Fatal("Failed to load config: %v", err)
	}

	// 如果命令行指定了 table-workers，覆盖配置文件
	if *tableWorkers > 0 {
		cfg.Sync.TableWorkers = *tableWorkers
	}

	logger.Info("SQL Server to OceanBase Sync Tool v%s", version)
	logger.Info("Job: %s", cfg.JobName)
	logger.Info("Source: %s@%s:%d/%s", cfg.Source.Username, cfg.Source.Host, cfg.Source.Port, cfg.Source.Database)
	logger.Info("Target: %s@%s:%d/%s", cfg.Target.Username, cfg.Target.Host, cfg.Target.Port, cfg.Target.Database)

	// 如果指定了表，覆盖配置
	if *tables != "" {
		tableList := strings.Split(*tables, ",")
		cfg.Tables = make([]config.TableConfig, 0, len(tableList))
		for _, t := range tableList {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			cfg.Tables = append(cfg.Tables, config.TableConfig{
				Source:             t,
				Target:             strings.ToLower(strings.ReplaceAll(t, "dbo.", "")),
				TruncateBeforeSync: true,
			})
		}
	}

	// 连接到数据库
	sourceDB, err := database.NewSQLServerConnection(cfg.Source)
	if err != nil {
		logger.Fatal("Failed to connect to source: %v", err)
	}
	defer sourceDB.Close()
	logger.Info("✓ Connected to source database")

	targetDB, err := database.NewMySQLConnection(cfg.Target)
	if err != nil {
		logger.Fatal("Failed to connect to target: %v", err)
	}
	defer targetDB.Close()
	logger.Info("✓ Connected to target database")

	// 如果只执行对比
	if *compareOnly {
		runCompare(cfg, sourceDB, targetDB)
		return
	}

	// 如果仅创建表结构
	if *createSchema {
		runCreateSchema(cfg, sourceDB, targetDB, *dryRun, *genScript)
		return
	}

	// 如果仅校验表结构
	if *validateSchema {
		runValidateSchema(cfg, sourceDB, targetDB)
		return
	}

	// 执行同步
	runSync(cfg, sourceDB, targetDB)
}

func runSync(cfg *config.Config, sourceDB, targetDB *database.Connection) {
	logger.Info("\nStarting sync with %d workers, batch size: %d, table workers: %d",
		cfg.Sync.Workers, cfg.Sync.BatchSize, cfg.Sync.TableWorkers)

	// 创建同步引擎
	engine := sync.NewEngine(cfg, sourceDB, targetDB)

	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动同步（带panic恢复）
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Fatal("Sync panic: %v", r)
			}
		}()
		done <- engine.Run()
	}()

	// 等待完成或中断
	select {
	case err := <-done:
		if err != nil {
			logger.Fatal("\nSync failed: %v", err)
		}
	case sig := <-sigChan:
		logger.Info("\nReceived signal: %v, stopping...", sig)
		engine.Stop()
		<-done
	}

	// 打印统计
	engine.GetStats().PrintSummary()

	// 如果配置了自动对比，执行对比
	if cfg.Compare.AutoCompare {
		logger.Info("\nRunning automatic data comparison...")
		runCompare(cfg, sourceDB, targetDB)
	}

	logger.Info("\n✓ Sync completed successfully!")
}

func runCompare(cfg *config.Config, sourceDB, targetDB *database.Connection) {
	logger.Info("Starting data comparison...")

	// 构建表对列表
	tablePairs := make([]compare.TablePair, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tablePairs = append(tablePairs, compare.TablePair{
			Source:     t.Source,
			Target:     t.Target,
			SampleRate: cfg.Compare.SampleRate,
		})
	}

	if len(tablePairs) == 0 {
		// 自动发现所有表
		logger.Info("Discovering tables...")
		tables, err := discoverTables(sourceDB.DB)
		if err != nil {
			logger.Error("Failed to discover tables: %v", err)
			return
		}
		for _, t := range tables {
			tablePairs = append(tablePairs, compare.TablePair{
				Source:     t,
				Target:     strings.ToLower(strings.ReplaceAll(t, "dbo.", "")),
				SampleRate: cfg.Compare.SampleRate,
			})
		}
	}

	// 创建对比器
	comparator := compare.NewComparator(sourceDB, targetDB, cfg.Compare.CompareWorkers)

	// 执行对比
	results, err := comparator.CompareTables(tablePairs)
	if err != nil {
		logger.Fatal("Compare failed: %v", err)
	}

	// 打印结果
	compare.PrintCompareSummary(results)
}

func discoverTables(db *sql.DB) ([]string, error) {
	query := `
		SELECT SCHEMA_NAME(schema_id) + '.' + name
		FROM sys.tables
		WHERE is_ms_shipped = 0
		ORDER BY name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}
		tables = append(tables, table)
	}

	return tables, rows.Err()
}

// runCreateSchema 创建表结构
func runCreateSchema(cfg *config.Config, sourceDB, targetDB *database.Connection, dryRun bool, scriptFile string) {
	logger.Info("\nCreating schema for tables...")
	if dryRun {
		logger.Info("(Dry run mode - no changes will be made)")
	}

	// 获取要创建的表
	tables := cfg.Tables
	if len(tables) == 0 {
		// 自动发现所有表
		discovery := schema.NewSchemaDiscovery(sourceDB.DB)
		allTables, err := discovery.DiscoverAllTables()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to discover tables: %v\n", err)
			os.Exit(1)
		}
		for _, t := range allTables {
			tables = append(tables, config.TableConfig{
				Source:             t,
				Target:             strings.ToLower(strings.ReplaceAll(t, "dbo.", "")),
				TruncateBeforeSync: false,
			})
		}
	}

	// 发现所有表结构
	var schemas []*schema.ExtendedTableSchema
	sd := schema.NewSchemaDiscovery(sourceDB.DB)

	for _, table := range tables {
		schemaParts := strings.Split(table.Source, ".")
		var schemaName, tableName string
		if len(schemaParts) == 2 {
			schemaName = schemaParts[0]
			tableName = schemaParts[1]
		} else {
			schemaName = "dbo"
			tableName = schemaParts[0]
		}

		logger.Info("  Discovering: %s.%s", schemaName, tableName)
		s, err := sd.DiscoverExtendedTable(schemaName, tableName)
		if err != nil {
			logger.Error("  Failed to discover %s: %v", table.Source, err)
			continue
		}
		schemas = append(schemas, s)
	}

	// 如果指定了脚本文件，生成脚本
	if scriptFile != "" {
		creator := schema.NewCreator(targetDB.DB)
		script := creator.GenerateCreateTableScript(schemas)
		if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
			logger.Fatal("Failed to write script file: %v", err)
		}
		logger.Info("\n✓ Script saved to: %s", scriptFile)
		return
	}

	// 创建表结构
	creator := schema.NewCreator(targetDB.DB)
	if err := creator.CreateSchemaForTables(schemas, dryRun); err != nil {
		logger.Fatal("\nSchema creation failed: %v", err)
	}

	if dryRun {
		logger.Info("\n✓ Dry run completed")
	} else {
		logger.Info("\n✓ Schema created successfully!")
	}
}

// runValidateSchema 校验表结构
func runValidateSchema(cfg *config.Config, sourceDB, targetDB *database.Connection) {
	logger.Info("\nValidating schema...")

	// 获取要校验的表
	tables := cfg.Tables
	if len(tables) == 0 {
		// 自动发现所有表
		discovery := schema.NewSchemaDiscovery(sourceDB.DB)
		allTables, err := discovery.DiscoverAllTables()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to discover tables: %v\n", err)
			os.Exit(1)
		}
		for _, t := range allTables {
			tables = append(tables, config.TableConfig{
				Source: t,
				Target: strings.ToLower(strings.ReplaceAll(t, "dbo.", "")),
			})
		}
	}

	// 执行校验
	validator := schema.NewValidator(sourceDB.DB, targetDB.DB)
	results, err := validator.ValidateAllTables(tables)
	if err != nil {
		logger.Fatal("Validation failed: %v", err)
	}

	// 打印摘要
	schema.PrintValidationSummary(results)
}
