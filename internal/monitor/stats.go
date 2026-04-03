package monitor

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wangdong58/FastSync/internal/logger"
)

// SyncStats 同步统计信息
type SyncStats struct {
	// 总体统计
	StartTime       time.Time
	EndTime         time.Time
	TotalTables     int
	CompletedTables int64 // 改为 int64 以支持原子操作
	TotalRows       int64
	SyncedRows      int64
	FailedRows      int64

	// 当前活跃表统计（多表并发时使用）
	activeTables sync.Map // map[string]*TableStats

	// 速度计算
	mu             sync.RWMutex
	lastSyncedRows int64
	lastUpdateTime time.Time
}

// TableStats 单个表的统计
type TableStats struct {
	TableName   string
	TotalRows   int64
	SyncedRows  int64
	StartTime   time.Time
	IsCompleted bool
}

// NewSyncStats 创建新的统计对象
func NewSyncStats() *SyncStats {
	return &SyncStats{
		lastUpdateTime: time.Now(),
	}
}

// StartTable 开始同步表
func (s *SyncStats) StartTable(tableName string, totalRows int64) *TableStats {
	ts := &TableStats{
		TableName: tableName,
		TotalRows: totalRows,
		StartTime: time.Now(),
	}
	s.activeTables.Store(tableName, ts)
	return ts
}

// CompleteTable 完成表同步
func (s *SyncStats) CompleteTable(tableName string) {
	if ts, ok := s.activeTables.Load(tableName); ok {
		stats := ts.(*TableStats)
		stats.IsCompleted = true
		s.activeTables.Delete(tableName)
		atomic.AddInt64(&s.CompletedTables, 1)
	}
}

// UpdateTableProgress 更新表进度
func (s *SyncStats) UpdateTableProgress(tableName string, synced int64) {
	if ts, ok := s.activeTables.Load(tableName); ok {
		stats := ts.(*TableStats)
		atomic.AddInt64(&stats.SyncedRows, synced)
		atomic.AddInt64(&s.SyncedRows, synced)
	}
}

// GetActiveTables 获取所有活跃表
func (s *SyncStats) GetActiveTables() []*TableStats {
	var tables []*TableStats
	s.activeTables.Range(func(key, value interface{}) bool {
		tables = append(tables, value.(*TableStats))
		return true
	})
	return tables
}
func (s *SyncStats) Start() {
	s.StartTime = time.Now()
	s.lastUpdateTime = s.StartTime
}

// Stop 停止统计
func (s *SyncStats) Stop() {
	s.EndTime = time.Now()
}

// Update 更新统计
func (s *SyncStats) Update(synced, failed int64) {
	atomic.AddInt64(&s.SyncedRows, synced)
	atomic.AddInt64(&s.FailedRows, failed)
}

// GetProgressPercent 获取总体进度百分比
func (s *SyncStats) GetProgressPercent() float64 {
	total := atomic.LoadInt64(&s.TotalRows)
	synced := atomic.LoadInt64(&s.SyncedRows)
	if total == 0 {
		return 0
	}
	return float64(synced) / float64(total) * 100
}

// GetCurrentTableProgressPercent 获取当前表进度百分比（保留方法兼容性）
func (s *SyncStats) GetCurrentTableProgressPercent() float64 {
	return 0
}

// GetActiveTableProgress 获取活跃表的进度
func (s *SyncStats) GetActiveTableProgress(tableName string) float64 {
	if ts, ok := s.activeTables.Load(tableName); ok {
		stats := ts.(*TableStats)
		total := stats.TotalRows
		if total == 0 {
			return 0
		}
		synced := atomic.LoadInt64(&stats.SyncedRows)
		return float64(synced) / float64(total) * 100
	}
	return 0
}

// GetSpeed 获取当前同步速度 (行/秒)
func (s *SyncStats) GetSpeed() float64 {
	s.mu.RLock()
	lastTime := s.lastUpdateTime
	lastRows := s.lastSyncedRows
	s.mu.RUnlock()

	now := time.Now()
	duration := now.Sub(lastTime).Seconds()
	if duration < 0.1 { // 避免除零，最小0.1秒
		return 0
	}

	currentRows := atomic.LoadInt64(&s.SyncedRows)
	if currentRows < lastRows {
		return 0
	}

	speed := float64(currentRows-lastRows) / duration
	return speed
}

// GetAverageSpeed 获取平均同步速度 (行/秒)
func (s *SyncStats) GetAverageSpeed() float64 {
	elapsed := s.GetElapsed().Seconds()
	if elapsed < 0.1 {
		return 0
	}

	synced := atomic.LoadInt64(&s.SyncedRows)
	return float64(synced) / elapsed
}

// GetElapsed 获取已用时间
func (s *SyncStats) GetElapsed() time.Duration {
	if s.EndTime.IsZero() {
		return time.Since(s.StartTime)
	}
	return s.EndTime.Sub(s.StartTime)
}

// GetETA 获取预计完成时间
func (s *SyncStats) GetETA() time.Duration {
	return s.GetETAWithSpeed(s.GetAverageSpeed())
}

// GetETAWithSpeed 使用指定速度获取预计完成时间
func (s *SyncStats) GetETAWithSpeed(speed float64) time.Duration {
	if speed <= 0 {
		return 0
	}

	total := atomic.LoadInt64(&s.TotalRows)
	synced := atomic.LoadInt64(&s.SyncedRows)
	remaining := total - synced

	if remaining <= 0 {
		return 0
	}

	return time.Duration(float64(remaining) / speed * float64(time.Second))
}

// FormatDuration 格式化持续时间
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

// FormatNumber 格式化数字
func FormatNumber(n int64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.2fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.2fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}

// StartReporting 启动定期报告
func (s *SyncStats) StartReporting(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		// 先打印进度（使用旧的时间基准计算速度）
		s.PrintProgress()

		// 再更新基准时间
		s.mu.Lock()
		s.lastSyncedRows = s.SyncedRows
		s.lastUpdateTime = time.Now()
		s.mu.Unlock()
	}
}

// PrintProgress 打印进度
func (s *SyncStats) PrintProgress() {
	// 获取所有活跃表
	activeTables := s.GetActiveTables()
	if len(activeTables) == 0 {
		return
	}

	// 整体进度
	totalSynced := atomic.LoadInt64(&s.SyncedRows)
	totalRows := atomic.LoadInt64(&s.TotalRows)
	overallProgress := float64(0)
	if totalRows > 0 {
		overallProgress = float64(totalSynced) / float64(totalRows) * 100
	}

	speed := s.GetAverageSpeed()
	elapsed := s.GetElapsed()
	eta := s.GetETAWithSpeed(speed)

	completedTables := atomic.LoadInt64(&s.CompletedTables)

	// 显示每个活跃表的进度
	for _, ts := range activeTables {
		tableProgress := float64(0)
		if ts.TotalRows > 0 {
			tableProgress = float64(atomic.LoadInt64(&ts.SyncedRows)) / float64(ts.TotalRows) * 100
		}

		// 提取表名（去掉schema前缀）
		tableName := ts.TableName
		if idx := strings.LastIndex(ts.TableName, "."); idx != -1 {
			tableName = ts.TableName[idx+1:]
		}

		synced := atomic.LoadInt64(&ts.SyncedRows)
		total := ts.TotalRows

		// 格式: Overall: x% (总同步/总记录) [表名] 表进度% (表同步/表记录) | Speed | Elapsed | ETA
		progressMsg := fmt.Sprintf("Overall: %.1f%% (%s/%s rows, %d/%d tables) [%s] %.1f%% (%s/%s) | Speed: %s/s | Elapsed: %s | ETA: %s",
			overallProgress,
			FormatNumber(totalSynced),
			FormatNumber(totalRows),
			completedTables,
			s.TotalTables,
			tableName,
			tableProgress,
			FormatNumber(synced),
			FormatNumber(total),
			FormatNumber(int64(speed)),
			FormatDuration(elapsed),
			FormatDuration(eta),
		)
		logger.Info("  %s", progressMsg)
	}
}

// PrintSummary 打印同步摘要
func (s *SyncStats) PrintSummary() {
	logger.Info("\n========================================")
	logger.Info("           SYNC SUMMARY")
	logger.Info("========================================")
	logger.Info("Total Tables:     %d", s.TotalTables)
	logger.Info("Completed Tables: %d", s.CompletedTables)
	logger.Info("Total Rows:       %s", FormatNumber(s.TotalRows))
	logger.Info("Synced Rows:      %s", FormatNumber(s.SyncedRows))
	logger.Info("Failed Rows:      %s", FormatNumber(s.FailedRows))
	logger.Info("Elapsed Time:     %s", FormatDuration(s.GetElapsed()))
	if s.SyncedRows > 0 {
		avgSpeed := float64(s.SyncedRows) / s.GetElapsed().Seconds()
		logger.Info("Avg Speed:        %s rows/s", FormatNumber(int64(avgSpeed)))
	}
	logger.Info("========================================")
}

// Reporter 报告器
type Reporter struct {
	stats    *SyncStats
	interval time.Duration
	stopChan chan struct{}
}

// NewReporter 创建报告器
func NewReporter(stats *SyncStats, interval time.Duration) *Reporter {
	return &Reporter{
		stats:    stats,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// Start 启动报告
func (r *Reporter) Start() {
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.stats.PrintProgress()
			case <-r.stopChan:
				return
			}
		}
	}()
}

// Stop 停止报告
func (r *Reporter) Stop() {
	close(r.stopChan)
}

// HTTPMonitor HTTP监控
type HTTPMonitor struct {
	stats *SyncStats
	port  int
}

// NewHTTPMonitor 创建HTTP监控
func NewHTTPMonitor(stats *SyncStats, port int) *HTTPMonitor {
	return &HTTPMonitor{
		stats: stats,
		port:  port,
	}
}

// Start 启动HTTP监控服务
func (h *HTTPMonitor) Start() error {
	// 简化的HTTP监控实现
	// 实际实现可以使用 net/http 启动服务器
	return nil
}

// StatusResponse HTTP状态响应
type StatusResponse struct {
	Status         string  `json:"status"`
	Progress       float64 `json:"progress"`
	TableProgress  float64 `json:"table_progress"`
	CurrentTable   string  `json:"current_table"`
	SyncedRows     int64   `json:"synced_rows"`
	TotalRows      int64   `json:"total_rows"`
	Speed          float64 `json:"speed"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	ETASeconds     float64 `json:"eta_seconds"`
}

// GetStatusResponse 获取状态响应
func (s *SyncStats) GetStatusResponse() StatusResponse {
	return StatusResponse{
		Status:         "running",
		Progress:       s.GetProgressPercent(),
		TableProgress:  s.GetCurrentTableProgressPercent(),
		CurrentTable:   "",
		SyncedRows:     s.SyncedRows,
		TotalRows:      s.TotalRows,
		Speed:          s.GetSpeed(),
		ElapsedSeconds: s.GetElapsed().Seconds(),
		ETASeconds:     s.GetETA().Seconds(),
	}
}
