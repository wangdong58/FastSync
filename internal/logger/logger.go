package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Level 日志级别
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// Logger 日志记录器
type Logger struct {
	consoleOut io.Writer
	logFile    *os.File
	fileOut    io.Writer
	level      Level
	mu         sync.Mutex
}

// globalLogger 全局日志实例
var globalLogger *Logger

// Init 初始化全局日志记录器
// exePath: 可执行文件路径，用于确定日志目录
func Init(exePath string) error {
	exeDir := filepath.Dir(exePath)
	logger, err := NewLogger(exeDir, INFO)
	if err != nil {
		return err
	}
	globalLogger = logger
	return nil
}

// InitWithDir 使用指定目录初始化全局日志
func InitWithDir(logDir string) error {
	logger, err := NewLogger(logDir, INFO)
	if err != nil {
		return err
	}
	globalLogger = logger
	return nil
}

// NewLogger 创建日志记录器
// logDir: 日志文件存放目录
// level: 日志级别
func NewLogger(logDir string, level Level) (*Logger, error) {
	// 确保日志目录存在
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir failed: %w", err)
	}

	// 生成日志文件名: sync-tool-YYYYMMDD-HHMMSS.log
	timestamp := time.Now().Format("20060102-150405")
	logFileName := fmt.Sprintf("sync-tool-%s.log", timestamp)
	logFilePath := filepath.Join(logDir, logFileName)

	// 打开日志文件
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file failed: %w", err)
	}

	return &Logger{
		consoleOut: os.Stdout,
		logFile:    logFile,
		fileOut:    logFile,
		level:      level,
	}, nil
}

// Close 关闭日志文件
func (l *Logger) Close() error {
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// log 内部日志记录方法
func (l *Logger) log(level Level, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] [%s] %s\n", timestamp, level.String(), message)

	// 输出到控制台
	if l.consoleOut != nil {
		fmt.Fprint(l.consoleOut, logLine)
	}

	// 输出到文件
	if l.fileOut != nil {
		fmt.Fprint(l.fileOut, logLine)
	}
}

// Debug 记录调试日志
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

// Info 记录信息日志
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

// Warn 记录警告日志
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WARN, format, args...)
}

// Error 记录错误日志
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, format, args...)
}

// Fatal 记录致命错误并退出程序
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
	os.Exit(1)
}

// ========== 全局日志函数 ==========

// Debug 全局调试日志
func Debug(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Debug(format, args...)
	}
}

// Info 全局信息日志
func Info(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Info(format, args...)
	} else {
		// 如果未初始化，直接输出到控制台
		fmt.Printf(format+"\n", args...)
	}
}

// Warn 全局警告日志
func Warn(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Warn(format, args...)
	}
}

// Error 全局错误日志
func Error(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Error(format, args...)
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

// Fatal 全局致命错误日志
func Fatal(format string, args ...interface{}) {
	if globalLogger != nil {
		globalLogger.Fatal(format, args...)
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		os.Exit(1)
	}
}

// Close 关闭全局日志
func Close() error {
	if globalLogger != nil {
		return globalLogger.Close()
	}
	return nil
}

// SetLevel 设置全局日志级别
func SetLevel(level Level) {
	if globalLogger != nil {
		globalLogger.SetLevel(level)
	}
}
