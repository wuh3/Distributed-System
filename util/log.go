package util

import (
	"fmt"
	"log"
	"os"
	"sync"
)
// Logger for logging membership list updates to local file
var ProcessLogger *Logger
var LoggerErr error

type LogEntry struct {
	Type    string
	Time    int64
	Message string
}

type Logger struct {
	logFile *os.File
	log     *log.Logger
	logChan chan LogEntry
	wg      sync.WaitGroup
}

func NewLogger(logFilePath string, bufferSize int) (*Logger, error) {
	logFile, err := os.Create(logFilePath)
	if err != nil {
		return nil, err
	}

	logger := &Logger{
		logFile: logFile,
		// create a new logger with two flags and no prefix
		log:     log.New(logFile, "", log.Lmicroseconds|log.Ltime),
		logChan: make(chan LogEntry, bufferSize),
	}

	logger.wg.Add(1)
	go logger.processLogs()

	return logger, nil
}

func (l *Logger) LogJoin(time int64, message string) {
	entry := LogEntry{
		Type:    "JOIN",
		Time:    time,
		Message: message,
	}
	l.logChan <- entry
}

func (l *Logger) LogLeave(time int64, message string) {
	entry := LogEntry{
		Type:    "LEFT",
		Time:    time,
		Message: message,
	}
	l.logChan <- entry
}

func (l *Logger) LogFail(time int64, message string) {
	entry := LogEntry{
		Type:    "FAIL",
		Time:    time,
		Message: message,
	}
	l.logChan <- entry
}

func (l *Logger) LogSUS(time int64, message string) {
	entry := LogEntry{
		Type:    "SUS",
		Time:    time,
		Message: message,
	}
	l.logChan <- entry
}

func (l *Logger) Close() {
	close(l.logChan)
	l.wg.Wait()
	l.logFile.Close()
}

func (l *Logger) processLogs() {
	// use waitgroup to ensure all logs are written before closing
	defer l.wg.Done()

	for entry := range l.logChan {
		logLine := fmt.Sprintf("(%d) [%s] %s ", entry.Time, entry.Type, entry.Message)
		l.log.Println(logLine)
	}
}

func CreateProcessLogger(logName string) {
	ProcessLogger, LoggerErr = NewLogger(logName, 100)
	if LoggerErr != nil {
		fmt.Printf("Error creating logger: %v\n", LoggerErr)
	}
}

