//go:build integration

package helper

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type Logger struct {
	store  []string
	mu     sync.Mutex
	debug  bool
	hasErr bool
}

func NewLogger() *Logger {
	log := new(Logger)
	log.debug = Debug

	return log
}

func (l *Logger) Infof(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	time := time.Now().Format(time.RFC3339)
	msg := fmt.Sprintf(format, args...)
	logMsg := fmt.Sprintf("[%s] INFO: %s\n", time, msg)

	l.store = append(l.store, logMsg)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.hasErr = true

	time := time.Now().Format(time.RFC3339)
	msg := fmt.Sprintf(format, args...)
	logMsg := fmt.Sprintf("[%s] ERROR: %s\n", time, msg)

	l.store = append(l.store, logMsg)
}

func (l *Logger) Printf(format string, args ...any) {
	l.Infof(format, args...)
}

func (l *Logger) PrintLogs(t *testing.T) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.hasErr {
		t.Fatalf("\n%s", strings.Join(l.store, ""))
	}

	if l.debug {
		fmt.Print(strings.Join(l.store, ""))
	}
}
