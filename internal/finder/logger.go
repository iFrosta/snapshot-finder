package finder

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Logger writes "2006-01-02 15:04:05 [LEVEL] message" lines — the same shape
// as upstream's log — to every writer (stdout and, optionally, a log file).
type Logger struct {
	mu      sync.Mutex
	w       io.Writer
	Verbose bool
}

func NewLogger(w io.Writer, verbose bool) *Logger { return &Logger{w: w, Verbose: verbose} }

func (l *Logger) logf(level, format string, args ...any) {
	if l == nil || l.w == nil {
		return
	}
	line := fmt.Sprintf("%s [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), level, fmt.Sprintf(format, args...))
	l.mu.Lock()
	_, _ = io.WriteString(l.w, line)
	l.mu.Unlock()
}

func (l *Logger) Debugf(format string, args ...any) {
	if l != nil && l.Verbose {
		l.logf("DEBUG", format, args...)
	}
}
func (l *Logger) Infof(format string, args ...any)  { l.logf("INFO", format, args...) }
func (l *Logger) Warnf(format string, args ...any)  { l.logf("WARNING", format, args...) }
func (l *Logger) Errorf(format string, args ...any) { l.logf("ERROR", format, args...) }
