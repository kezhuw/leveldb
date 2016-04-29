package logger

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/kezhuw/leveldb/internal/file"
)

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type LogCloser interface {
	Logger
	io.Closer
}

var Discard LogCloser = nopLogger{}

type nopLogger struct{}

func (nopLogger) Debugf(format string, args ...interface{}) {}
func (nopLogger) Infof(format string, args ...interface{})  {}
func (nopLogger) Warnf(format string, args ...interface{})  {}
func (nopLogger) Errorf(format string, args ...interface{}) {}
func (nopLogger) Close() error                              { return nil }

type nopCloser struct {
	Logger
}

func (nopCloser) Close() error { return nil }

func NopCloser(l Logger) LogCloser {
	return nopCloser{l}
}

func StdLogger(f *os.File) LogCloser {
	return nopCloser{FileLogger(f)}
}

type fileLogger struct {
	file file.WriteCloser
}

func (f *fileLogger) printf(severity, format string, args ...interface{}) {
	var buf bytes.Buffer
	buf.WriteString(severity)
	buf.WriteByte(' ')
	fmt.Fprintf(&buf, format, args...)
	if b := buf.Bytes(); b[len(b)-1] != '\n' {
		buf.WriteByte('\n')
	}
	f.file.Write(buf.Bytes())
}

func (f *fileLogger) Debugf(format string, args ...interface{}) {
	f.printf("DEBUG", format, args...)
}

func (f *fileLogger) Warnf(format string, args ...interface{}) {
	f.printf("WARN", format, args...)
}

func (f *fileLogger) Infof(format string, args ...interface{}) {
	f.printf("INFO", format, args...)
}

func (f *fileLogger) Errorf(format string, args ...interface{}) {
	f.printf("ERROR", format, args...)
}

func (f *fileLogger) Close() error {
	return f.file.Close()
}

func FileLogger(f file.WriteCloser) LogCloser {
	return &fileLogger{f}
}
