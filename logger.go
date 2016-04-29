package leveldb

import (
	"github.com/kezhuw/leveldb/internal/logger"
)

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// DiscardLogger is a nop Logger.
var DiscardLogger = logger.Discard

var _ Logger = (logger.Logger)(nil)
var _ logger.Logger = (Logger)(nil)
