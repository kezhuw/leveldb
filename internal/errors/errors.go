package errors

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound           = errors.New("leveldb: key not found")
	ErrDBExists           = errors.New("leveldb: db exists")
	ErrDBMissing          = errors.New("leveldb: missing db")
	ErrDBClosed           = errors.New("leveldb: db closed")
	ErrCorruptWriteBatch  = errors.New("leveldb: corrupt write batch")
	ErrCorruptInternalKey = errors.New("leveldb: corrupt internal key")
	ErrComparatorMismatch = errors.New("leveldb: comparator mismatch")
	ErrOverlappedTables   = errors.New("leveldb: overlapped tables in level 1+")
	ErrBatchTooManyWrites = errors.New("leveldb: too many writes in one batch")
	ErrSnapshotReleased   = errors.New("leveldb: snapshot released")
)

type CorruptionError struct {
	Err        error
	Offset     int64
	Category   string
	FileNumber uint64
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("leveldb: corrupt %s in file %d at %d: %s", e.Category, e.FileNumber, e.Offset, e.Err)
}

func NewCorruption(fileNumber uint64, category string, offset int64, err string) error {
	return &CorruptionError{Err: errors.New(err), Offset: offset, Category: category, FileNumber: fileNumber}
}

func IsCorrupt(err error) bool {
	if _, ok := err.(*CorruptionError); ok {
		return true
	}
	return strings.HasPrefix(err.Error(), "leveldb: corrupt ")
}
