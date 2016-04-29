package leveldb

import "github.com/kezhuw/leveldb/internal/errors"

var (
	ErrNotFound  = errors.ErrNotFound // key not found
	ErrDBExists  = errors.ErrDBExists
	ErrDBMissing = errors.ErrDBMissing
	ErrDBClosed  = errors.ErrDBClosed
)

// IsCorrupt returns a boolean indicating whether the error is a corruption error.
func IsCorrupt(err error) bool {
	return errors.IsCorrupt(err)
}
