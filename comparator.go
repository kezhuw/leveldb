package leveldb

import "github.com/kezhuw/leveldb/internal/keys"

// Comparator defines a total order over keys in LevelDB database.
// Methods of a comparator may be called by concurrent goroutines.
type Comparator interface {
	// Name returns the name of this comparator. A DB created with one comparator
	// can't be opened using a comparator with different name.
	//
	// Client should switch to a new name if the comparator implementation changes
	// in a way that cause the relative order of any two keys varied.
	//
	// Names starting with "leveldb." are reserved and should not used by clients.
	Name() string

	// Compare returns a value 'less than', 'equal to' or 'greater than' 0 depending
	// on whether a is 'less than', 'equal to' or 'greater than' b.
	Compare(a, b []byte) int

	// AppendSuccessor appends a possibly shortest byte sequence in range [start, limit)
	// to dst. Empty limit acts as infinite large. In particularly, if limit equals to
	// start, it returns append(dst, start).
	AppendSuccessor(dst, start, limit []byte) []byte

	// MakePrefixSuccessor returns a byte sequence 'limit' such that all byte sequences
	// falling in [prefix, limit) have 'prefix' as prefix. Zero length 'limit' acts as
	// infinite large.
	MakePrefixSuccessor(prefix []byte) []byte
}

// BytewiseComparator is an lexicographic ordering comparator, it has same ordring with bytes.Compare.
var BytewiseComparator Comparator = keys.BytewiseComparator

// Ensure keys.Comparator equals to Comparator.
var _ Comparator = (keys.UserComparator)(nil)
var _ keys.UserComparator = (Comparator)(nil)
