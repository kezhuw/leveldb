package leveldb

import "github.com/kezhuw/leveldb/internal/iterator"

// Iterator defines methods to iterate through a set of data. The initial
// status of an newly created iterator is invalid. Client must call one of
// the seek methods before using. All iterators returned from this package
// are not designed for concurrent usage.
type Iterator interface {
	// Next moves to next entry. It returns whether such entry exists.
	// If it is the first seek method called, it behaves as First().
	Next() bool

	// Prev moves to previous entry. It returns whether such entry exists.
	// If it is the first seek method called, it behaves as Last().
	Prev() bool

	// First moves to the first entry. It returns whether such entry exists.
	First() bool

	// Last moves to the last entry. It returns whether such entry exists.
	Last() bool

	// Seek moves the iterator to the first key that equal to or greater than
	// key. It returns whether such entry exists.
	Seek(key []byte) bool

	// Valid returns whether the iterator is point to a valid entry.
	Valid() bool

	// Key returns the key of current entry. The behaviour is undefined if
	// current status of iterator is invalid.
	Key() []byte

	// Value returns the value of current entry. The behaviour is undefined
	// if current status of iterator is invalid.
	Value() []byte

	// Err returns error we encounters so far. Seek methods, First, Last and Seek may
	// clear this error.
	Err() error

	// Release releases any resources hold by this iterator, and returns
	// any error it encounters so far. The behaviour is undefined if you
	// call any methods after this iterator has been closed.
	Release() error
}

var _ Iterator = (iterator.Iterator)(nil)
var _ iterator.Iterator = (Iterator)(nil)
