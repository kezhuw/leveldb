package iterator

// Iterator has same method set as the exported one, but with fewer
// safety guarantees. The exported interface is a superset of internal
// one. Not all internal iterators can be safely exported to clients.
// Internal functions can't specify exported interface in their signature.
// Callers should be aware of this fact, and don't call any method that
// may lead to undefined behaviours.
type Iterator interface {
	// Next moves to next entry. It returns whether such entry exists.
	// The behaviour is undefined if current status of iterator is invalid.
	Next() bool

	// Prev moves to previous entry. It returns whether such entry exists.
	// The behaviour is undefined if current status of iterator is invalid.
	Prev() bool

	// First moves to the first entry. It returns whether such entry exists.
	First() bool

	// Last moves to the last entry. It returns whether such entry exists.
	Last() bool

	// Seek moves the iterator to the first entry with key >= target.
	// It returns whether such entry exists.
	Seek(target []byte) bool

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
