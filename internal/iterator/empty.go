package iterator

type emptyIterator struct {
}

func (*emptyIterator) First() bool      { return false }
func (*emptyIterator) Last() bool       { return false }
func (*emptyIterator) Next() bool       { panic("leveldb: empty iterator") }
func (*emptyIterator) Prev() bool       { panic("leveldb: empty iterator") }
func (*emptyIterator) Seek([]byte) bool { return false }
func (*emptyIterator) Valid() bool      { return false }

func (*emptyIterator) Key() []byte   { panic("leveldb: empty iterator") }
func (*emptyIterator) Value() []byte { panic("leveldb: empty iterator") }

func (*emptyIterator) Err() error     { return nil }
func (*emptyIterator) Release() error { return nil }

var empty = (*emptyIterator)(nil)

// Empty returns an empty iterator.
//
// This empty iterator has following properties:
// * First/Last/Seek/Valid return false.
// * Next/Prev/Key/Value panic.
// * Err/Release return nil.
func Empty() Iterator {
	return empty
}
