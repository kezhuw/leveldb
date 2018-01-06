package iterator

type errorIterator struct {
	err error
}

func (e *errorIterator) Valid() bool { return false }

func (e *errorIterator) First() bool          { return false }
func (e *errorIterator) Last() bool           { return false }
func (e *errorIterator) Next() bool           { panic("leveldb: error iterator: " + e.err.Error()) }
func (e *errorIterator) Prev() bool           { panic("leveldb: error iterator: " + e.err.Error()) }
func (e *errorIterator) Seek(key []byte) bool { return false }

func (e *errorIterator) Key() []byte   { panic("leveldb: error iterator: " + e.err.Error()) }
func (e *errorIterator) Value() []byte { panic("leveldb: error iterator: " + e.err.Error()) }

func (e *errorIterator) Err() error     { return e.err }
func (e *errorIterator) Release() error { return e.err }

// Error returns an error iterator.
//
// This error iterator has following properties:
// * First/Last/Seek/Valid return false.
// * Next/Prev/Key/Value panic.
// * Err/Release return the specified err.
func Error(err error) Iterator {
	return &errorIterator{err}
}
