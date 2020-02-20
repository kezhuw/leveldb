package iterator

import "github.com/kezhuw/leveldb/internal/util"

type cleanupIterator struct {
	Iterator
	cleanup func() error
}

func (it *cleanupIterator) Close() (err error) {
	defer func() {
		err = util.FirstError(err, it.cleanup())
	}()
	return it.Iterator.Close()
}

func WithCleanup(iterator Iterator, cleanup func() error) Iterator {
	if cleanup == nil {
		return iterator
	}
	return &cleanupIterator{Iterator: iterator, cleanup: cleanup}
}
