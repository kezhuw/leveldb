package iterator

import "github.com/kezhuw/leveldb/internal/keys"

type limitIterator struct {
	cmp    keys.Comparer
	iter   Iterator
	limit  []byte
	valid  bool
	seeked bool
}

func (it *limitIterator) checkLimit(valid bool) bool {
	if valid && it.cmp.Compare(it.iter.Key(), it.limit) < 0 {
		it.valid = true
		return true
	}
	it.valid = false
	return false
}

func (it *limitIterator) First() bool {
	it.seeked = true
	return it.checkLimit(it.iter.First())
}

func (it *limitIterator) Last() bool {
	it.seeked = true
	switch {
	case it.iter.Seek(it.limit):
		for it.iter.Prev() {
			if it.cmp.Compare(it.iter.Key(), it.limit) < 0 {
				it.valid = true
				return true
			}
		}
	case it.iter.Last():
		// There are two reasons to fall in this case statment:
		// * Iterator has no elements greater than or equal to limit.
		// * Error happens in seeking. In this case, possibility exists
		//   that the last element exceed limit. So we iterate backward
		//   if necessary.
		for it.cmp.Compare(it.iter.Key(), it.limit) >= 0 {
			if !it.iter.Prev() {
				it.valid = false
				return false
			}
		}
		it.valid = true
		return true
	}
	it.valid = false
	return false
}

func (it *limitIterator) Next() bool {
	it.seeked = true
	return it.checkLimit(it.iter.Next())
}

func (it *limitIterator) Seek(target []byte) bool {
	it.seeked = true
	return it.checkLimit(it.iter.Seek(target))
}

func (it *limitIterator) Prev() bool {
	if it.seeked {
		it.valid = it.iter.Prev()
		return it.valid
	}
	return it.Last()
}

func (it *limitIterator) Valid() bool {
	return it.valid
}

func (it *limitIterator) Key() []byte {
	if it.valid {
		return it.iter.Key()
	}
	return nil
}

func (it *limitIterator) Value() []byte {
	if it.valid {
		return it.iter.Value()
	}
	return nil
}

func (it *limitIterator) Err() error {
	return it.iter.Err()
}

func (it *limitIterator) Release() error {
	return it.iter.Release()
}

func newLimitIterator(limit []byte, cmp keys.Comparer, it Iterator) Iterator {
	return &limitIterator{cmp: cmp, iter: it, limit: limit}
}
