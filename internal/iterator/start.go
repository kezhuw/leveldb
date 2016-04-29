package iterator

import "github.com/kezhuw/leveldb/internal/keys"

type startIterator struct {
	cmp   keys.Comparer
	soi   bool
	iter  Iterator
	valid bool
	start []byte
}

func (it *startIterator) First() bool {
	it.soi = true
	it.valid = it.iter.Seek(it.start)
	return it.valid
}

func (it *startIterator) checkStart(valid bool) bool {
	if valid && it.cmp.Compare(it.iter.Key(), it.start) >= 0 {
		it.valid = true
		return true
	}
	it.valid = false
	return false
}

func (it *startIterator) Last() bool {
	it.soi = true
	return it.checkStart(it.Last())
}

func (it *startIterator) Next() bool {
	if it.soi {
		it.valid = it.iter.Next()
		return it.valid
	}
	return it.First()
}

func (it *startIterator) Prev() bool {
	it.soi = true
	return it.checkStart(it.iter.Prev())
}

func (it *startIterator) Seek(target []byte) bool {
	if it.cmp.Compare(target, it.start) < 0 {
		target = it.start
	}
	it.soi = true
	it.valid = it.iter.Seek(target)
	return it.valid
}

func (it *startIterator) Valid() bool {
	return it.valid
}

func (it *startIterator) Key() []byte {
	if it.Valid() {
		return it.iter.Key()
	}
	return nil
}

func (it *startIterator) Value() []byte {
	if it.Valid() {
		return it.iter.Value()
	}
	return nil
}

func (it *startIterator) Err() error {
	return it.iter.Err()
}

func (it *startIterator) Release() error {
	return it.iter.Release()
}

func newStartIterator(start []byte, cmp keys.Comparer, it Iterator) Iterator {
	return &startIterator{cmp: cmp, iter: it, start: start}
}
