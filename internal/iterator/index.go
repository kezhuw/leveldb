package iterator

type indexIterator struct {
	err    error
	data   Iterator
	index  Iterator
	blockf func([]byte) Iterator
}

var _ Iterator = (*indexIterator)(nil)

func (it *indexIterator) First() bool {
	if !it.index.First() {
		it.err = it.index.Err()
		it.clearData()
		return false
	}
	it.err = nil
	it.resetData()
	return it.checkNext(it.data.First())
}

func (it *indexIterator) Last() bool {
	if !it.index.Last() {
		it.err = it.index.Err()
		it.clearData()
		return false
	}
	it.err = nil
	it.resetData()
	return it.checkPrev(it.data.Last())
}

func (it *indexIterator) Seek(key []byte) bool {
	if !it.index.Seek(key) {
		it.err = it.index.Err()
		it.clearData()
		return false
	}
	it.err = nil
	it.resetData()
	return it.checkNext(it.data.Seek(key))
}

func (it *indexIterator) Next() bool {
	return it.checkNext(it.data.Next())
}

func (it *indexIterator) Prev() bool {
	return it.checkPrev(it.data.Prev())
}

func (it *indexIterator) Valid() bool {
	return it.data.Valid()
}

func (it *indexIterator) Key() []byte {
	return it.data.Key()
}

func (it *indexIterator) Value() []byte {
	return it.data.Value()
}

func (it *indexIterator) Err() error {
	return it.err
}

func (it *indexIterator) Release() error {
	it.data.Release()
	it.data = empty
	it.index.Release()
	it.index = empty
	return it.err
}

func (it *indexIterator) clearData() {
	it.data.Release()
	it.data = empty
}

func (it *indexIterator) resetData() {
	it.data.Release()
	it.data = it.blockf(it.index.Value())
}

func (it *indexIterator) checkNext(valid bool) bool {
	for !valid {
		if err := it.data.Err(); err != nil {
			it.err = err
			it.clearData()
			return false
		}
		if !it.index.Next() {
			it.err = it.index.Err()
			it.clearData()
			return false
		}
		it.resetData()
		valid = it.data.First()
	}
	return true
}

func (it *indexIterator) checkPrev(valid bool) bool {
	for !valid {
		if err := it.data.Err(); err != nil {
			it.err = err
			it.clearData()
			return false
		}
		if !it.index.Prev() {
			it.err = it.index.Err()
			it.clearData()
			return false
		}
		it.resetData()
		valid = it.data.Last()
	}
	return true
}

func NewIndexIterator(index Iterator, blockf func([]byte) Iterator) Iterator {
	return &indexIterator{data: empty, index: index, blockf: blockf}
}
