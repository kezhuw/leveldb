package memtable

// Exported iterator will strip sequence with bigger value.
// Don't do it here.
type memtableIterator struct {
	m     *MemTable
	n     *node
	prevs [maxHeight]*node
}

func (it *memtableIterator) First() bool {
	it.n = it.m.head.nexts[0]
	return it.Valid()
}

func (it *memtableIterator) Last() bool {
	it.m.mutex.RLock()
	it.n = it.m.findLast()
	it.m.mutex.RUnlock()
	return it.Valid()
}

func (it *memtableIterator) Next() bool {
	it.n = it.n.nexts[0]
	return it.Valid()
}

func (it *memtableIterator) Prev() bool {
	it.m.mutex.RLock()
	it.n = it.m.findLessThan(it.n.ikey)
	it.m.mutex.RUnlock()
	return it.Valid()
}

func (it *memtableIterator) Seek(ikey []byte) bool {
	it.m.mutex.RLock()
	it.n, _ = it.m.findGreaterOrEqual(ikey, it.prevs[:])
	it.m.mutex.RUnlock()
	return it.Valid()
}

func (it *memtableIterator) Valid() bool {
	return it.n != nil
}

func (it *memtableIterator) Key() []byte {
	return it.n.ikey
}

func (it *memtableIterator) Value() []byte {
	return it.n.value
}

func (it *memtableIterator) Err() error {
	return nil
}

func (it *memtableIterator) Release() error {
	it.m = nil
	it.n = nil
	return nil
}
