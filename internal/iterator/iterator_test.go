package iterator_test

import (
	"sort"
	"strings"

	"github.com/kezhuw/leveldb/internal/iterator"
)

type iterationEntry struct {
	err   error
	key   string
	value string
}

type iterationSlice []iterationEntry

func (entries iterationSlice) Len() int {
	return len(entries)
}

func (entries iterationSlice) Less(i, j int) bool {
	return strings.Compare(entries[i].key, entries[j].key) < 0
}

func (entries iterationSlice) Swap(i, j int) {
	entries[i], entries[j] = entries[j], entries[i]
}

func (entries iterationSlice) Equal(others iterationSlice) bool {
	if len(entries) != len(others) {
		return false
	}
	i, n := 0, len(entries)
	for i < n && entries[i].Equal(others[i]) {
		i++
	}
	return i == n
}

var _ sort.Interface = (iterationSlice)(nil)

func (entry iterationEntry) Equal(other iterationEntry) bool {
	return entry.key == other.key && entry.value == other.value
}

type entryIterator struct {
	index   int
	entries []iterationEntry
}

func newSliceIterator(entries []iterationEntry) iterator.Iterator {
	return &entryIterator{
		entries: entries,
	}
}

func (it *entryIterator) Valid() bool {
	i, n := it.index, len(it.entries)
	return i >= 1 && i <= n && it.entries[i-1].err == nil
}

func (it *entryIterator) Err() error {
	i, n := it.index, len(it.entries)
	if i >= 1 && i <= n {
		return it.entries[i-1].err
	}
	return nil
}

func (it *entryIterator) Release() error {
	return nil
}

func (it *entryIterator) First() bool {
	it.index = 1
	return it.Valid()
}

func (it *entryIterator) Last() bool {
	it.index = len(it.entries)
	return it.Valid()
}

func (it *entryIterator) Next() bool {
	it.index++
	return it.Valid()
}

func (it *entryIterator) Prev() bool {
	switch it.index {
	case 0:
		return it.Last()
	case 1:
		it.index = -1
	default:
		it.index--
	}
	return it.Valid()
}

func (it *entryIterator) Key() []byte {
	if it.Valid() {
		return []byte(it.entries[it.index-1].key)
	}
	panic("invalid iterator")
}

func (it *entryIterator) Value() []byte {
	if it.Valid() {
		return []byte(it.entries[it.index-1].value)
	}
	panic("invalid iterator")
}

func (it *entryIterator) Seek(key []byte) bool {
	i := sort.Search(len(it.entries), func(i int) bool {
		return strings.Compare(it.entries[i].key, string(key)) >= 0
	})
	it.index = i + 1
	return it.Valid()
}

var _ iterator.Iterator = (*entryIterator)(nil)
