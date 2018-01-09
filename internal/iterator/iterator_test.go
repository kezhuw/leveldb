package iterator_test

import (
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/kezhuw/leveldb/internal/iterator"
)

var (
	errFoo = errors.New("error foo")
	errBar = errors.New("error bar")
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

type sliceIterator struct {
	index   int
	entries []iterationEntry
}

func newSliceIterator(entries []iterationEntry) iterator.Iterator {
	return &sliceIterator{
		entries: entries,
	}
}

func (it *sliceIterator) Valid() bool {
	i, n := it.index, len(it.entries)
	return i >= 1 && i <= n && it.entries[i-1].err == nil
}

func (it *sliceIterator) Err() error {
	i, n := it.index, len(it.entries)
	if i >= 1 && i <= n {
		return it.entries[i-1].err
	}
	return nil
}

func (it *sliceIterator) Release() error {
	it.entries = nil
	return nil
}

func (it *sliceIterator) released() bool {
	return it.entries == nil
}

func (it *sliceIterator) First() bool {
	it.index = 1
	return it.Valid()
}

func (it *sliceIterator) Last() bool {
	it.index = len(it.entries)
	return it.Valid()
}

func (it *sliceIterator) Next() bool {
	if it.index >= 0 {
		it.index++
	}
	return it.Valid()
}

func (it *sliceIterator) Prev() bool {
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

func (it *sliceIterator) Key() []byte {
	if it.Valid() {
		return []byte(it.entries[it.index-1].key)
	}
	panic("invalid iterator")
}

func (it *sliceIterator) Value() []byte {
	if it.Valid() {
		return []byte(it.entries[it.index-1].value)
	}
	panic("invalid iterator")
}

func (it *sliceIterator) Seek(key []byte) bool {
	i := sort.Search(len(it.entries), func(i int) bool {
		return strings.Compare(it.entries[i].key, string(key)) >= 0
	})
	it.index = i + 1
	return it.Valid()
}

var _ iterator.Iterator = (*sliceIterator)(nil)

func matchEntries(entries []iterationEntry, others []iterationEntry) bool {
	n := len(entries)
	if n != len(others) {
		return false
	}
	for i, e := range entries {
		if !e.Equal(others[i]) {
			return false
		}
	}
	return true
}

func appendForward(entries []iterationEntry, it iterator.Iterator, n int) []iterationEntry {
	if n <= 0 {
		n = math.MaxInt32
	}
	for n > 0 && it.Next() {
		entries = append(entries, iterationEntry{
			key:   string(it.Key()),
			value: string(it.Value()),
		})
		n--
	}
	return entries
}

func appendBackward(entries []iterationEntry, it iterator.Iterator, n int) []iterationEntry {
	if n <= 0 {
		n = math.MaxInt32
	}
	i := len(entries)
	for n > 0 && it.Prev() {
		entries = append(entries, iterationEntry{
			key:   string(it.Key()),
			value: string(it.Value()),
		})
		n--
	}
	reverseds := entries[i:]
	for len(reverseds) >= 2 {
		last := len(reverseds) - 1
		reverseds[0], reverseds[last] = reverseds[last], reverseds[0]
		reverseds = reverseds[1:last]
	}
	return entries
}
