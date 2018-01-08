package iterator_test

import (
	"testing"

	"github.com/kezhuw/leveldb/internal/iterator"
)

type nextIteratorTest struct {
	entries []iterationEntry
}

var nextIteratorTests = []nextIteratorTest{
	{
		entries: []iterationEntry{},
	},
	{
		entries: []iterationEntry{
			{key: "b"},
			{key: "d"},
			{key: "f"},
			{key: "h"},
		},
	},
}

func appendNext(dst []iterationEntry, it iterator.Iterator) []iterationEntry {
	for iterator.Next(it) {
		dst = append(dst, iterationEntry{
			key:   string(it.Key()),
			value: string(it.Value()),
		})
	}
	return dst
}

func TestNext(t *testing.T) {
	for i, test := range nextIteratorTests {
		it := newSliceIterator(test.entries)
		got := appendNext(nil, it)
		if !matchEntries(got, test.entries) {
			t.Errorf("test=%d got=%v want=%v", i, got, test.entries)
		}
	}
}
