package iterator_test

import (
	"testing"

	"github.com/kezhuw/leveldb/internal/iterator"
)

type nextIteratorTest struct {
	entries []iterationEntry
}

var nextIteratorTests = []nextIteratorTest{
	{},
	{
		entries: []iterationEntry{
			{key: "b"},
			{key: "d"},
			{key: "f"},
			{key: "h"},
		},
	},
	{
		entries: []iterationEntry{
			{key: "b"},
			{key: "d"},
			{err: errFoo, key: "e"},
			{key: "f"},
			{key: "h"},
		},
	},
}

func appendNext(dst []iterationEntry, it iterator.Iterator) ([]iterationEntry, error) {
	for iterator.Next(it) {
		dst = append(dst, iterationEntry{
			key:   string(it.Key()),
			value: string(it.Value()),
		})
	}
	return dst, it.Err()
}

func appendEntries(dst []iterationEntry, entries []iterationEntry) ([]iterationEntry, error) {
	for i, e := range entries {
		if e.err != nil {
			return append(dst, entries[:i]...), e.err
		}
	}
	return append(dst, entries...), nil
}

func TestNext(t *testing.T) {
	for i, test := range nextIteratorTests {
		it := newSliceIterator(test.entries)
		gotEntries, gotErr := appendNext(nil, it)
		wantEntries, wantErr := appendEntries(nil, test.entries)
		if !(matchEntries(gotEntries, wantEntries) && gotErr == wantErr) {
			t.Errorf("test=%d got=(%v,%s) want=(%v,%s)", i, gotEntries, gotErr, wantEntries, wantErr)
		}
	}
}
