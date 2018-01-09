package iterator_test

import (
	"sort"
	"testing"

	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
)

type rangeIteratorRange struct {
	seek  string
	start string
	limit string
}

type rangeIteratorTest struct {
	entries []iterationEntry
	ranges  []rangeIteratorRange
}

var rangeIteratorTests = []rangeIteratorTest{
	{
		ranges: []rangeIteratorRange{
			{},
			{seek: "a", limit: "a"},
			{seek: "a", limit: "e"},
			{seek: "g", limit: "f"},
			{seek: "i", limit: "z"},
			{seek: "A", start: "a"},
			{seek: "c", start: "e"},
			{seek: "g", start: "e"},
			{seek: "a", start: "m"},
			{seek: "A", start: "a", limit: "g"},
			{seek: "e", start: "a", limit: "h"},
			{seek: "i", start: "a", limit: "h"},
			{seek: "g", start: "a", limit: "m"},
			{seek: "g", start: "d", limit: "g"},
			{seek: "k", start: "g", limit: "g"},
			{seek: "a", start: "m", limit: "m"},
			{seek: "x", start: "m", limit: "z"},
			{seek: "e", start: "z", limit: "a"},
		},
	},
	{
		entries: []iterationEntry{
			{key: "c", value: "cv"},
			{key: "e", value: "ev"},
			{key: "g", value: "gv"},
			{key: "i", value: "iv"},
			{key: "k", value: "kv"},
		},
		ranges: []rangeIteratorRange{
			{},
			{seek: "a", limit: "a"},
			{seek: "a", limit: "e"},
			{seek: "g", limit: "f"},
			{seek: "i", limit: "z"},
			{seek: "A", start: "a"},
			{seek: "c", start: "e"},
			{seek: "g", start: "e"},
			{seek: "a", start: "m"},
			{seek: "A", start: "a", limit: "g"},
			{seek: "e", start: "a", limit: "h"},
			{seek: "i", start: "a", limit: "h"},
			{seek: "g", start: "a", limit: "m"},
			{seek: "g", start: "d", limit: "g"},
			{seek: "k", start: "g", limit: "g"},
			{seek: "a", start: "m", limit: "m"},
			{seek: "x", start: "m", limit: "z"},
			{seek: "e", start: "z", limit: "a"},
		},
	},
}

func buildRangeIterator(start, limit string, entries []iterationEntry) iterator.Iterator {
	it := newSliceIterator(entries)
	return iterator.NewRangeIterator([]byte(start), []byte(limit), keys.BytewiseComparator, it)
}

func appendRangeEntries(dst []iterationEntry, start, limit string, entries []iterationEntry) []iterationEntry {
	if len(start) == 0 && len(limit) == 0 {
		return append(dst, entries...)
	}
	n := len(entries)
	i, j := 0, n
	if len(start) != 0 {
		i = sort.Search(n, func(i int) bool {
			return entries[i].key >= start
		})
	}
	if len(limit) != 0 {
		j = sort.Search(n, func(i int) bool {
			return entries[i].key >= limit
		})
	}
	if i >= j {
		return dst
	}
	return append(dst, entries[i:j]...)
}

func TestRangeIteratorFirst(t *testing.T) {
	for i, test := range rangeIteratorTests {
		for j, r := range test.ranges {
			it := buildRangeIterator(r.start, r.limit, test.entries)
			valid := it.First()
			if valid != it.Valid() {
				t.Errorf("test=%d-%d First()=%t Valid()=%t", i, j, valid, it.Valid())
			}
			got, want := iterationEntry{}, iterationEntry{}
			if valid {
				got = iterationEntry{
					key:   string(it.Key()),
					value: string(it.Value()),
				}
			}
			entries := appendRangeEntries(nil, r.start, r.limit, test.entries)
			if len(entries) != 0 {
				want = entries[0]
			}
			if !got.Equal(want) {
				t.Errorf("test=%d-%d got=%v want=%v", i, j, got, want)
			}
		}
	}
}

func TestRangeIteratorLast(t *testing.T) {
	for i, test := range rangeIteratorTests {
		for j, r := range test.ranges {
			it := buildRangeIterator(r.start, r.limit, test.entries)
			valid := it.Last()
			got, want := iterationEntry{}, iterationEntry{}
			if valid {
				got = iterationEntry{
					key:   string(it.Key()),
					value: string(it.Value()),
				}
			}
			entries := appendRangeEntries(nil, r.start, r.limit, test.entries)
			if len(entries) != 0 {
				want = entries[len(entries)-1]
			}
			switch {
			case valid != it.Valid():
				t.Errorf("test=%d-%d Last()=%t Valid()=%t", i, j, valid, it.Valid())
			case !got.Equal(want):
				t.Errorf("test=%d-%d got=%v want=%v", i, j, got, want)
			}
		}
	}
}

func TestRangeIteratorForward(t *testing.T) {
	for i, test := range rangeIteratorTests {
		for j, r := range test.ranges {
			it := buildRangeIterator(r.start, r.limit, test.entries)
			got := appendForward(nil, it, 0)
			want := appendRangeEntries(nil, r.start, r.limit, test.entries)
			if !matchEntries(got, want) {
				t.Errorf("test=%d-%d got=%v want=%v", i, j, got, want)
			}
		}
	}
}

func TestRangeIteratorBackward(t *testing.T) {
	for i, test := range rangeIteratorTests {
		for j, r := range test.ranges {
			it := buildRangeIterator(r.start, r.limit, test.entries)
			got := appendBackward(nil, it, 0)
			want := appendRangeEntries(nil, r.start, r.limit, test.entries)
			if !matchEntries(got, want) {
				t.Errorf("test=%d-%d got=%v want=%v", i, j, got, want)
			}
		}
	}
}

func maxStart(seek, start string) string {
	if start == "" || seek > start {
		return seek
	}
	return start
}

func minLimit(seek, limit string) string {
	if limit == "" || limit > seek {
		return seek
	}
	return limit
}

func TestRangeIteratorSeekForward(t *testing.T) {
	for i, test := range rangeIteratorTests {
		for j, r := range test.ranges {
			if r.seek == "" {
				continue
			}
			it := buildRangeIterator(r.start, r.limit, test.entries)
			var got []iterationEntry
			if it.Seek([]byte(r.seek)) {
				got = append(got, iterationEntry{key: string(it.Key()), value: string(it.Value())})
				got = appendForward(got, it, 0)
			}
			want := appendRangeEntries(nil, maxStart(r.start, r.seek), r.limit, test.entries)
			if !matchEntries(got, want) {
				t.Errorf("test=%d-%d got=%v want=%v", i, j, got, want)
			}
		}
	}
}

func TestRangeIteratorSeekBackward(t *testing.T) {
	for i, test := range rangeIteratorTests {
		for j, r := range test.ranges {
			if r.seek == "" {
				continue
			}
			it := buildRangeIterator(r.start, r.limit, test.entries)
			var got []iterationEntry
			switch {
			case it.Seek([]byte(r.seek)):
				got = appendBackward(got, it, 0)
			case it.Last():
				e := iterationEntry{key: string(it.Key()), value: string(it.Value())}
				got = appendBackward(got, it, 0)
				got = append(got, e)
			}
			want := appendRangeEntries(nil, r.start, minLimit(r.seek, r.limit), test.entries)
			if !matchEntries(got, want) {
				t.Errorf("test=%d-%d got=%v want=%v", i, j, got, want)
			}
		}
	}
}

func TestRangeIteratorRelease(t *testing.T) {
	for i, test := range rangeIteratorTests {
		for j, r := range test.ranges {
			it := newSliceIterator(test.entries)
			rangeIt := iterator.NewRangeIterator([]byte(r.start), []byte(r.limit), keys.BytewiseComparator, it)
			sliceIt := it.(*sliceIterator)
			if released := sliceIt.released(); released {
				t.Errorf("test=%d-%d got=%t want=%t", i, j, released, false)
			}
			rangeIt.Release()
			if released := sliceIt.released(); !released {
				t.Errorf("test=%d-%d-released got=%t want=%t", i, j, released, true)
			}
		}
	}
}
