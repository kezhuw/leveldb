package iterator_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
)

var (
	error1 = errors.New("error1")
	error2 = errors.New("error2")
)

type iterationError struct {
	// positive for next, negative for prev
	off int
	err error
}

type mergeIteratorIterationTest struct {
	seeks  []string
	nexts  []int
	prevs  []int
	errors []iterationError
	merges []iterationSlice
	merged []iterationEntry
}

func buildSliceIterators(merges []iterationSlice) []iterator.Iterator {
	iterators := make([]iterator.Iterator, 0, len(merges))
	for _, slice := range merges {
		iterators = append(iterators, newSliceIterator(slice))
	}
	return iterators
}

func buildMergeIterator(merges []iterationSlice) iterator.Iterator {
	iterators := buildSliceIterators(merges)
	return iterator.NewMergeIterator(keys.BytewiseComparator, iterators...)
}

func init() {
	var merged []iterationEntry
	for i, n := 0, len(mergeIteratorIterationTests); i < n; i++ {
		test := &mergeIteratorIterationTests[i]
		merged = merged[:0]
		for _, slice := range test.merges {
			merged = append(merged, slice...)
		}
		sort.Sort(iterationSlice(merged))
		test.merged = append([]iterationEntry(nil), merged...)
	}
}

var mergeIteratorIterationTests = []mergeIteratorIterationTest{
	{
		merges: nil,
	},
	{
		nexts: []int{1, 3, 5, 7, 9},
		prevs: []int{1, 3, 5, 7, 9},
		seeks: []string{"A", "a", "d", "x"},
		merges: []iterationSlice{
			{
				{key: "a", value: "av"},
				{key: "d", value: "dv"},
			},
			{
				{key: "b", value: "bv"},
				{key: "f", value: "fv"},
				{key: "k", value: "kv"},
			},
		},
	},
	{
		nexts: []int{2, 4, 6, 8, 10},
		prevs: []int{2, 4, 6, 8, 10},
		seeks: []string{"A", "a", "b", "d", "k", "x"},
		merges: []iterationSlice{
			{
				{key: "b", value: "bv"},
				{key: "f", value: "fv"},
				{key: "k", value: "kv"},
			},
			{
				{key: "a", value: "av"},
				{key: "d", value: "dv"},
			},
			{
				{key: "b", value: "bv"},
				{key: "c", value: "cv"},
				{key: "g", value: "gv"},
			},
		},
	},
	{
		nexts: []int{2, 4, 6, 8, 10, 15},
		prevs: []int{2, 4, 6, 8, 10, 15},
		seeks: []string{"A", "a", "b", "d", "k", "x"},
		merges: []iterationSlice{
			{
				{key: "b", value: "bv"},
				{key: "c", value: "cv"},
				{key: "g", value: "gv"},
			},
			{
				{key: "e", value: "ev"},
			},
			{
				{key: "b", value: "bv"},
				{key: "f", value: "fv"},
				{key: "k", value: "kv"},
			},
			{
				{key: "a", value: "av"},
				{key: "d", value: "dv"},
			},
		},
	},
	{
		errors: []iterationError{
			{
				off: -10,
				err: nil,
			},
			{
				off: 10,
				err: nil,
			},
		},
		merges: []iterationSlice{
			{
				{key: "a"},
			},
			{
				{key: "e"},
			},
			{
				{key: "h"},
			},
		},
	},
	{
		errors: []iterationError{
			{
				off: 1,
				err: error1,
			},
			{
				off: -3,
				err: error2,
			},
		},
		merges: []iterationSlice{
			{
				{key: "a", err: error1},
				{key: "b"},
			},
			{
				{key: "c"},
			},
			{
				{key: "f", err: error2},
				{key: "g"},
				{key: "k"},
			},
		},
	},
	{
		errors: []iterationError{
			{
				off: 4,
				err: error1,
			},
			{
				off: -5,
				err: error1,
			},
		},
		merges: []iterationSlice{
			{
				{key: "a"},
				{key: "c"},
				{key: "g"},
				{key: "j"},
			},
			{
				{key: "e"},
				{key: "f", err: error1},
				{key: "h"},
				{key: "i"},
			},
			{
				{key: "z"},
			},
		},
	},
}

func TestMergeIteratorFirst(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		if len(test.errors) != 0 {
			continue
		}
		mergeIt := buildMergeIterator(test.merges)
		valid := mergeIt.First()
		hasElements := len(test.merged) != 0
		switch {
		case valid != mergeIt.Valid():
			t.Errorf("test=%d First()=%t Valid()=%t", i, valid, mergeIt.Valid())
		case valid != hasElements:
			t.Errorf("test=%d got=%t want=%t", i, mergeIt.Valid(), hasElements)
		case valid:
			got := iterationEntry{
				key:   string(mergeIt.Key()),
				value: string(mergeIt.Value()),
			}
			want := test.merged[0]
			if !want.Equal(got) {
				t.Errorf("test=%d got=%v want=%v", i, got, want)
			}
		}
	}
}

func TestMergeIteratorLast(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		if len(test.errors) != 0 {
			continue
		}
		mergeIt := buildMergeIterator(test.merges)
		valid := mergeIt.Last()
		hasElements := len(test.merged) != 0
		switch {
		case valid != mergeIt.Valid():
			t.Errorf("test=%d Last()=%t Valid()=%t", i, valid, mergeIt.Valid())
		case valid != hasElements:
			t.Errorf("test=%d got=%t want=%t", i, mergeIt.Valid(), hasElements)
		case valid:
			got := iterationEntry{
				key:   string(mergeIt.Key()),
				value: string(mergeIt.Value()),
			}
			want := test.merged[len(test.merged)-1]
			if !want.Equal(got) {
				t.Errorf("test=%d got=%v want=%v", i, got, want)
			}
		}
	}
}

func TestMergeIteratorForward(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		if len(test.errors) != 0 {
			continue
		}
		mergeIt := buildMergeIterator(test.merges)
		entries := appendForward(nil, mergeIt, 0)
		if !matchEntries(test.merged, entries) {
			t.Errorf("test=%d got=%v want=%v", i, entries, test.merged)
		}
	}
}

func TestMergeIteratorBackward(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		if len(test.errors) != 0 {
			continue
		}
		mergeIt := buildMergeIterator(test.merges)
		entries := appendBackward(nil, mergeIt, 0)
		if !matchEntries(test.merged, entries) {
			t.Errorf("test=%d got=%v want=%v", i, entries, test.merged)
		}
	}
}

func TestMergeIteratorSeek(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		if len(test.errors) != 0 {
			continue
		}
		for j, s := range test.seeks {
			mergeIt := buildMergeIterator(test.merges)
			valid := mergeIt.Seek([]byte(s))
			n := len(test.merged)
			k := sort.Search(n, func(i int) bool {
				return test.merged[i].key >= s
			})
			got, want := "<nil>", "<nil>"
			if k < n {
				want = test.merged[k].key
			}
			if valid {
				got = string(mergeIt.Key())
			}
			switch {
			case valid != mergeIt.Valid():
				t.Errorf("test=%d-%d seek=%q mergeIt.Seek=%t mergeIt.Valid=%t", i, j, s, valid, mergeIt.Valid())
			case got != want:
				t.Errorf("test=%d-%d seek=%q got=%q want=%q", i, j, s, got, want)
			case valid:
				entries := appendBackward(nil, mergeIt, 0)
				if !mergeIt.Seek([]byte(s)) {
					t.Errorf("test=%d-%d-2nd seek=%q got=nil want=%q", i, j, s, test.merged[k].key)
				}
				entries = append(entries, iterationEntry{key: string(mergeIt.Key()), value: string(mergeIt.Value())})
				entries = appendForward(entries, mergeIt, 0)
				if !matchEntries(test.merged, entries) {
					t.Errorf("test=%d-%d seek=%q got=%v want=%v", i, j, s, entries, test.merged)
				}
			}
		}
	}
}

func TestMergeIteratorReverseNext(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		if len(test.errors) != 0 {
			continue
		}
		for j, next := range test.nexts {
			mergeIt := buildMergeIterator(test.merges)
			if next <= 0 || next > len(test.merged) {
				continue
			}
			forwards := appendForward(nil, mergeIt, next)
			forwards = forwards[:len(forwards)-1]
			backwards := appendBackward(nil, mergeIt, 0)
			if !matchEntries(forwards, backwards) {
				t.Errorf("test=%d-%d next=%d got=%v want=%v", i, j, next, backwards, forwards)
			}
		}
	}
}

func TestMergeIteratorReversePrev(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		if len(test.errors) != 0 {
			continue
		}
		for j, next := range test.nexts {
			mergeIt := buildMergeIterator(test.merges)
			if next <= 0 || next > len(test.merged) {
				continue
			}
			backwards := appendBackward(nil, mergeIt, next)
			forwards := []iterationEntry{{key: string(mergeIt.Key()), value: string(mergeIt.Value())}}
			forwards = appendForward(forwards, mergeIt, 0)
			if !matchEntries(forwards, backwards) {
				t.Errorf("test=%d-%d next=%d got=%v want=%v", i, j, next, backwards, forwards)
			}
		}
	}
}

func TestMergeIteratorError(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		for j, e := range test.errors {
			mergeIt := buildMergeIterator(test.merges)
			var direction string
			switch {
			case e.off > 0:
				direction = "forward"
				appendForward(nil, mergeIt, e.off)
			case e.off < 0:
				direction = "backward"
				appendBackward(nil, mergeIt, -e.off)
			}
			if err := mergeIt.Err(); err != e.err {
				t.Errorf("test=%d-%d direction=%s offset=%d got=%q want=%q", i, j, direction, e.off, err, e.err)
			}
		}
	}
}

func TestMergeIteratorReleased(t *testing.T) {
	for i, test := range mergeIteratorIterationTests {
		iterators := buildSliceIterators(test.merges)
		mergeIt := iterator.NewMergeIterator(keys.BytewiseComparator, iterators...)
		for j, it := range iterators {
			sliceIt := it.(*sliceIterator)
			if released := sliceIt.released(); released {
				t.Errorf("test=%d-%d got=%t want=%t", i, j, released, false)
			}
		}
		mergeIt.Release()
		for j, it := range iterators {
			sliceIt := it.(*sliceIterator)
			if released := sliceIt.released(); !released {
				t.Errorf("test=%d-%d-released got=%t want=%t", i, j, released, true)
			}
		}
	}
}
