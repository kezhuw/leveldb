package keys_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/keys"
)

const (
	bytewiseComparatorName = "leveldb.BytewiseComparator"
)

func TestByteWiseComparatorName(t *testing.T) {
	bytewiseComparator := keys.BytewiseComparator
	if name := bytewiseComparator.Name(); name != bytewiseComparatorName {
		t.Errorf("test=comparator-name: got=%q want=%q", name, bytewiseComparatorName)
	}
	if name := bytewiseComparator.Name(); name != bytewiseComparatorName {
		t.Errorf("test=comparator-name-second-call: got=%q want=%q", name, bytewiseComparatorName)
	}
}

type bytewiseCompareTest struct {
	a []byte
	b []byte
	r int
}

var bytewiseCompareTests = []bytewiseCompareTest{
	{
		a: []byte("aaaaaa"),
		b: []byte("aaaaaa"),
		r: 0,
	},
	{
		a: []byte("aaaaaa"),
		b: []byte("aaaaab"),
		r: -1,
	},
	{
		a: []byte("aaaaaa"),
		b: []byte("aaabaa"),
		r: -1,
	},
	{
		a: []byte("bbbbbb"),
		b: []byte("aaaaaa"),
		r: 1,
	},
}

func TestBytewiseComparatorCompare(t *testing.T) {
	comparator := keys.BytewiseComparator
	for i, test := range bytewiseCompareTests {
		got := comparator.Compare(test.a, test.b)
		if got != test.r {
			t.Errorf("test=%d got=%d want=%d", i, got, test.r)
		}
	}
}

type bytewiseComparatorSuccessorTest struct {
	start     []byte
	limit     []byte
	successor []byte
}

var bytewiseComparatorSuccessorTests = []bytewiseComparatorSuccessorTest{
	{
		start:     []byte("aaaaaa"),
		limit:     []byte("aaaaaa"),
		successor: []byte("aaaaaa"),
	},
	{
		start:     []byte("aaaaaa"),
		limit:     []byte("aaaaab"),
		successor: []byte("aaaaaa"),
	},
	{
		start:     []byte("aaaaaa"),
		limit:     []byte("abcdefgh"),
		successor: []byte("aab"),
	},
	{
		start:     []byte("aaaaaa"),
		limit:     []byte("acdefgh"),
		successor: []byte("ab"),
	},
	{
		start:     []byte("aaaaaa"),
		limit:     nil,
		successor: []byte("b"),
	},
	{
		start:     []byte("a\xff\xff"),
		limit:     []byte("a\xff\xff\xff"),
		successor: []byte("a\xff\xff"),
	},
	{
		start:     []byte("a\xf0\xff"),
		limit:     []byte("a\xff\xff\xff"),
		successor: []byte("a\xf1"),
	},
}

func TestBytewiseComparatorSuccessor(t *testing.T) {
	comparator := keys.BytewiseComparator
	for i, test := range bytewiseComparatorSuccessorTests {
		got := comparator.AppendSuccessor(nil, test.start, test.limit)
		if !bytes.Equal(got, test.successor) {
			t.Errorf("test=%d start=%q limit=%q got=%q want=%q", i, test.start, test.limit, got, test.successor)
		}
	}
}

func TestBytewiseComparatorAppendSuccessor(t *testing.T) {
	comparator := keys.BytewiseComparator
	for i, test := range bytewiseComparatorSuccessorTests {
		buf := make([]byte, len(test.start)+i)
		rand.Read(buf)
		got := comparator.AppendSuccessor(buf, test.start, test.limit)
		want := append(buf[:len(buf):len(buf)], test.successor...)
		if !bytes.Equal(got, want) {
			t.Errorf("test=%d start=%q limit=%q buf=%q got=%q want=%q", i, test.start, test.limit, buf, got, want)
		}
	}
}

type bytewisePrefixSuccessorTest struct {
	prefix    []byte
	successor []byte
}

var bytewisePrefixSuccessorTests = []bytewisePrefixSuccessorTest{
	{
		prefix:    []byte("aabbccdd"),
		successor: []byte("aabbccde"),
	},
	{
		prefix:    []byte{0xff, 0xff, 0xff},
		successor: nil,
	},
	{
		prefix:    []byte{0x01, 0x03, 0x04},
		successor: []byte{0x01, 0x03, 0x05},
	},
}

func TestBytewiseComparatorPrefixSuccessor(t *testing.T) {
	comparator := keys.BytewiseComparator
	for i, test := range bytewisePrefixSuccessorTests {
		got := comparator.MakePrefixSuccessor(test.prefix)
		if !bytes.Equal(got, test.successor) {
			t.Errorf("test=%d prefix=%q got=%q want=%q", i, test.prefix, got, test.successor)
		}
	}
}

func testKeyRangeError(t *testing.T, i int, comparator keys.Comparator, test *bytewiseComparatorSuccessorTest) {
	if len(test.limit) != 0 && comparator.Compare(test.start, test.limit) != 0 {
		start, limit := test.limit, test.start
		defer func() {
			r := recover()
			if _, ok := r.(*errors.KeyRangeError); !ok {
				t.Errorf("test=%d start=%v limit=%v got=%v want=KeyRangeError", i, start, limit, r)
			}
		}()
		comparator.AppendSuccessor(nil, start, limit)
	}
}

func TestBytewiseComparatorKeyRangeError(t *testing.T) {
	comparator := keys.BytewiseComparator
	for i, test := range bytewiseComparatorSuccessorTests {
		testKeyRangeError(t, i, comparator, &test)
	}
}
