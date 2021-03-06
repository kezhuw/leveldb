package keys_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

const (
	internalComparatorName = "leveldb.InternalKeyComparator"
)

var internalComparator keys.Comparator = &keys.InternalComparator{UserKeyComparator: keys.BytewiseComparator}

func TestInternalComparatorName(t *testing.T) {
	if name := internalComparator.Name(); name != internalComparatorName {
		t.Errorf("test=comparator-name got=%q want=%q", name, internalComparatorName)
	}
	if name := internalComparator.Name(); name != internalComparatorName {
		t.Errorf("test=comparator-name-2nd got=%q want=%q", name, internalComparatorName)
	}
	var comparator keys.Comparator = &keys.InternalComparator{UserKeyComparator: keys.BytewiseComparator}
	if name := comparator.Name(); name != internalComparatorName {
		t.Errorf("test=comparator-name-new got=%q want=%q", name, internalComparatorName)
	}
}

var internalComparatorCompareTests = []comparatorCompareTest{
	{
		a: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: 0,
	},
	{
		a: []byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: 0,
	},
	{
		a: []byte{0x0a, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: -1,
	},
	{
		a: []byte{0x1a, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: 1,
	},
	{
		a: []byte{0x0f, 0x01, 0x12, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: -1,
	},
	{
		a: []byte{0x0f, 0x01, 0x12, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x0f, 0x01, 0x22, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: 1,
	},
	{
		a: []byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x0f, 0x00, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: -1,
	},
	{
		a: []byte{0x0f, 0x00, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		b: []byte{0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		r: 1,
	},
}

func TestInternalComparatorCompare(t *testing.T) {
	comparator := internalComparator
	for i, test := range internalComparatorCompareTests {
		got := comparator.Compare(test.a, test.b)
		if got != test.r {
			t.Errorf("test=%d a=%v b=%b got=%d want=%d", i, test.a, test.b, got, test.r)
		}
	}
}

var internalComparatorSuccessorTests = []comparatorSuccessorTest{
	{
		start:     []byte{0x0f, 0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		successor: []byte{0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
	},
	{
		start:     []byte{0x0f, 0x0f, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		limit:     []byte{0x0f, 0x2f, 0x01, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
		successor: []byte{0x0f, 0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
	},
}

func TestInternalComparatorSuccessor(t *testing.T) {
	comparator := internalComparator
	for i, test := range internalComparatorSuccessorTests {
		buf := make([]byte, len(test.start)+i)
		rand.Read(buf)
		got := comparator.AppendSuccessor(buf, test.start, test.limit)
		want := append(buf[:len(buf):len(buf)], test.successor...)
		if !bytes.Equal(got, want) {
			t.Errorf("test=%d start=%v limit=%v got=%v want=%v", i, test.start, test.limit, got, want)
		}
	}
}

func TestInternalComparatorAppendSuccessor(t *testing.T) {
	comparator := internalComparator
	for i, test := range internalComparatorSuccessorTests {
		got := comparator.AppendSuccessor(nil, test.start, test.limit)
		if !bytes.Equal(got, test.successor) {
			t.Errorf("test=%d start=%v limit=%v got=%v want=%v", i, test.start, test.limit, got, test.successor)
		}
	}
}
