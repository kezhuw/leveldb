package keys_test

import (
	"bytes"
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

type bytewiseComparer struct{}

func (bytewiseComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

var bytesComparer keys.Comparer = bytewiseComparer{}

type comparerTest struct {
	a   []byte
	b   []byte
	min []byte
	max []byte
}

var comparerTests = []comparerTest{
	{
		a: []byte("aaaa"),
		b: []byte("aaaa"),
	},
	{
		a: []byte("aaaa"),
		b: []byte("aaab"),
	},
	{
		a: []byte("aaaa"),
		b: []byte("bbbb"),
	},
	{
		a: []byte("bbbb"),
		b: []byte("aaaa"),
	},
}

func init() {
	for i, n := 0, len(comparerTests); i < n; i++ {
		t := &comparerTests[i]
		a, b := t.a, t.b
		switch bytes.Compare(a, b) {
		case 0:
			t.min = a
			t.max = a
		case -1:
			t.min = a
			t.max = b
		case +1:
			t.min = b
			t.max = a
		}
	}
}

func TestComparerMax(t *testing.T) {
	for i, test := range comparerTests {
		got := keys.Max(bytesComparer, test.a, test.b)
		if !bytes.Equal(got, test.max) {
			t.Errorf("test=%d a=%q b=%q got=%q want=%q", i, test.a, test.b, got, test.max)
		}
	}
}

func TestComparerMin(t *testing.T) {
	for i, test := range comparerTests {
		got := keys.Min(bytesComparer, test.a, test.b)
		if !bytes.Equal(got, test.min) {
			t.Errorf("test=%d a=%q b=%q got=%q want=%q", i, test.a, test.b, got, test.min)
		}
	}
}
