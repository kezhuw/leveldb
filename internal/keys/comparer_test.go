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
	min []byte
	max []byte
}

var comparerTests = []comparerTest{
	{
		min: []byte("aaaa"),
		max: []byte("aaaa"),
	},
	{
		min: []byte("aaaa"),
		max: []byte("aaab"),
	},
	{
		min: []byte("aaaa"),
		max: []byte("bbbb"),
	},
}

func TestComparerMax(t *testing.T) {
	for i, test := range comparerTests {
		got := keys.Max(bytesComparer, test.min, test.max)
		if !bytes.Equal(got, test.max) {
			t.Errorf("test=%d min=%x max=%x got=%x", i, test.min, test.max, got)
		}
	}
}

func TestComparerMin(t *testing.T) {
	for i, test := range comparerTests {
		got := keys.Min(bytesComparer, test.min, test.max)
		if !bytes.Equal(got, test.min) {
			t.Errorf("test=%d min=%x max=%x got=%x", i, test.min, test.max, got)
		}
	}
}
