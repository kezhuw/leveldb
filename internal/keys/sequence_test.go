package keys_test

import (
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

type nextSequenceTest struct {
	seq  keys.Sequence
	next uint64
	want keys.Sequence
}

var nextSequenceTests = []nextSequenceTest{
	{
		seq:  0x00123456789abcde,
		next: 0x000fc9a8743210fe,
		want: 0x0021fdfeeccccddc,
	},
	{
		seq:  0x00edcba987654321,
		next: 0x0000149efb5c218e,
		want: 0x00ede04882c164af,
	},
}

func TestSequenceNext(t *testing.T) {
	for i, test := range nextSequenceTests {
		got := test.seq.Next(test.next)
		if got != test.want {
			t.Errorf("test=%d sequence=%#x next=%d got=%#x want=%#x", i, test.seq, test.next, got, test.want)
		}
	}
}

type maxSequenceTest struct {
	seq      keys.Sequence
	overflow bool
}

var maxSequenceTests = []maxSequenceTest{
	{
		seq:      0x00123456789abcde,
		overflow: false,
	},
	{
		seq:      0x00edcba987654321,
		overflow: false,
	},
	{
		seq:      keys.MaxSequence,
		overflow: false,
	},
	{
		seq:      keys.MaxSequence + 1,
		overflow: true,
	},
	{
		seq:      0x10123456789abcde,
		overflow: true,
	},
	{
		seq:      0x1000000000000000,
		overflow: true,
	},
}

func TestMaxSequence(t *testing.T) {
	for i, test := range maxSequenceTests {
		tag := keys.PackTag(test.seq, keys.Seek)
		got, _ := keys.UnpackTag(tag)
		if (got == test.seq) == test.overflow {
			t.Errorf("test=%d seq=%#x overflow=%t got=%#x", i, test.seq, test.overflow, got)
		}
	}
}
