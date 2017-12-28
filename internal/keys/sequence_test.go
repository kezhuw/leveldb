package keys_test

import (
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

type sequenceTest struct {
	seq  keys.Sequence
	next uint64
	want keys.Sequence
}

var sequenceTests = []sequenceTest{
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

func TestSequenceAdd(t *testing.T) {
	for i, test := range sequenceTests {
		got := test.seq.Add(test.next)
		if got != test.want {
			t.Errorf("test=%d sequence=%#x next=%d got=%#x want=%#x", i, test.seq, test.next, got, test.want)
		}
	}
}
