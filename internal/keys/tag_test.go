package keys_test

import (
	"bytes"
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

type tagTest struct {
	buf  []byte
	tag  keys.Tag
	seq  keys.Sequence
	kind keys.Kind
}

var tagTests = []tagTest{
	{
		buf:  []byte{0x01, 0xdf, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		tag:  0x123456789abcdf01,
		seq:  0x00123456789abcdf,
		kind: keys.Value,
	},
	{
		buf:  []byte{0x00, 0xdf, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		tag:  0x123456789abcdf00,
		seq:  0x00123456789abcdf,
		kind: keys.Delete,
	},
}

func TestPackTag(t *testing.T) {
	for i, test := range tagTests {
		got := keys.PackTag(test.seq, test.kind)
		if got != test.tag {
			t.Errorf("test=%d, sequence=%#x kind=%s got=%#x want=%#x", i, test.seq, test.kind, got, test.tag)
		}
	}
}

func TestUnpackTag(t *testing.T) {
	for i, test := range tagTests {
		seq, kind := keys.UnpackTag(test.tag)
		if seq != test.seq || kind != test.kind {
			t.Errorf("test=%d tag=%#x got=(%#x,%s) want=(%#x,%s)", i, test.tag, seq, kind, test.seq, test.kind)
		}
	}
}

func TestPutTag(t *testing.T) {
	for i, test := range tagTests {
		got := make([]byte, keys.TagBytes)
		keys.PutTag(got, test.tag)
		if !bytes.Equal(got, test.buf) {
			t.Errorf("test=%d tag=%#x got=%#x want=%#x", i, test.tag, got, test.buf)
		}
	}
}

func TestGetTag(t *testing.T) {
	for i, test := range tagTests {
		got := keys.GetTag(test.buf)
		if got != test.tag {
			t.Errorf("test=%d buf=%#x got=%#x want=%#x", i, test.buf, got, test.tag)
		}
	}
}

func TestCombineTag(t *testing.T) {
	for i, test := range tagTests {
		got := make([]byte, keys.TagBytes)
		keys.CombineTag(got, test.seq, test.kind)
		if !bytes.Equal(got, test.buf) {
			t.Errorf("test=%d sequence=%#x kind=%s got=%#x want=%#x", i, test.seq, test.kind, got, test.buf)
		}
	}
}

func TestExtractTag(t *testing.T) {
	for i, test := range tagTests {
		seq, kind := keys.ExtractTag(test.buf)
		if seq != test.seq || kind != test.kind {
			t.Errorf("test=%d buf=%#x got=(%#x,%s) want=(%#x,%s)", i, test.buf, seq, kind, test.seq, test.kind)
		}
	}
}
