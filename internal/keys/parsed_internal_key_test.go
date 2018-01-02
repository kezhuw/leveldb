package keys_test

import (
	"bytes"
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

type parsedInternalKeyTest struct {
	key   []byte
	seq   keys.Sequence
	kind  keys.Kind
	ikey  keys.InternalKey
	valid bool
}

var parsedInternalKeyTests = []parsedInternalKeyTest{
	{
		ikey:  nil,
		valid: false,
	},
	{
		ikey:  make([]byte, keys.TagBytes-1),
		valid: false,
	},
	{
		key:   nil,
		seq:   0x00123456789abcde,
		kind:  keys.Delete,
		ikey:  []byte{0x00, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		valid: true,
	},
	{
		key:   nil,
		seq:   0x00123456789abcde,
		kind:  keys.Value,
		ikey:  []byte{0x01, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		valid: true,
	},
	{
		key:   []byte("key"),
		seq:   0x00123456789abcde,
		kind:  keys.Delete,
		ikey:  []byte{'k', 'e', 'y', 0x00, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		valid: true,
	},
	{
		key:   []byte("key"),
		seq:   0x00123456789abcde,
		kind:  keys.Value,
		ikey:  []byte{'k', 'e', 'y', 0x01, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		valid: true,
	},
	{
		key:   []byte{0x15, 0x2c, 0x4f, 0x5e},
		seq:   0x00123456789abcde,
		kind:  keys.Delete,
		ikey:  []byte{0x15, 0x2c, 0x4f, 0x5e, 0x00, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		valid: true,
	},
	{
		key:   []byte{0x15, 0x2c, 0x4f, 0x5e},
		seq:   0x00123456789abcde,
		kind:  keys.Value,
		ikey:  []byte{0x15, 0x2c, 0x4f, 0x5e, 0x01, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
		valid: true,
	},
}

func TestParsedInternalKeyParse(t *testing.T) {
	var ikey keys.ParsedInternalKey
	for i, test := range parsedInternalKeyTests {
		got := ikey.Parse(test.ikey)
		if got != test.valid {
			t.Errorf("test=%d ikey=%x got=%t want=%t", i, test.ikey, got, test.valid)
			continue
		}
	}
}

func TestParsedInternalKeyFields(t *testing.T) {
	var ikey keys.ParsedInternalKey
	for i, test := range parsedInternalKeyTests {
		if !ikey.Parse(test.ikey) {
			continue
		}
		if !(bytes.Equal(ikey.UserKey, test.key) && ikey.Sequence == test.seq && ikey.Kind == test.kind) {
			t.Errorf("test=%d ikey=%x got=(%x,%#x,%s) want=(%x,%#x,%s)", i, test.ikey, ikey.UserKey, ikey.Sequence, ikey.Kind, test.key, test.seq, test.kind)
		}
	}
}

func TestParsedInternalKeyTag(t *testing.T) {
	var ikey keys.ParsedInternalKey
	for i, test := range parsedInternalKeyTests {
		if !ikey.Parse(test.ikey) {
			continue
		}
		got, want := ikey.Tag(), test.ikey.Tag()
		if got != want {
			t.Errorf("test=%d ikey=%x got=%x want=%x", i, test.ikey, got, want)
		}
	}
}

func TestParsedInternalKeyAppend(t *testing.T) {
	var ikey keys.ParsedInternalKey
	for i, test := range parsedInternalKeyTests {
		if !ikey.Parse(test.ikey) {
			continue
		}
		buf := ikey.Append(nil)
		if !bytes.Equal(buf, []byte(test.ikey)) {
			t.Errorf("test=%d ikey=%x got=%x", i, test.ikey, buf)
		}
	}
}
