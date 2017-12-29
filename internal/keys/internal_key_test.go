package keys_test

import (
	"bytes"
	"math/rand"
	"runtime"
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
)

func equalInternalKey(a, b keys.InternalKey) bool {
	return bytes.Equal([]byte(a), []byte(b))
}

type toInternalKeyTest struct {
	ikey  []byte
	valid bool
}

var toInternalKeyTests = []toInternalKeyTest{
	{
		ikey:  nil,
		valid: false,
	},
	{
		ikey:  make([]byte, keys.TagBytes-1),
		valid: false,
	},
	{
		ikey:  make([]byte, keys.TagBytes),
		valid: true,
	},
	{
		ikey:  make([]byte, keys.TagBytes+1),
		valid: true,
	},
}

func TestToInternalKey(t *testing.T) {
	for i, test := range toInternalKeyTests {
		got, ok := keys.ToInternalKey(test.ikey)
		if ok != test.valid || (ok && !bytes.Equal([]byte(got), test.ikey)) {
			t.Errorf("test=%d ikey=%x valid=%t got=(%x,%t)", i, test.ikey, test.valid, got, ok)
		}
	}
}

type internalKeyTest struct {
	key  []byte
	seq  keys.Sequence
	kind keys.Kind
	ikey keys.InternalKey
}

var internalKeyTests = []internalKeyTest{
	{
		key:  nil,
		seq:  0x00123456789abcde,
		kind: keys.Delete,
		ikey: []byte{0x00, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
	},
	{
		key:  nil,
		seq:  0x00123456789abcde,
		kind: keys.Value,
		ikey: []byte{0x01, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
	},
	{
		key:  []byte("key"),
		seq:  0x00123456789abcde,
		kind: keys.Delete,
		ikey: []byte{'k', 'e', 'y', 0x00, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
	},
	{
		key:  []byte("key"),
		seq:  0x00123456789abcde,
		kind: keys.Value,
		ikey: []byte{'k', 'e', 'y', 0x01, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
	},
	{
		key:  []byte{0x15, 0x2c, 0x4f, 0x5e},
		seq:  0x00123456789abcde,
		kind: keys.Delete,
		ikey: []byte{0x15, 0x2c, 0x4f, 0x5e, 0x00, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
	},
	{
		key:  []byte{0x15, 0x2c, 0x4f, 0x5e},
		seq:  0x00123456789abcde,
		kind: keys.Value,
		ikey: []byte{0x15, 0x2c, 0x4f, 0x5e, 0x01, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12},
	},
}

func TestMakeInternalKey(t *testing.T) {
	for i, test := range internalKeyTests {
		n := len(test.key) + keys.TagBytes
		buf := make([]byte, n+i)
		got := keys.MakeInternalKey(buf, test.key, test.seq, test.kind)
		if !equalInternalKey(got, test.ikey) {
			t.Errorf("test=%d key=%x seq=%#x kind=%s got=%x want=%x", i, test.key, test.seq, test.kind, got, test.ikey)
		}
	}
}

func testPanicMakeInternalKey(t *testing.T, i int, test *internalKeyTest) {
	n := len(test.ikey) - 1 - rand.Intn(keys.TagBytes)
	defer func() {
		r := recover()
		_, ok := r.(runtime.Error)
		if !ok {
			t.Errorf("test=%d key=%x seq=%#x kind=%s len(buf)=%d got=%v want=RuntimeError", i, test.key, test.seq, test.kind, n, r)
		}
	}()
	buf := make([]byte, n)
	keys.MakeInternalKey(buf, test.key, test.seq, test.kind)
}

func TestMakeInternalKeyShortBuf(t *testing.T) {
	for i, test := range internalKeyTests {
		testPanicMakeInternalKey(t, i, &test)
	}
}

func TestNewInternalKey(t *testing.T) {
	for i, test := range internalKeyTests {
		got := keys.NewInternalKey(test.key, test.seq, test.kind)
		if !equalInternalKey(got, test.ikey) {
			t.Errorf("test=%d key=%x seq=%#x kind=%s got=%x want=%x", i, test.key, test.seq, test.kind, got, test.ikey)
		}
	}
}

func TestInternalKeyDup(t *testing.T) {
	for i, test := range internalKeyTests {
		got := test.ikey.Dup()
		if !equalInternalKey(got, test.ikey) {
			t.Errorf("test=%d ikey=%x got=%x", i, test.ikey, got)
		}
	}
}

func TestInternalKeyUserKey(t *testing.T) {
	for i, test := range internalKeyTests {
		got := test.ikey.UserKey()
		if !bytes.Equal(got, test.key) {
			t.Errorf("test=%d ikey=%x got=%x want=%x", i, test.ikey, got, test.key)
		}
	}
}

func TestInternalKeyTag(t *testing.T) {
	for i, test := range internalKeyTests {
		want := keys.PackTag(test.seq, test.kind)
		got := test.ikey.Tag()
		if got != want {
			t.Errorf("test=%d ikey=%x got=%#x want=%#x", i, test.ikey, got, want)
		}
	}
}

func TestInternalKeySplit(t *testing.T) {
	for i, test := range internalKeyTests {
		key, seq, kind := test.ikey.Split()
		if !(bytes.Equal(key, test.key) && seq == test.seq && kind == test.kind) {
			t.Errorf("test=%d ikey=%x got=(%x,%#x,%s) want=(%x,%#x,%s)", i, test.ikey, key, seq, kind, test.key, test.seq, test.kind)
		}
	}
}
