package compress_test

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/kezhuw/leveldb/internal/compress"
)

type compressTest struct {
	typ  compress.Type
	size int
}

var compressionTests = []compressTest{
	{typ: compress.SnappyCompression, size: 1},
	{typ: compress.SnappyCompression, size: 16},
	{typ: compress.SnappyCompression, size: 128},
	{typ: compress.SnappyCompression, size: 1024},
	{typ: compress.SnappyCompression, size: 1024 * 32},
	{typ: compress.SnappyCompression, size: 1024 * 128},
	{typ: compress.SnappyCompression, size: 1024 * 1024},
	{typ: compress.SnappyCompression, size: 1024 * 1024 * 4},
	{typ: compress.SnappyCompression, size: 1024 * 1024 * 16},
}

func randomBuffer(size int) []byte {
	buf := make([]byte, size)
	rand.Read(buf)
	return buf
}

func TestCompression(t *testing.T) {
	for i, tt := range compressionTests {
		buf := randomBuffer(tt.size)
		compressed, err := compress.Encode(tt.typ, nil, buf)
		if err != nil {
			t.Fatalf("test=%d compression type=%d encode error=%q", i, tt.typ, err)
		}
		decompressed, err := compress.Decode(tt.typ, nil, compressed)
		if err != nil {
			t.Fatalf("test=%d compression type=%d decode error=%q", i, tt.typ, err)
		}
		if !bytes.Equal(buf, decompressed) {
			t.Fatalf("test=%d compression type=%d decompressed content don't equal to original", i, tt.typ)
		}
	}
}

func TestNoCompression(t *testing.T) {
	var err error
	_, err = compress.Encode(compress.NoCompression, nil, nil)
	if err != compress.ErrNoCompression {
		t.Fatalf("Encode expect ErrNoCompression, got %s", err)
	}
	_, err = compress.Decode(compress.NoCompression, nil, nil)
	if err != compress.ErrNoCompression {
		t.Fatalf("Decode expect ErrNoCompression, got %s", err)
	}
}

var invalidCompression compress.Type = math.MinInt32

func TestUnsupportedCompression(t *testing.T) {
	var err error
	_, err = compress.Encode(invalidCompression, nil, nil)
	if err != compress.ErrUnsupportedCompression {
		t.Fatalf("Encode expect ErrUnsupportedCompression, got %s", err)
	}
	_, err = compress.Decode(invalidCompression, nil, nil)
	if err != compress.ErrUnsupportedCompression {
		t.Fatalf("Decode expect ErrUnsupportedCompression, got %s", err)
	}
}
