package util

import "io"

// EmptyBytes is an byte slice with length 0, but not equal to nil.
var EmptyBytes = []byte{}

// DupBytes dups b.
func DupBytes(b []byte) []byte {
	dst := make([]byte, len(b))
	copy(dst, b)
	return dst
}

type ByteReader interface {
	io.Reader
	io.ByteReader
}

type byteReader byte

func (c byteReader) ReadByte() (byte, error) {
	return byte(c), nil
}

func (c byteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(c)
	}
	return len(p), nil
}

// NewByteReader creates an ByteReader that returns unlimited bytes with value c.
func NewByteReader(c byte) ByteReader {
	return byteReader(c)
}

// ZeroByteReader is an byteReader that returns unlimited zero bytes.
//
// os.Open("/dev/zero") returns a similar byteReader.
var ZeroByteReader = NewByteReader(0)
