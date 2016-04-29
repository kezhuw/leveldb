package util

// EmptyBytes is an byte slice with length 0, but not equal to nil.
var EmptyBytes = []byte{}

// DupBytes dups b.
func DupBytes(b []byte) []byte {
	dst := make([]byte, len(b))
	copy(dst, b)
	return dst
}
