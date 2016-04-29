package hash

import "github.com/kezhuw/leveldb/internal/endian"

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := seed ^ (uint32(len(b)) * m)
	for len(b) >= 4 {
		h += endian.Uint32(b)
		h *= m
		h ^= h >> 16
		b = b[4:]
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
