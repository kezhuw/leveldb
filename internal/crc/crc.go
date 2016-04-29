// Package crc computes masked CRC-32 checksum using Castagnoli's polynomial.
package crc

import (
	"hash/crc32"
	"strconv"
)

var table = crc32.MakeTable(crc32.Castagnoli)

// CRC is a CRC-32 checksum computed using Castagnoli's polynomial.
type CRC struct {
	// Saved as a field to avoid accidentally cast.
	checksum uint32
}

// New computes checksum using given bytes.
func New(b []byte) CRC {
	return CRC{crc32.Checksum(b, table)}
}

// Update updates checksum using given bytes.
func Update(c CRC, b []byte) CRC {
	return CRC{crc32.Update(c.checksum, table, b)}
}

// Value returns a masked checksum value.
func (c CRC) Value() uint32 {
	return (c.checksum>>15 | c.checksum<<17) + 0xa282ead8
}

// String implements fmt.Stringer.
func (c CRC) String() string {
	return strconv.FormatUint(uint64(c.Value()), 10)
}
