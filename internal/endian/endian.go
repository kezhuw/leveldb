package endian

import (
	"encoding/binary"
)

// Endian is the binary.ByteOrder used by LevelDB.
var Endian = binary.LittleEndian

func Uint16(b []byte) uint16 {
	return Endian.Uint16(b)
}

func Uint32(b []byte) uint32 {
	return Endian.Uint32(b)
}

func Uint64(b []byte) uint64 {
	return Endian.Uint64(b)
}

func PutUint16(b []byte, u uint16) {
	Endian.PutUint16(b, u)
}

func PutUint32(b []byte, u uint32) {
	Endian.PutUint32(b, u)
}

func PutUint64(b []byte, u uint64) {
	Endian.PutUint64(b, u)
}
