package block

import "encoding/binary"

const MaxHandleEncodedLength = 2 * binary.MaxVarintLen64

type Handle struct {
	Offset uint64
	Length uint64
}

func EncodeHandle(dst []byte, h Handle) int {
	i := binary.PutUvarint(dst, h.Offset)
	i += binary.PutUvarint(dst[i:], h.Length)
	return i
}

func DecodeHandle(buf []byte) (Handle, int) {
	offset, i := binary.Uvarint(buf)
	if i <= 0 {
		return Handle{}, i
	}
	length, j := binary.Uvarint(buf[i:])
	if j <= 0 {
		return Handle{}, j
	}
	return Handle{Offset: offset, Length: length}, i + j
}
