package table

import (
	"errors"

	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/table/block"
)

const (
	footerLength = 2*block.MaxHandleEncodedLength + 8
)

var ErrTableMagicNumberWrong = errors.New("leveldb: corrupt table: wrong magic number")

type Footer struct {
	MetaIndexHandle block.Handle
	DataIndexHandle block.Handle
}

var footerZeroBytes [footerLength]byte

func (f *Footer) Encode(buf []byte) int {
	i := block.EncodeHandle(buf, f.MetaIndexHandle)
	i += block.EncodeHandle(buf[i:], f.DataIndexHandle)
	if n := 2*block.MaxHandleEncodedLength - i; n != 0 {
		copy(buf[i:], footerZeroBytes[:n])
		i = 2 * block.MaxHandleEncodedLength
	}
	endian.PutUint64(buf[i:], magicNumber)
	return i + 8
}

func (f *Footer) Unmarshal(buf []byte) error {
	if len(buf) < footerLength {
		panic("buf length for footer is wrong")
	}
	magic := endian.Uint64(buf[footerLength-8:])
	if magic != magicNumber {
		return ErrTableMagicNumberWrong
	}
	buf = buf[:footerLength-8]
	var i int
	f.MetaIndexHandle, i = block.DecodeHandle(buf)
	f.DataIndexHandle, _ = block.DecodeHandle(buf[i:])
	return nil
}
