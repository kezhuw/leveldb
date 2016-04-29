package table

import (
	"io"

	"github.com/kezhuw/leveldb/internal/compress"
	"github.com/kezhuw/leveldb/internal/crc"
	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/table/block"
)

func ReadBlock(r io.ReaderAt, fileNumber uint64, h block.Handle, verifyChecksums bool) ([]byte, error) {
	n := h.Length + blockTrailerSize
	buf := make([]byte, n)
	if _, err := r.ReadAt(buf, int64(h.Offset)); err != nil {
		return nil, err
	}
	if verifyChecksums {
		actualChecksum := crc.New(buf[:n-4]).Value()
		expectedChecksum := endian.Uint32(buf[n-4:])
		if actualChecksum != expectedChecksum {
			return nil, errors.NewCorruption(fileNumber, "table block", int64(h.Offset), "checksum mismatch")
		}
	}
	compression := compress.Type(buf[h.Length])
	if compression != compress.NoCompression {
		return compress.Decode(compression, nil, buf[:h.Length])
	}
	return buf[:h.Length:h.Length], nil
}

func ReadDataBlock(r io.ReaderAt, fileNumber uint64, h block.Handle, verifyChecksums bool) (*block.Block, error) {
	buf, err := ReadBlock(r, fileNumber, h, verifyChecksums)
	if err != nil {
		return nil, err
	}
	return block.NewBlock(buf), nil
}
