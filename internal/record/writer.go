package record

import (
	"io"

	"github.com/kezhuw/leveldb/internal/crc"
	"github.com/kezhuw/leveldb/internal/endian"
)

type Writer struct {
	w           io.Writer
	err         error
	offset      int64
	blockBuffer []byte
	blockOffset int
	blockSize   int
}

func newWriter(w io.Writer, blockSize int, offset int64) *Writer {
	if blockSize <= headerSize {
		blockSize = defaultBlockSize
	}
	blockOffset := int(offset % int64(blockSize))
	return &Writer{
		w:           w,
		offset:      offset,
		blockBuffer: make([]byte, blockSize),
		blockOffset: blockOffset,
		blockSize:   blockSize,
	}
}

func NewWriter(w io.Writer, offset int64) *Writer {
	return newWriter(w, defaultBlockSize, offset)
}

func (w *Writer) Err() error {
	return w.err
}

func (w *Writer) Offset() int64 {
	return w.offset
}

func calcBlockType(begin, end bool) int {
	switch {
	case begin == true && end == true:
		return fullBlock
	case begin == true:
		return firstBlock
	case end == true:
		return lastBlock
	default:
		return middleBlock
	}
}

// Write creates an record, and appends it to underlying io.Writer.
func (w *Writer) Write(b []byte) error {
	switch {
	case w.err != nil:
		return w.err
	case len(b) == 0:
		return nil
	}
	begin, end := true, false
	blockOffset, blockSize := w.blockOffset, w.blockSize
	for !end {
		leftover := blockSize - blockOffset
		if leftover < headerSize {
			if leftover != 0 {
				copy(w.blockBuffer[blockOffset:], trailer[:leftover])
			}
			n, err := w.w.Write(w.blockBuffer[w.blockOffset:])
			if err != nil {
				w.err = err
				return err
			}
			w.offset += int64(n)
			w.blockOffset = 0
			blockOffset = 0
			leftover = blockSize
		}
		length := leftover - headerSize
		if l := len(b); l <= length {
			end = true
			length = l
		}
		typ := calcBlockType(begin, end)
		sum := crc.Update(typeChecksums[typ], b[:length]).Value()
		head := w.blockBuffer[blockOffset : blockOffset+headerSize]
		endian.PutUint32(head[:4], sum)
		endian.PutUint16(head[4:6], uint16(length))
		head[6] = byte(typ)
		blockOffset += headerSize
		copy(w.blockBuffer[blockOffset:], b[:length])
		blockOffset += length
		begin = false
		b = b[length:]
	}
	n, err := w.w.Write(w.blockBuffer[w.blockOffset:blockOffset])
	if err != nil {
		w.err = err
		return err
	}
	w.blockOffset = blockOffset
	w.offset += int64(n)
	return nil
}
