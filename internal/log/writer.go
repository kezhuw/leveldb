package log

import (
	"io"

	"github.com/kezhuw/leveldb/internal/crc"
	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/iovecs"
)

type Writer struct {
	w           iovecs.Writer
	err         error
	offset      int64
	iovecs      [][]byte
	headers     []header
	blockOffset int
	blockSize   int
}

func newWriter(w io.Writer, blockSize int, offset int64) *Writer {
	if blockSize <= headerSize {
		blockSize = defaultBlockSize
	}
	blockOffset := int(offset % int64(blockSize))
	return &Writer{w: iovecs.NewWriter(w), offset: offset, blockOffset: blockOffset, blockSize: blockSize}
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

func (w *Writer) header(i int) []byte {
	if len(w.headers) <= i {
		w.headers = make([]header, i+1)
	}
	return w.headers[i][:]
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
	blockSize := w.blockSize
	begin, end := true, false
	offset, iovecs := w.blockOffset, w.iovecs[:0]
	for i := 0; true; i++ {
		leftover := blockSize - offset
		if leftover < headerSize {
			if leftover != 0 {
				iovecs = append(iovecs, trailer[:leftover])
			}
			offset = 0
			leftover = blockSize
		}
		length := leftover - headerSize
		if l := len(b); l <= length {
			end = true
			length = l
		}
		typ := calcBlockType(begin, end)
		sum := crc.Update(typeChecksums[typ], b[:length]).Value()
		head := w.header(i)
		endian.PutUint32(head[:4], sum)
		endian.PutUint16(head[4:6], uint16(length))
		head[6] = byte(typ)
		switch length != 0 {
		case true:
			iovecs = append(iovecs, head, b[:length])
		default:
			iovecs = append(iovecs, head)
		}
		offset += headerSize + length
		if end {
			break
		}
		begin = false
		b = b[length:]
	}
	n, err := w.w.Writev(iovecs...)
	if err != nil {
		zeroIOVecs(iovecs)
		w.err = err
		return err
	}
	zeroIOVecs(iovecs)
	w.blockOffset, w.iovecs = offset, iovecs
	w.offset += n
	return nil
}

func zeroIOVecs(vecs [][]byte) {
	for i, n := 0, len(vecs); i < n; i++ {
		// let gc to collect unused slices
		vecs[i] = nil
	}
}
