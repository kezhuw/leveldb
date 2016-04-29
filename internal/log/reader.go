package log

import (
	"errors"
	"fmt"
	"io"

	"github.com/kezhuw/leveldb/internal/crc"
	"github.com/kezhuw/leveldb/internal/endian"
)

var (
	ErrIncompleteRecord = errors.New("leveldb: incomplete log record")
	ErrMismatchChecksum = errors.New("leveldb: corrupt log record: mismatch checksum")
)

type Reader struct {
	r      io.Reader
	err    error
	buf    []byte // len(buf) equals to block size
	block  []byte
	offset int64 // Points after last record read.
}

func newReader(r io.Reader, blockSize int) *Reader {
	if blockSize <= headerSize {
		blockSize = defaultBlockSize
	}
	buf := make([]byte, blockSize)
	return &Reader{r: r, buf: buf}
}

// NewReader creates a log reader to read from r. r must be in start of a block.
func NewReader(r io.Reader) *Reader {
	return newReader(r, defaultBlockSize)
}

// Offset returns an offset points after last successfully read record.
func (r *Reader) Offset() int64 {
	return r.offset
}

// AppendRecord reads a new record from underlying reader, and append it to b.
// It returns a byte slice with new record appended and a potential error.
func (r *Reader) AppendRecord(b []byte) ([]byte, error) {
	start := len(b)
	block := r.block
	middle := false
	offset := r.offset
	for {
		if len(block) < headerSize {
			if r.err != nil {
				return b[:start], r.err
			}
			offset += int64(len(block))
			r.offset = offset
			block = r.readBlock()
			if len(block) < headerSize {
				if r.err == io.EOF && (len(block) != 0 || middle) {
					return b[:start], ErrIncompleteRecord
				}
				return b[:start], r.err
			}
		}

		head := block[:headerSize]
		length := int(endian.Uint16(head[4:6]))
		span := length + headerSize
		if span > len(block) {
			b = b[:start]
			switch err := r.err; err {
			case nil:
				return b, errors.New("record beyond block boundary")
			case io.EOF:
				return b, ErrIncompleteRecord
			default:
				return b, err
			}
		}

		actualChecksum := crc.New(block[6:span]).Value()
		expectedChecksum := endian.Uint32(head[:4])
		if actualChecksum != expectedChecksum {
			return b, ErrMismatchChecksum
		}

		// Corruption is relatively rare, so we append record here to simplify code
		// in switch statement below.
		b = append(b, block[headerSize:span]...)
		block = block[span:]
		offset += int64(span)
		switch typ := head[6]; typ {
		case fullBlock:
			if middle {
				return b[:start], errors.New("full record in middle")
			}
			r.block = block
			r.offset = offset
			return b, nil
		case firstBlock:
			if middle {
				return b[:start], errors.New("first record in middle")
			}
			middle = true
		case middleBlock:
			if !middle {
				return b[:start], errors.New("middle record at first")
			}
		case lastBlock:
			if !middle {
				return b[:start], errors.New("last record at first")
			}
			r.block = block
			r.offset = offset
			return b, nil
		default:
			return b[:start], fmt.Errorf("unknown record type: %d", typ)
		}
	}
}

func (r *Reader) readBlock() []byte {
	n, err := io.ReadFull(r.r, r.buf)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		r.err = err
	}
	r.block = r.buf[:n]
	return r.block
}
