package block

import (
	"bytes"
	"encoding/binary"

	"github.com/kezhuw/leveldb/internal/endian"
)

type Writer struct {
	buf             bytes.Buffer
	scratch         [3 * binary.MaxVarintLen64]byte
	counter         int
	lastKey         []byte
	restarts        []uint32
	RestartInterval int
}

func (w *Writer) Empty() bool {
	return w.buf.Len() == 0
}

func (w *Writer) Reset() {
	w.buf.Reset()
	w.counter = 0
	w.lastKey = w.lastKey[:0]
	// Offset for first key, which has no shared parts with lastKey,
	// thus will be stored as an restart point.
	w.restarts = append(w.restarts[:0], 0)
}

func (w *Writer) ApproximateSize() int {
	return w.buf.Len() + len(w.restarts)*4 + 4
}

func (w *Writer) Add(key, value []byte) {
	l := len(key)
	shared := 0
	lastKey := w.lastKey
	switch {
	case w.counter < w.RestartInterval:
		n := len(lastKey)
		if l < n {
			n = l
		}
		for shared < n && lastKey[shared] == key[shared] {
			shared++
		}
	default:
		w.counter = 0
		w.restarts = append(w.restarts, uint32(w.buf.Len()))
	}
	n := binary.PutUvarint(w.scratch[:], uint64(shared))
	n += binary.PutUvarint(w.scratch[n:], uint64(len(key)-shared))
	n += binary.PutUvarint(w.scratch[n:], uint64(len(value)))
	w.buf.Write(w.scratch[:n])
	w.buf.Write(key[shared:])
	w.buf.Write(value)
	w.lastKey = append(lastKey[:shared], key[shared:]...)
	w.counter++
}

func (w *Writer) Finish() *bytes.Buffer {
	w.buf.Grow(4*len(w.restarts) + 4)
	tmp4 := w.scratch[:4]
	for _, x := range w.restarts {
		endian.PutUint32(tmp4, x)
		w.buf.Write(tmp4)
	}
	endian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf.Write(tmp4)
	return &w.buf
}
