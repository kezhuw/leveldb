package filter

import (
	"bytes"

	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/filter"
)

const (
	baseLg = 11
)

type Writer struct {
	buf       bytes.Buffer
	offsets   []uint32
	scratch   [4]byte
	Generator filter.Generator
}

func (w *Writer) Reset() {
	w.buf.Reset()
	w.offsets = w.offsets[:0]
}

func (w *Writer) Add(key []byte) {
	if w.Generator == nil {
		return
	}
	w.Generator.Add(key)
}

func (w *Writer) StartBlock(offset uint64) {
	if w.Generator == nil {
		return
	}
	index := int(offset >> baseLg)
	for index > len(w.offsets) {
		w.generate()
	}
}

func (w *Writer) generate() {
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	w.Generator.Append(&w.buf)
}

func (w *Writer) Finish() []byte {
	if w.Generator == nil || (w.Generator.Empty() && w.buf.Len() == 0) {
		return nil
	}
	if !w.Generator.Empty() {
		w.generate()
	}
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	w.buf.Grow(4*len(w.offsets) + 1)
	tmp4 := w.scratch[:4]
	for _, x := range w.offsets {
		endian.PutUint32(tmp4, x)
		w.buf.Write(tmp4)
	}
	w.buf.WriteByte(byte(baseLg))
	return w.buf.Bytes()
}
