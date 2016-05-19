package batch

import (
	"encoding/binary"
	"math"

	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/keys"
)

const batchHeaderSize = 12

var firstBatchHeaderBytes = [batchHeaderSize]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}

type Item struct {
	Key   []byte
	Value []byte
}

type Batch struct {
	data []byte
}

func (b *Batch) Put(key, value []byte) {
	scratch, ok := b.grow(1 + 2*binary.MaxVarintLen64 + len(key) + len(value))
	if !ok {
		return
	}
	b.data = append(b.data, keys.Value)
	b.appendBytes(scratch, key)
	b.appendBytes(scratch, value)
}

func (b *Batch) Delete(key []byte) {
	scratch, ok := b.grow(1 + binary.MaxVarintLen64 + len(key))
	if !ok {
		return
	}
	b.data = append(b.data, keys.Delete)
	b.appendBytes(scratch, key)
}

func (b *Batch) Clear() {
	b.data = b.data[:0]
}

func (b *Batch) appendBytes(scratch []byte, bytes []byte) {
	n := binary.PutUvarint(scratch, uint64(len(bytes)))
	b.data = append(b.data, scratch[:n]...)
	b.data = append(b.data, bytes...)
}

func (b *Batch) grow(n int) (scratch []byte, ok bool) {
	n += binary.MaxVarintLen64
	l, z := len(b.data), cap(b.data)
	if l+n > z {
		z += z/2 + n
		buf := make([]byte, l, z)
		copy(buf, b.data)
		b.data = buf
	}
	scratch = b.data[:z][z-binary.MaxVarintLen64:]
	if l == 0 {
		b.data = b.data[:batchHeaderSize]
		copy(b.data, firstBatchHeaderBytes[:])
		return scratch, true
	}
	p := b.countData()
	for i := range p {
		p[i]++
		if p[i] != 0 {
			return scratch, true
		}
	}
	p[0] = 0xFF
	p[1] = 0xFF
	p[2] = 0xFF
	p[3] = 0xFF
	return nil, false
}

func (b *Batch) Reset(data []byte) {
	b.data = data
}

func (b *Batch) Append(buf []byte) bool {
	switch {
	case len(buf) <= batchHeaderSize:
		return true
	case len(b.data) == 0:
		b.data = append(b.data, buf...)
		return true
	default:
		n := uint64(endian.Uint32(b.countData())) + uint64(endian.Uint32(buf[8:]))
		if n >= math.MaxUint32 {
			return false
		}
		b.data = append(b.data, buf[batchHeaderSize:]...)
		endian.PutUint32(b.countData(), uint32(n))
		return true
	}
}

func (b *Batch) Empty() bool {
	return len(b.data) <= batchHeaderSize
}

func (b *Batch) Count() uint32 {
	if b.Empty() {
		return 0
	}
	return endian.Uint32(b.countData())
}

func (b *Batch) Err() error {
	if b.Count() == math.MaxUint32 {
		return errors.ErrBatchTooManyWrites
	}
	return nil
}

func (b *Batch) Sequence() keys.Sequence {
	return keys.Sequence(endian.Uint64(b.data[:8]))
}

func (b *Batch) SetSequence(seq keys.Sequence) {
	endian.PutUint64(b.data[:8], uint64(seq))
}

func (b *Batch) countData() []byte {
	return b.data[8:12]
}

func (b *Batch) Body() []byte {
	return b.data[batchHeaderSize:]
}

func (b *Batch) Bytes() []byte {
	return b.data
}

func (b *Batch) Pin() {
	n := len(b.data)
	b.data = b.data[:n:n]
}

func (b *Batch) Size() int {
	return len(b.data)
}

type Iterator interface {
	Add(seq keys.Sequence, kind keys.Kind, key, value []byte)
}

func (b *Batch) Iterate(it Iterator) {
	seq := b.Sequence()
	buf := b.Body()
	for i, n := uint32(0), b.Count(); i < n; i++ {
		var key, value []byte
		kind := keys.Kind(buf[0])
		key, buf = getLengthPrefixedBytesPanic(buf[1:])
		if kind == keys.Value {
			value, buf = getLengthPrefixedBytesPanic(buf)
		}
		it.Add(seq, kind, key, value)
		seq++
	}
}

func (b *Batch) Split(dst []Item) (seq keys.Sequence, items []Item, ok bool) {
	n := b.Count()
	if n <= 0 {
		return 0, dst[:0], true
	}
	switch {
	case n > uint32(cap(dst)):
		items = make([]Item, 0, n)
	default:
		items = dst[:0]
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	seq = b.Sequence()
	buf := b.Body()
	for i := uint32(0); i < n; i++ {
		var key, value []byte
		typ := buf[0]
		key, buf = getLengthPrefixedBytesPanic(buf[1:])
		switch typ {
		case keys.Value:
			value, buf = getLengthPrefixedBytesPanic(buf)
		case keys.Delete:
		default:
			return seq, items, false
		}
		items = append(items, Item{Key: key, Value: value})
	}
	return seq, items, len(buf) == 0
}

func getLengthPrefixedBytesPanic(buf []byte) (bytes, remains []byte) {
	l, n := binary.Uvarint(buf)
	if n <= 0 || n > binary.MaxVarintLen32 {
		panic("invalid length prefixed bytes")
	}
	buf = buf[n:]
	return buf[:l], buf[l:]
}
