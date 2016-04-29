package block

import (
	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
)

type Block struct {
	err            error
	contents       []byte
	restartsOffset uint32
	restartsNumber uint32
}

func checkRestarts(restarts []byte, limit uint32) bool {
	start := endian.Uint32(restarts)
	for i, n := 4, len(restarts); i < n; i += 4 {
		limit := endian.Uint32(restarts[i:])
		if limit < start+3 {
			return false
		}
		start = limit
	}
	return !(limit < start+3)
}

func (b *Block) Len() int {
	return len(b.contents)
}

func (b *Block) NewIterator(cmp keys.Comparator) iterator.Iterator {
	switch {
	case b.err != nil:
		return iterator.Error(b.err)
	case b.contents == nil:
		return iterator.Empty()
	default:
		r := &reader{
			comparer:       cmp,
			contents:       b.contents,
			restartsOffset: b.restartsOffset,
			restartsNumber: b.restartsNumber,
		}
		r.invalidate(nil)
		return r
	}
}

func NewBlock(contents []byte) *Block {
	b := new(Block)
	n := uint32(len(contents))
	if n < 4 {
		b.err = ErrCorruptBlock
		return b
	}
	restartsNumber := endian.Uint32(contents[n-4:])
	if restartsNumber == 0 {
		return b
	}
	if n-4 < 4*restartsNumber {
		b.err = ErrCorruptBlock
		return b
	}
	restartsOffset := n - 4 - 4*restartsNumber
	if !checkRestarts(contents[restartsOffset:n-4], restartsOffset) {
		b.err = ErrCorruptBlock
		return b
	}
	b.contents = contents
	b.restartsOffset = restartsOffset
	b.restartsNumber = restartsNumber
	return b
}
