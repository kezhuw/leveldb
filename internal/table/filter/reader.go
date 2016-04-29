package filter

import (
	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/filter"
)

type Reader struct {
	num     int
	baseLg  int
	filter  filter.Filter
	filters []byte
	offsets []byte
}

func NewReader(filter filter.Filter, contents []byte) *Reader {
	n := len(contents)
	if n < 5 {
		return nil
	}
	offset := int(endian.Uint32(contents[n-5 : n-1]))
	switch {
	case offset > n-5:
		return nil
	case (n-5-offset)%4 != 0:
		return nil
	}
	num := (n - 5 - offset) / 4
	baseLg := int(contents[n-1])
	filters := contents[:offset]
	offsets := contents[offset : n-1] // last offset used as sentinel
	return &Reader{
		num:     num,
		baseLg:  baseLg,
		filter:  filter,
		filters: filters,
		offsets: offsets,
	}
}

func (r *Reader) Contains(blockOffset uint64, key []byte) bool {
	index := int(blockOffset >> uint64(r.baseLg))
	if index >= r.num {
		// Errors are treated as potential matches
		return true
	}
	offset := index * 4
	start := endian.Uint32(r.offsets[offset : offset+4])
	limit := endian.Uint32(r.offsets[offset+4 : offset+8])
	if start == limit {
		// Empty filters do not match any keys
		return false
	}
	return r.filter.Contains(r.filters[start:limit], key)
}
