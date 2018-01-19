package manifest

import (
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/table"
)

type matcher interface {
	Match(level int, file *FileMeta, ikey keys.InternalKey, opts *options.ReadOptions) bool
}

type getMatcher struct {
	cache *table.Cache

	err   error
	value []byte
}

func (m *getMatcher) Match(level int, file *FileMeta, ikey keys.InternalKey, opts *options.ReadOptions) (ok bool) {
	m.value, m.err, ok = m.cache.Get(file.Number, file.Size, ikey, opts)
	return ok
}
