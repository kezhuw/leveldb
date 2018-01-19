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

	err         error
	value       []byte
	firstMatch  LevelFileMeta
	seekThrough LevelFileMeta
}

func (m *getMatcher) Match(level int, file *FileMeta, ikey keys.InternalKey, opts *options.ReadOptions) (ok bool) {
	switch {
	case m.firstMatch.FileMeta == nil:
		m.firstMatch.Level = level
		m.firstMatch.FileMeta = file
	case m.seekThrough.FileMeta == nil:
		m.seekThrough = m.firstMatch
	}
	m.value, m.err, ok = m.cache.Get(file.Number, file.Size, ikey, opts)
	return ok
}

type seekOverlapMatcher struct {
	seekOverlap LevelFileMeta
}

func (m *seekOverlapMatcher) Match(level int, file *FileMeta, ikey keys.InternalKey, opts *options.ReadOptions) bool {
	if m.seekOverlap.FileMeta == nil {
		m.seekOverlap.Level = level
		m.seekOverlap.FileMeta = file
		return false
	}
	return true
}
