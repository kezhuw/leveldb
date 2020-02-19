package leveldb

import (
	"sync/atomic"

	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/memtable"
)

type bundle struct {
	mem     *memtable.MemTable
	imm     *memtable.MemTable
	version *manifest.Version
	refs    int32
}

var closedBundle = &bundle{}

func (db *DB) releaseBundle(b *bundle) {
	if atomic.AddInt32(&b.refs, -1) == 0 {
		b.version.Release()
	}
}

func (db *DB) isClosedBundle(b *bundle) bool {
	return b == closedBundle
}

func (db *DB) loadBundle() *bundle {
	db.bundleMu.RLock()
	b := db.bundle
	atomic.AddInt32(&b.refs, 1)
	db.bundleMu.RUnlock()
	return b
}

func (db *DB) storeBundle(newBundle *bundle) {
	db.bundleMu.Lock()
	oldBundle := db.bundle
	db.bundle = newBundle
	db.bundleMu.Unlock()
	db.releaseBundle(oldBundle)
}

func (db *DB) switchBundleMemTable(mem *memtable.MemTable) *memtable.MemTable {
	imm := mem
	mem = memtable.New(db.options.Comparator)
	newBundle := &bundle{
		mem:     mem,
		imm:     imm,
		version: db.bundle.version,
		refs:    1,
	}
	db.storeBundle(newBundle)
	return mem
}

func (db *DB) switchBundleVersion(level int, version *manifest.Version) {
	newBundle := &bundle{
		mem:     db.bundle.mem,
		imm:     db.bundle.imm,
		version: version,
		refs:    1,
	}
	if level == -1 {
		newBundle.imm = nil
	}
	db.storeBundle(newBundle)
}
