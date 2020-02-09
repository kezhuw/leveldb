package leveldb

import (
	"sync/atomic"
	"unsafe"

	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/memtable"
)

type bundle struct {
	mem     *memtable.MemTable
	imm     *memtable.MemTable
	version *manifest.Version
}

func (db *DB) loadBundle() *bundle {
	return (*bundle)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&db.bundle))))
}

func (db *DB) swapBundle(old, new *bundle) bool {
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&db.bundle)), unsafe.Pointer(old), unsafe.Pointer(new))
}

func (db *DB) storeBundle(new *bundle) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.bundle)), unsafe.Pointer(new))
}

func (db *DB) switchMemTable(mem *memtable.MemTable) *memtable.MemTable {
	imm := mem
	mem = memtable.New(db.options.Comparator)
	old := db.loadBundle()
	new := &bundle{
		mem:     mem,
		imm:     imm,
		version: old.version,
	}
	for !db.swapBundle(old, new) {
		old = db.loadBundle()
		// Use version from compaction goroutine.
		new.version = old.version
	}
	return mem
}

func (db *DB) switchVersion(level int, version *manifest.Version) {
	defer db.wakeupWrite(level)
	db.manifest.Append(version)
	old := db.loadBundle()
	new := &bundle{
		mem:     old.mem,
		imm:     old.imm,
		version: version,
	}
	if level == -1 {
		new.imm = nil
	}
	for !db.swapBundle(old, new) {
		old = db.loadBundle()
		// Use memtables from write goroutine.
		new.mem = old.mem
		new.imm = old.imm
	}
}
