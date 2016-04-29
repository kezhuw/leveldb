package leveldb

import (
	"sync/atomic"

	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
)

type snapshotList struct {
	dummy Snapshot
}

func (sl *snapshotList) Init() {
	sl.dummy.next = &sl.dummy
	sl.dummy.prev = &sl.dummy
}

func (sl *snapshotList) PushBack(ss *Snapshot) {
	ss.next = sl.dummy.next
	sl.dummy.next.prev = ss
	sl.dummy.next = ss
	ss.prev = &sl.dummy
}

func (sl *snapshotList) Remove(ss *Snapshot) {
	ss.prev.next = ss.next
	ss.next.prev = ss.prev
}

func (sl *snapshotList) Empty() bool {
	return sl.dummy.prev == &sl.dummy
}

func (sl *snapshotList) Oldest() keys.Sequence {
	return sl.dummy.prev.seq
}

type Snapshot struct {
	db   *DB
	seq  keys.Sequence
	refs int64
	next *Snapshot
	prev *Snapshot
}

func (ss *Snapshot) Retain() {
	atomic.AddInt64(&ss.refs, 1)
}

func (ss *Snapshot) Release() {
	if atomic.AddInt64(&ss.refs, -1) == 0 {
		ss.db.releaseSnapshot(ss)
	}
}

func (ss *Snapshot) Get(key []byte, opts *options.ReadOptions) ([]byte, error) {
	return ss.db.get(key, ss.seq, opts)
}

func (ss *Snapshot) All(opts *options.ReadOptions) iterator.Iterator {
	return ss.db.between(nil, nil, ss.seq, opts)
}

func (ss *Snapshot) Find(start []byte, opts *options.ReadOptions) iterator.Iterator {
	return ss.db.between(start, nil, ss.seq, opts)
}

func (ss *Snapshot) Range(start, limit []byte, opts *options.ReadOptions) iterator.Iterator {
	return ss.db.between(start, limit, ss.seq, opts)
}

func (ss *Snapshot) Prefix(prefix []byte, opts *options.ReadOptions) iterator.Iterator {
	return ss.db.prefix(prefix, ss.seq, opts)
}
