package leveldb

import (
	"runtime"

	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/leveldb"
)

// Snapshot is a readonly and frozen state of LevelDB in particular moment.
type Snapshot struct {
	err    error
	shared *leveldb.Snapshot
}

func newSnapshot(shared *leveldb.Snapshot) *Snapshot {
	ss := &Snapshot{shared: shared}
	runtime.SetFinalizer(ss, (*Snapshot).finalize)
	return ss
}

func (ss *Snapshot) finalize() error {
	switch {
	case ss.err != nil:
		return ss.err
	case ss.shared == nil:
		return nil
	}
	shared := ss.shared
	ss.shared = nil
	shared.Release()
	return nil
}

// Dup creates a new snapshot from this one. The newly created
// snapshot has independent lifetime as this one.
func (ss *Snapshot) Dup() *Snapshot {
	switch {
	case ss.err != nil:
		return &Snapshot{err: ss.err}
	case ss.shared == nil:
		return &Snapshot{err: errors.ErrSnapshotReleased}
	}
	ss.shared.Retain()
	return newSnapshot(ss.shared)
}

// Release releases any resources hold by this snapshot.
func (ss *Snapshot) Release() error {
	runtime.SetFinalizer(ss, nil)
	return ss.finalize()
}

// Get gets value for given key. It returns ErrNotFound if this snapshot does not
// contain that key.
func (ss *Snapshot) Get(key []byte, opts *ReadOptions) ([]byte, error) {
	switch {
	case ss.err != nil:
		return nil, ss.err
	case ss.shared == nil:
		return nil, errors.ErrSnapshotReleased
	}
	return ss.shared.Get(key, convertReadOptions(opts))
}

// All returns an iterator catching all keys in this snapshot.
func (ss *Snapshot) All(opts *ReadOptions) Iterator {
	return ss.shared.All(convertReadOptions(opts))
}

// Find returns an iterator catching all keys greater than or equal to start in this snapshot.
// Zero length start acts as infinite small.
func (ss *Snapshot) Find(start []byte, opts *ReadOptions) Iterator {
	return ss.shared.Find(start, convertReadOptions(opts))
}

// Range returns an iterator catching all keys between range [start, limit) in this snapshot.
// Zero length start acts as infinite small, zero length limit acts as
// infinite large.
func (ss *Snapshot) Range(start, limit []byte, opts *ReadOptions) Iterator {
	return ss.shared.Range(start, limit, convertReadOptions(opts))
}

// Prefix returns an iterator catching all keys having prefix as prefix in this snapshot.
func (ss *Snapshot) Prefix(prefix []byte, opts *ReadOptions) Iterator {
	return ss.shared.Prefix(prefix, convertReadOptions(opts))
}
