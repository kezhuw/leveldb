package leveldb

import (
	"runtime"

	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/leveldb"
)

// DB represents a opened LevelDB database.
type DB struct {
	db *leveldb.DB
}

// Open opens a LevelDB database stored in directory 'dbname'.
func Open(dbname string, opts *Options) (*DB, error) {
	ldb, err := leveldb.Open(dbname, convertOptions(opts))
	if err != nil {
		return nil, err
	}
	db := &DB{db: ldb}
	runtime.SetFinalizer(db, (*DB).finalize)
	return db, nil
}

func (db *DB) finalize() {
	go db.db.Close()
}

// Close closes the opened LevelDB database. Make sure that there are no
// ongoing accesses and outstanding iterators. All operations after this
// call will get error ErrDBClosed. A judicious caller should call this
// function after all ongoing accesses is done and all outstanding iterators
// is releases, and should not call any functions of this db instance after
// it is closed.
func (db *DB) Close() error {
	runtime.SetFinalizer(db, nil)
	return db.db.Close()
}

// Get gets value for given key. It returns ErrNotFound if db does not
// contain that key.
func (db *DB) Get(key []byte, opts *ReadOptions) ([]byte, error) {
	return db.db.Get(key, convertReadOptions(opts))
}

// Put stores a key/value pair in DB.
func (db *DB) Put(key, value []byte, opts *WriteOptions) error {
	return db.db.Put(key, value, convertWriteOptions(opts))
}

// Delete deletes the database entry for given key. It is not an error
// if db does not contain that key.
func (db *DB) Delete(key []byte, opts *WriteOptions) error {
	return db.db.Delete(key, convertWriteOptions(opts))
}

// Write applies batch to db.
func (db *DB) Write(batch Batch, opts *WriteOptions) error {
	switch {
	case batch.empty():
		return nil
	case batch.err() != nil:
		return batch.err()
	}
	return db.db.Write(batch.batch, convertWriteOptions(opts))
}

// All returns an iterator catching all keys in db.
func (db *DB) All(opts *ReadOptions) Iterator {
	return db.db.All(convertReadOptions(opts))
}

// Find returns an iterator catching all keys greater than or equal to start in db.
// Zero length start acts as infinite small.
func (db *DB) Find(start []byte, opts *ReadOptions) Iterator {
	return db.db.Find(start, convertReadOptions(opts))
}

// Range returns an iterator catching all keys in range [start, limit).
// Zero length start acts as infinite small, zero length limit acts as
// infinite large.
func (db *DB) Range(start, limit []byte, opts *ReadOptions) Iterator {
	return db.db.Range(start, limit, convertReadOptions(opts))
}

// Prefix returns an iterator catching all keys having prefix as prefix.
func (db *DB) Prefix(prefix []byte, opts *ReadOptions) Iterator {
	return db.db.Prefix(prefix, convertReadOptions(opts))
}

// GetSnapshot captures current state of db as a Snapshot. Following updates in
// db will not affect the state of Snapshot.
func (db *DB) GetSnapshot() *Snapshot {
	ss := db.db.NewSnapshot()
	if ss == nil {
		return &Snapshot{err: errors.ErrDBClosed}
	}
	return newSnapshot(ss)
}

// CompactRange compacts keys in range [start, limit] to max level these keys
// reside in currently.
//
// Zero length start acts as infinite small, zero length limit acts as
// infinite large.
func (db *DB) CompactRange(start, limit []byte) error {
	return db.db.CompactRange(start, limit)
}
