package leveldb

import "github.com/kezhuw/leveldb/internal/batch"

// Batch holds a collection of updates to apply atomatically to a DB.
type Batch struct {
	batch batch.Batch
}

// Put adds a key/value update to batch.
func (b *Batch) Put(key, value []byte) {
	b.batch.Put(key, value)
}

// Delete adds a key deletion to batch.
func (b *Batch) Delete(key []byte) {
	b.batch.Delete(key)
}

// Clear clears all updates written before.
func (b *Batch) Clear() {
	b.batch.Clear()
}

func (b *Batch) empty() bool {
	return b.batch.Empty()
}

func (b *Batch) err() error {
	return b.batch.Err()
}
