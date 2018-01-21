package request

import "github.com/kezhuw/leveldb/internal/batch"

type Request struct {
	Sync  bool
	Batch batch.Batch
	Reply chan error
}
