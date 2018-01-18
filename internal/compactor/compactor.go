package compactor

import "github.com/kezhuw/leveldb/internal/manifest"

type Compactor interface {
	Level() int
	Rewind()
	Compact(edit *manifest.Edit) error
}
