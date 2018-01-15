package compact

import "github.com/kezhuw/leveldb/internal/manifest"

type Compactor interface {
	Level() int
	Rewind()
	FileNumbers() []uint64
	Compact(edit *manifest.Edit) error
}
