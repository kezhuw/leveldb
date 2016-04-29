package compact

import "github.com/kezhuw/leveldb/internal/version"

type Compactor interface {
	Level() int
	Rewind()
	FileNumbers() []uint64
	Compact(edit *version.Edit) error
}
