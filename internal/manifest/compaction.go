package manifest

import (
	"github.com/kezhuw/leveldb/internal/compaction"
	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
)

type Compaction struct {
	Registration       *compaction.Registration
	Level              int
	Base               *Version
	Inputs             [2]FileList
	Grandparents       FileList
	MaxOutputFileSize  int64
	NextCompactPointer keys.InternalKey
}

func (c *Compaction) NewIterator() iterator.Iterator {
	v := c.Base
	opts := &options.ReadOptions{DontFillCache: true, VerifyChecksums: true}
	icmp := v.options.Comparator
	var iterators []iterator.Iterator
	inputs0 := c.Inputs[0]
	switch c.Level {
	case 0:
		iterators = make([]iterator.Iterator, 0, len(inputs0)+1)
		for _, f := range inputs0 {
			iterators = append(iterators, v.cache.NewIterator(f.Number, f.Size, opts))
		}
	default:
		iterators = make([]iterator.Iterator, 1, 2)
		iterators[0] = newSortedFileIterator(icmp, inputs0, v.cache, opts)
	}
	if inputs1 := c.Inputs[1]; len(inputs1) != 0 {
		iterators = append(iterators, newSortedFileIterator(icmp, inputs1, v.cache, opts))
	}
	return iterator.NewMergeIterator(icmp, iterators...)
}

func (c *Compaction) IsTrivialMove() bool {
	return len(c.Inputs[0]) == 1 && len(c.Inputs[1]) == 0 && c.Grandparents.TotalFileSize() < configs.MaxGrandparentOverlappingBytes
}
