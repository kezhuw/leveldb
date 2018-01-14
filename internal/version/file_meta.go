package version

import (
	"sort"

	"github.com/kezhuw/leveldb/internal/keys"
)

// FileMeta contains meta info for a sorted table.
type FileMeta struct {
	Number   uint64
	Size     uint64
	Smallest keys.InternalKey
	Largest  keys.InternalKey
}

func indexFile(files []FileMeta, number uint64) int {
	for i, f := range files {
		if f.Number == number {
			return i
		}
	}
	return -1
}

type byNewestFileMeta []FileMeta

var _ sort.Interface = (byNewestFileMeta)(nil)

func (files byNewestFileMeta) Len() int {
	return len(files)
}

func (files byNewestFileMeta) Less(i, j int) bool {
	return files[i].Number > files[j].Number
}

func (files byNewestFileMeta) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}

type byFileKey struct {
	cmp   keys.Comparer
	files []FileMeta
}

var _ sort.Interface = (*byFileKey)(nil)

func (by *byFileKey) Len() int {
	return len(by.files)
}

func (by *byFileKey) Less(i, j int) bool {
	return by.cmp.Compare(by.files[i].Smallest, by.files[j].Smallest) < 0
}

func (by *byFileKey) Swap(i, j int) {
	by.files[i], by.files[j] = by.files[j], by.files[i]
}
