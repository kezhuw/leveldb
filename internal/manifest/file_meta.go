package manifest

import (
	"fmt"
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

// GoString implements fmt.GoStringer.
func (f *FileMeta) GoString() string {
	return fmt.Sprintf("%#v", *f)
}

// FileList represents a list of table files.
type FileList []*FileMeta

// TotalFileSize returns total file size from those files.
func (files FileList) TotalFileSize() (size uint64) {
	for _, f := range files {
		size += f.Size
	}
	return
}

// SortByNewestFileNumber sorts files by decreasing file number.
func (files FileList) SortByNewestFileNumber() {
	sort.Sort((byNewestFileNumber)(files))
}

// SortBySmallestKey sorts files by increasing smallest key.
func (files FileList) SortBySmallestKey(cmp keys.Comparer) {
	sort.Sort(&byFileKey{cmp: cmp, files: files})
}

// Dup returns a FileList with same files as this one.
func (files FileList) Dup() FileList {
	return append((FileList)(nil), files...)
}

// IndexFile returns zero-based index for file `number`, -1 if not found.
func (files FileList) IndexFile(number uint64) int {
	for i, f := range files {
		if f.Number == number {
			return i
		}
	}
	return -1
}

// DeleteFile deletes file `number` from files. Returns true if file exists in
// files, otherwise false. The order of files is undefined if it returns true.
func (files *FileList) DeleteFile(number uint64) bool {
	i := files.IndexFile(number)
	if i == -1 {
		return false
	}
	files1 := *files
	n := len(files1) - 1
	files1[i], files1[n] = files1[n], nil
	*files = files1[:n]
	return true
}

type byNewestFileNumber []*FileMeta

var _ sort.Interface = (byNewestFileNumber)(nil)

func (files byNewestFileNumber) Len() int {
	return len(files)
}

func (files byNewestFileNumber) Less(i, j int) bool {
	return files[i].Number > files[j].Number
}

func (files byNewestFileNumber) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}

type byFileKey struct {
	cmp   keys.Comparer
	files []*FileMeta
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
