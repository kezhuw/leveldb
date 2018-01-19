package manifest

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/kezhuw/leveldb/internal/keys"
)

// FileMeta contains meta info for a sorted table.
type FileMeta struct {
	// Number of overlap seeks to next level(or file in level 0) before we
	// decide to compact this file to next level.
	allowedOverlapSeeks int64

	Number   uint64
	Size     uint64
	Smallest keys.InternalKey
	Largest  keys.InternalKey
}

func (f *FileMeta) resetAllowedSeeks(compactionBytesCost int, minimalSeeks int) {
	allowedOverlapSeeks := int64(f.Size / uint64(compactionBytesCost))
	if allowedOverlapSeeks < int64(minimalSeeks) {
		allowedOverlapSeeks = int64(minimalSeeks)
	}
	f.allowedOverlapSeeks = allowedOverlapSeeks
}

// seekOverlap consumes allowed seeks due to overlapp and returns whether
// caller should compact this file.
func (f *FileMeta) seekOverlap() bool {
	return atomic.AddInt64(&f.allowedOverlapSeeks, -1) == 0
}

// seekThrough consumes allowed seeks due to missing read and returns whether
// caller should compact this file.
func (f *FileMeta) seekThrough() bool {
	r := atomic.AddInt64(&f.allowedOverlapSeeks, -2)
	return r <= 0 && r > -2
}

// GoString implements fmt.GoStringer.
func (f *FileMeta) GoString() string {
	return fmt.Sprintf("%#v", *f)
}

// LevelFileMeta includes a file's meta data and its corresponding level.
type LevelFileMeta struct {
	Level int
	*FileMeta
}

func (f LevelFileMeta) seekOverlap() LevelFileMeta {
	if file := f.FileMeta; file != nil && file.seekOverlap() {
		return f
	}
	return LevelFileMeta{}
}

func (f LevelFileMeta) seekThrough() LevelFileMeta {
	if file := f.FileMeta; file != nil && file.seekThrough() {
		return f
	}
	return LevelFileMeta{}
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
