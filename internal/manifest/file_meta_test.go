package manifest_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/manifest"
)

type fileListTest struct {
	files     manifest.FileList // file list with no repeated number
	totalSize uint64
}

var fileListTests = []fileListTest{
	{
		files:     nil,
		totalSize: 0,
	},
	{
		files: manifest.FileList{
			&manifest.FileMeta{
				Number:   1000,
				Size:     100,
				Smallest: []byte("aaaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("afff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
		},
		totalSize: 100,
	},
	{
		files: manifest.FileList{
			&manifest.FileMeta{
				Number:   1000,
				Size:     100,
				Smallest: []byte("aaaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("afff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   200,
				Size:     128,
				Smallest: []byte("baaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("bfff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
		},
		totalSize: 228,
	},
	{
		files: manifest.FileList{
			&manifest.FileMeta{
				Number:   1000,
				Size:     100,
				Smallest: []byte("aaaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("afff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   2000,
				Size:     256,
				Smallest: []byte("daaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("dfff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   200,
				Size:     128,
				Smallest: []byte("baaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("bfff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
		},
		totalSize: 484,
	},
	{
		files: manifest.FileList{
			&manifest.FileMeta{
				Number:   1000,
				Size:     100,
				Smallest: []byte("aaaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("afff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   2000,
				Size:     256,
				Smallest: []byte("daaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("dfff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   200,
				Size:     128,
				Smallest: []byte("baaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("bfff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   300,
				Size:     512,
				Smallest: []byte("caaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("efff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   3000,
				Size:     112,
				Smallest: []byte("bbaa\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("eeff\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
			&manifest.FileMeta{
				Number:   400,
				Size:     111,
				Smallest: []byte("ccca\x00\x12\x34\x56\x78\x9a\xbc\xde"),
				Largest:  []byte("eeef\x01\x12\x34\x56\x78\x9a\xbc\xde"),
			},
		},
		totalSize: 1219,
	},
}

func equalFileList(a, b manifest.FileList) bool {
	if len(a) != len(b) {
		return false
	}
	for i, f := range a {
		if f != b[i] {
			return false
		}
	}
	return true
}

func isSortedByNewestFileNumber(files manifest.FileList) bool {
	if len(files) == 0 {
		return true
	}
	last := files[0].Number
	for _, f := range files[1:] {
		if f.Number > last {
			return false
		}
		last = f.Number
	}
	return true
}

func isSortedBySmallestKey(files manifest.FileList, cmp keys.Comparer) bool {
	if len(files) == 0 {
		return true
	}
	last := files[0].Smallest
	for _, f := range files[1:] {
		if cmp.Compare(f.Smallest, last) < 0 {
			return false
		}
		last = f.Smallest
	}
	return true
}

func TestFileMetaGoString(t *testing.T) {
	for i, tt := range fileListTests {
		for j, f := range tt.files {
			got := fmt.Sprintf("%#v", f)
			want := fmt.Sprintf("%#v", *f)
			if got != want {
				t.Errorf("test=%d-%d got=%s want=%s", i, j, got, want)
			}
		}
	}
}

func TestFileListDup(t *testing.T) {
	for i, tt := range fileListTests {
		got := tt.files.Dup()
		if !equalFileList(got, tt.files) {
			t.Errorf("test=%d got=%v want=%v", i, got, tt.files)
		}
	}
}

func TestFileListTotalFileSize(t *testing.T) {
	for i, tt := range fileListTests {
		got := tt.files.TotalFileSize()
		if got != tt.totalSize {
			t.Errorf("test=%d got=%d want=%d", i, got, tt.totalSize)
		}
	}
}

func TestFileListSortByNewestFileNumber(t *testing.T) {
	for i, tt := range fileListTests {
		dup := tt.files.Dup()
		dup.SortByNewestFileNumber()
		if !isSortedByNewestFileNumber(dup) {
			t.Errorf("test=%d files=%v got=%v", i, tt.files, dup)
		}
	}
}

func TestFileListSortBySmallestKey(t *testing.T) {
	var comparer keys.Comparer = &keys.InternalComparator{UserKeyComparator: keys.BytewiseComparator}
	for i, tt := range fileListTests {
		dup := tt.files.Dup()
		dup.SortBySmallestKey(comparer)
		if !isSortedBySmallestKey(dup, comparer) {
			t.Errorf("test=%d files=%v got=%v", i, tt.files, dup)
		}
	}
}

func TestFileListIndexFile(t *testing.T) {
	for i, tt := range fileListTests {
		for j, f := range tt.files {
			k := tt.files.IndexFile(f.Number)
			if k != j {
				t.Errorf("test=%d files=%v got=%d want=%d", i, tt.files, k, j)
			}
		}
	}
}

func notFoundFileNumbers(files manifest.FileList) (fileNumbers []uint64) {
	if len(files) != 0 {
		files = files.Dup()
		files.SortByNewestFileNumber()
		largestNumber, smallestNumber := files[0].Number, files[len(files)-1].Number
		if diff := math.MaxUint64 - largestNumber; diff != 0 {
			for step := uint64(1); step <= diff && step < 1000; step += 53 {
				fileNumbers = append(fileNumbers, largestNumber+step)
			}
		}
		if len(files) != 1 {
			lastNumber := largestNumber
			for _, f := range files[1:] {
				currentNumber := f.Number
				middleNumber := (lastNumber + currentNumber) / 2
				if middleNumber != lastNumber && middleNumber != currentNumber {
					fileNumbers = append(fileNumbers, middleNumber)
				}
				lastNumber = currentNumber
			}
		}
		if diff := smallestNumber - 0; diff != 0 {
			for step := uint64(1); step <= diff && step < 2000; step += 117 {
				fileNumbers = append(fileNumbers, smallestNumber-step)
			}
		}
	}
	return
}

func TestFileListDeleteFile(t *testing.T) {
	for i, tt := range fileListTests {
		for _, f := range tt.files {
			dup := tt.files.Dup()
			ok := dup.DeleteFile(f.Number)
			notFound := dup.IndexFile(f.Number)
			if !ok || notFound != -1 || len(tt.files) != len(dup)+1 {
				t.Errorf("test=%d files=%v fileNumber=%d got=%v", i, tt.files, f.Number, dup)
			}
		}
	}
}

func TestFileListIndexFileNotFound(t *testing.T) {
	for i, tt := range fileListTests {
		for _, fileNumber := range notFoundFileNumbers(tt.files) {
			got := tt.files.IndexFile(fileNumber)
			if got != -1 {
				t.Errorf("test=%d files=%v fileNumber=%d got=%d want=%d", i, tt.files, fileNumber, got, -1)
			}
		}
	}
}

func TestFileListDeleteFileNotFound(t *testing.T) {
	for i, tt := range fileListTests {
		for _, fileNumber := range notFoundFileNumbers(tt.files) {
			dup := tt.files.Dup()
			ok := dup.DeleteFile(fileNumber)
			if ok || len(tt.files) != len(dup) {
				t.Errorf("test=%d files=%#v fileNumber=%d got=%v", i, tt.files, fileNumber, dup)
			}
		}
	}
}
