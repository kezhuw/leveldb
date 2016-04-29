package leveldb

import (
	"io"

	"github.com/kezhuw/leveldb/internal/file"
)

// File defines methods on one file.
type File interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	io.ReaderAt
	Truncate(size int64) error
	Sync() error
}

// FileSystem defines methods for hierarchical file storage.
type FileSystem interface {
	// Open opens a file using specified flag.
	Open(name string, flag int) (File, error)

	// Lock locks a file for exclusive usage. If the file is locked by
	// someone else, failed with an error instead of blocking. If ok,
	// returns an io.Closer to let caller to release underly locker.
	Lock(name string) (io.Closer, error)

	// Exists returns true if the named file exists.
	Exists(name string) bool

	// MkdirAll creates a directory and all necessary parents.
	MkdirAll(path string) error

	// Lists returns all contents of the directory.
	List(dir string) ([]string, error)

	// Remove removes named file or directory.
	Remove(filename string) error

	// Rename renames(moves) oldpath to newpath. If newpath already exists,
	// Rename replaces it.
	Rename(oldpath, newpath string) error
}

type internalFileSystem struct {
	file.FileSystem
}

func (fs internalFileSystem) Open(name string, flag int) (File, error) {
	return fs.FileSystem.Open(name, flag)
}

type wrappedFileSystem struct {
	FileSystem
}

func (fs wrappedFileSystem) Open(name string, flag int) (file.File, error) {
	return fs.FileSystem.Open(name, flag)
}

// DefaultFileSystem is the file system provided by os package.
var DefaultFileSystem FileSystem = internalFileSystem{file.DefaultFileSystem}

var _ FileSystem = internalFileSystem{}
var _ file.FileSystem = wrappedFileSystem{}
