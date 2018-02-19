package file

import (
	"io"
	"os"
)

type Reader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type Writer interface {
	io.Writer
	io.Seeker
	Truncate(size int64) error
	Sync() error
}

type ReadCloser interface {
	Reader
	io.Closer
}

type WriteCloser interface {
	Writer
	io.Closer
}

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
	// someone else, failed with an error instead of blocking.
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

type osFileSystem struct{}

func (osFileSystem) Open(name string, flag int) (File, error) {
	return os.OpenFile(name, flag, 0666)
}

func (osFileSystem) MkdirAll(path string) error {
	return os.MkdirAll(path, 0740)
}

func (osFileSystem) Exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (osFileSystem) List(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	return f.Readdirnames(-1)
}

func (osFileSystem) Remove(name string) error {
	return os.Remove(name)
}

func (osFileSystem) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

var DefaultFileSystem FileSystem = osFileSystem{}
