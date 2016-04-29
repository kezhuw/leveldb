package file

import (
	"io"
	"os"
	"syscall"
)

func Lock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func Unlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN|syscall.LOCK_NB)
}

type lockCloser struct {
	f *os.File
}

func (l lockCloser) Close() error {
	return l.f.Close()
}

func (osFileSystem) Lock(name string) (io.Closer, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	if err := Lock(f); err != nil {
		return nil, err
	}
	return lockCloser{f}, nil
}
