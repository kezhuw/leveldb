// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package file

import (
	"io"
	"os"
	"syscall"
)

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
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	return lockCloser{f}, nil
}
