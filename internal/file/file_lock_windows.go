package file

import (
	"io"
	"syscall"
)

type lockCloser struct {
	fd syscall.Handle
}

func (l lockCloser) Close() error {
	return syscall.Close(l.fd)
}

func (osFileSystem) Lock(name string) (io.Closer, error) {
	path, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	fd, err := syscall.CreateFile(
		path,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}
	return lockCloser{fd: fd}, nil
}
