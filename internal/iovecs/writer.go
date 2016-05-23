package iovecs

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

type Writer interface {
	Write(p []byte) (int, error)
	Writev(slices ...[]byte) (int64, error)
}

type fileWriter struct {
	*os.File
	fd uintptr
}

func (f *fileWriter) Writev(slices ...[]byte) (int64, error) {
	vecs := make([]syscall.Iovec, 0, len(slices))
	for _, b := range slices {
		if len(b) == 0 {
			continue
		}
		vecs = append(vecs, syscall.Iovec{Base: &b[0], Len: uint64(len(b))})
	}
	if len(vecs) == 0 {
		return 0, nil
	}
	r, _, errno := syscall.Syscall(syscall.SYS_WRITEV, f.fd, uintptr(unsafe.Pointer(&vecs[0])), uintptr(len(vecs)))
	if errno != 0 {
		return int64(r), errno
	}
	return int64(r), nil
}

type ioWriter struct {
	io.Writer
}

func (w *ioWriter) Writev(slices ...[]byte) (int64, error) {
	var written int64
	for _, b := range slices {
		n, err := w.Write(b)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func NewWriter(w io.Writer) Writer {
	if f, ok := w.(*os.File); ok {
		return &fileWriter{File: f, fd: f.Fd()}
	}
	return &ioWriter{w}
}
