// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package file

import (
	"fmt"
	"io"
	"runtime"
)

var errNoFileLocking = fmt.Errorf("leveldb: file locking is not implemented on %s/%s", runtime.GOOS, runtime.GOARCH)

func (osFileSystem) Lock(name string) (io.Closer, error) {
	return nil, errNoFileLocking
}
