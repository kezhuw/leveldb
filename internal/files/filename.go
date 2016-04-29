package files

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kezhuw/leveldb/internal/file"
)

type Kind int

const (
	Invalid Kind = iota
	Lock
	Current
	Manifest
	Table
	Temp
	SSTTable
	Log
	InfoLog
)

var pathSeparator = string(os.PathSeparator)

func (k Kind) String() string {
	switch k {
	case Lock:
		return "LOCK"
	case Current:
		return "CURRENT"
	case Manifest:
		return "MANIFEST"
	case Table, SSTTable:
		return "TABLE"
	case Temp:
		return "TEMP"
	case Log:
		return "LOG"
	case InfoLog:
		return "InfoLog"
	}
	return "unknown"
}

func CurrentFileName(dbname string) string {
	return dbname + pathSeparator + "CURRENT"
}

func LockFileName(dbname string) string {
	return dbname + pathSeparator + "LOCK"
}

func LogFileName(dbname string, number uint64) string {
	return MakeFileName(dbname, number, "log")
}

func TableFileName(dbname string, number uint64) string {
	return MakeFileName(dbname, number, "ldb")
}

func SSTTableFileName(dbname string, number uint64) string {
	return MakeFileName(dbname, number, "sst")
}

func MakeFileName(dbname string, number uint64, ext string) string {
	return fmt.Sprintf("%s%c%06d.%s", dbname, os.PathSeparator, number, ext)
}

func ManifestFileName(dbname string, number uint64) string {
	return fmt.Sprintf("%s%cMANIFEST-%06d", dbname, os.PathSeparator, number)
}

func InfoLogFileName(dbname string) string {
	return dbname + pathSeparator + "LOG"
}

func OldInfoLogFileName(dbname string) string {
	return dbname + pathSeparator + "LOG.old"
}

func Parse(name string) (kind Kind, number uint64) {
	name = filepath.Base(name)
	switch {
	case name == "LOCK":
		return Lock, 0
	case name == "CURRENT":
		return Current, 0
	case name == "LOG" || name == "LOG.old":
		return InfoLog, 0
	case strings.HasPrefix(name, "MANIFEST-"):
		u, err := strconv.ParseUint(name[len("MANIFEST-"):], 10, 64)
		if err != nil {
			break
		}
		return Manifest, u
	default:
		i := strings.IndexByte(name, '.')
		if i <= 0 {
			break
		}
		var kind Kind
		switch ext := name[i+1:]; ext {
		case "ldb", "sst":
			kind = Table
		case "log":
			kind = Log
		case "dbtmp":
			kind = Temp
		default:
			return Invalid, 0
		}
		u, err := strconv.ParseUint(name[:i], 10, 64)
		if err != nil {
			break
		}
		return kind, u
	}
	return Invalid, 0
}

var (
	ErrCorruptCurrentFile = errors.New("corrupt current file")
)

func GetCurrentManifest(fs file.FileSystem, dbname, current string) (string, error) {
	f, err := fs.Open(current, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var buf [128]byte
	n, err := io.ReadFull(f, buf[:])
	switch err {
	case io.ErrUnexpectedEOF:
	case nil, io.EOF:
		return "", ErrCorruptCurrentFile
	default:
		return "", err
	}
	if n == 0 || buf[n-1] != '\n' {
		return "", ErrCorruptCurrentFile
	}
	return dbname + pathSeparator + string(buf[:n-1]), nil
}

func SetCurrentManifest(fs file.FileSystem, dbname, current string, manifestNumber uint64) (err error) {
	tmpFileName := MakeFileName(dbname, manifestNumber, "dbtmp")
	f, err := fs.Open(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			fs.Remove(tmpFileName)
		}
	}()
	manifestName := filepath.Base(ManifestFileName(dbname, manifestNumber))
	_, err = f.Write([]byte(manifestName + "\n"))
	if err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fs.Rename(tmpFileName, current)
}
