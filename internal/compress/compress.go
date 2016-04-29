package compress

import (
	"errors"

	"github.com/golang/snappy"
)

type Type int

const (
	NoCompression     Type = 0
	SnappyCompression Type = 1
)

var (
	ErrNoCompression          = errors.New("leveldb: no compression")
	ErrUnsupportedCompression = errors.New("leveldb: unsupported compression")
)

func Decode(typ Type, dst, src []byte) ([]byte, error) {
	switch typ {
	case NoCompression:
		return nil, ErrNoCompression
	case SnappyCompression:
		return snappy.Decode(dst, src)
	default:
		return nil, ErrUnsupportedCompression
	}
}

func Encode(typ Type, dst, src []byte) ([]byte, error) {
	switch typ {
	case NoCompression:
		return nil, ErrNoCompression
	case SnappyCompression:
		return snappy.Encode(dst, src), nil
	default:
		return nil, ErrUnsupportedCompression
	}
}
