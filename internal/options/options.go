package options

import (
	"fmt"
	"reflect"

	"github.com/kezhuw/leveldb/internal/compress"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/filter"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/logger"
)

const (
	DefaultBlockSize            = 4096
	DefaultBlockRestartInterval = 16
	DefaultWriteBufferSize      = 4 * 1024 * 1024
	DefaultCompression          = compress.SnappyCompression
	DefaultMaxOpenFiles         = 1000
	DefaultBlockCacheSize       = 8 * 1024 * 1024
)

var defaults = map[string]int64{
	"BlockSize":            DefaultBlockSize,
	"BlockRestartInterval": DefaultBlockRestartInterval,
	"WriteBufferSize":      DefaultWriteBufferSize,
	"MaxOpenFiles":         DefaultMaxOpenFiles,
	"BlockCacheSize":       DefaultBlockCacheSize,
}

type Options struct {
	Comparator  *keys.InternalComparator
	Compression compress.Type
	Filter      filter.Filter
	Logger      logger.LogCloser
	FileSystem  file.FileSystem

	BlockSize            int   `default`
	BlockRestartInterval int   `default`
	WriteBufferSize      int   `default`
	MaxOpenFiles         int   `default`
	BlockCacheSize       int64 `default`

	CreateIfMissing bool `default`
	ErrorIfExists   bool `default`
}

func (opts *Options) SetDefaults(v reflect.Value) error {
	v = reflect.Indirect(v)
	p := reflect.ValueOf(opts).Elem()
	t := p.Type()
	for i, n := 0, p.NumField(); i < n; i++ {
		ft := t.Field(i)
		if ft.Tag != "default" {
			continue
		}
		vv := v.FieldByName(ft.Name)
		switch {
		case vv.Kind() == reflect.Invalid:
			return fmt.Errorf("leveldb: Options has no field named `%s` with type `%s`", ft.Name, ft.Type)
		case vv.Kind() != ft.Type.Kind():
			return fmt.Errorf("leveldb: Options field `%s` has type `%s`, differs from internal type `%s`", ft.Name, vv.Kind(), ft.Type)
		}
		switch ft.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			x := vv.Int()
			if x <= 0 {
				x = defaults[ft.Name]
			}
			p.Field(i).SetInt(x)
		default:
			p.Field(i).Set(vv)
		}
	}
	return nil
}

type ReadOptions struct {
	DontFillCache   bool
	VerifyChecksums bool
}

type WriteOptions struct {
	Sync bool
}

var DefaultOptions = Options{
	Comparator:           &keys.InternalComparator{keys.BytewiseComparator},
	Compression:          compress.SnappyCompression,
	FileSystem:           file.DefaultFileSystem,
	BlockSize:            DefaultBlockSize,
	BlockRestartInterval: DefaultBlockRestartInterval,
	WriteBufferSize:      DefaultWriteBufferSize,
	MaxOpenFiles:         DefaultMaxOpenFiles,
	BlockCacheSize:       DefaultBlockCacheSize,
}
var DefaultReadOptions = ReadOptions{}
var DefaultWriteOptions = WriteOptions{}
