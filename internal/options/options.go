package options

import (
	"github.com/kezhuw/leveldb/internal/compress"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/filter"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/logger"
)

const (
	DefaultBlockSize             = 4096
	DefaultBlockRestartInterval  = 16
	DefaultWriteBufferSize       = 4 * 1024 * 1024
	DefaultCompression           = compress.SnappyCompression
	DefaultMaxOpenFiles          = 1000
	DefaultBlockCacheCapacity    = 8 * 1024 * 1024
	DefaultCompactionConcurrency = 1
)

var DefaultInternalComparator keys.InternalComparator = keys.InternalComparator{UserKeyComparator: keys.BytewiseComparator}

type Options struct {
	Comparator  *keys.InternalComparator
	Compression compress.Type
	Filter      filter.Filter
	Logger      logger.LogCloser
	FileSystem  file.FileSystem

	BlockSize             int
	BlockRestartInterval  int
	WriteBufferSize       int
	MaxOpenFiles          int
	BlockCacheCapacity    int
	CompactionConcurrency int

	CreateIfMissing bool
	ErrorIfExists   bool
}

type ReadOptions struct {
	DontFillCache   bool
	VerifyChecksums bool
}

type WriteOptions struct {
	Sync bool
}

var DefaultOptions = Options{
	Comparator:            &DefaultInternalComparator,
	Compression:           compress.SnappyCompression,
	FileSystem:            file.DefaultFileSystem,
	BlockSize:             DefaultBlockSize,
	BlockRestartInterval:  DefaultBlockRestartInterval,
	WriteBufferSize:       DefaultWriteBufferSize,
	MaxOpenFiles:          DefaultMaxOpenFiles,
	BlockCacheCapacity:    DefaultBlockCacheCapacity,
	CompactionConcurrency: DefaultCompactionConcurrency,
}
var DefaultReadOptions = ReadOptions{}
var DefaultWriteOptions = WriteOptions{}
