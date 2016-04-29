package leveldb

import (
	"reflect"
	"unsafe"

	"github.com/kezhuw/leveldb/internal/compress"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/filter"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/logger"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/util"
)

// CompressionType defines compression methods to compress a table block.
type CompressionType int

const (
	DefaultCompression CompressionType = iota // Points to SnappyCompression
	NoCompression
	SnappyCompression
)

// Options contains options controlling various parts of the db instance.
type Options struct {
	// Comparator defines the total order over keys in the database.
	//
	// The default comparator is BytewiseComparator, which uses the same ordering
	// as bytes.Compare.
	Comparator Comparator

	// Compression type used to compress blocks.
	//
	// The default value points to SnappyCompression.
	Compression CompressionType

	// BlockSize specifys the minimum uncompressed size in bytes for a table block.
	//
	// The default value is 4KiB.
	BlockSize int

	// BlockRestartInterval specifys the number of keys between restart points
	// for delta encoding of keys in a block.
	//
	// The default value is 16.
	BlockRestartInterval int

	// WriteBufferSize is the amount of data to build up in memory (backed by
	// an unsorted log on disk) before converting to a sorted on-disk file.
	//
	// Larger values increase performance, especially during bulk loads. Up to two
	// write buffers may be held in memory at the same time, so you may wish to
	// adjust this parameter to control memory usage. Also, a larger write buffer
	// will result in a longer recovery time the next time the database is opened.
	//
	// The default value is 4MiB.
	WriteBufferSize int

	// MaxOpenFiles is the number of open files that can be used this db instance.
	// You may need to increase this if your database has a large number of files.
	//
	// The default value is 1000.
	MaxOpenFiles int

	// BlockCacheSize specifys the capacity in bytes for block cache.
	//
	// The default value is 8MiB.
	BlockCacheSize int64

	// Filter specifys a Filter to filter out unnecessary disk reads when looking for
	// a specific key. The filter is also used to generate filter data when building
	// table files.
	//
	// The default value is nil.
	Filter Filter

	// Logger specifys a place that all internal progress/error information generated
	// by this db instance will be written to.
	//
	// The default value is a file named "LOG" stored under this db directory. You can
	// suppress logging by using DiscardLogger.
	Logger Logger

	// FileSystem defines a hierarchical file storage interface.
	//
	// The default file system is built around os package.
	FileSystem FileSystem

	// CreateIfMissing specifys whether to create one if the database does not exist.
	//
	// The default value is false.
	CreateIfMissing bool

	// ErrorIfExists specifys whether to report a error if the database already exists.
	//
	// The default value is false.
	ErrorIfExists bool
}

func (opts *Options) getLogger() logger.LogCloser {
	if opts == nil || opts.Logger == nil {
		return nil
	}
	return logger.NopCloser(opts.Logger)
}

func (opts *Options) getFilter() filter.Filter {
	if opts == nil || opts.Filter == nil {
		return nil
	}
	if f, ok := opts.Filter.(internalFilter); ok {
		return f.Filter
	}
	return wrappedFilter{opts.Filter}
}

func (opts *Options) getFileSystem() file.FileSystem {
	if opts == nil || opts.FileSystem == nil {
		return file.DefaultFileSystem
	}
	if fs, ok := opts.FileSystem.(internalFileSystem); ok {
		return fs.FileSystem
	}
	return wrappedFileSystem{opts.FileSystem}
}

func (opts *Options) getComparator() keys.UserComparator {
	if opts == nil || opts.Comparator == nil {
		return keys.BytewiseComparator
	}
	return opts.Comparator
}

func (opts *Options) getCompression() compress.Type {
	if opts == nil {
		return options.DefaultCompression
	}
	switch opts.Compression {
	case NoCompression:
		return compress.NoCompression
	case SnappyCompression:
		return compress.SnappyCompression
	}
	return options.DefaultCompression
}

func convertOptions(opts *Options) *options.Options {
	if opts == nil {
		return &options.DefaultOptions
	}
	var iopts options.Options
	iopts.SetDefaults(reflect.ValueOf(opts))
	iopts.Comparator = &keys.InternalComparator{opts.getComparator()}
	iopts.Compression = opts.getCompression()
	iopts.Filter = opts.getFilter()
	iopts.Logger = opts.getLogger()
	iopts.FileSystem = opts.getFileSystem()
	return &iopts
}

// ReadOptions contains options controlling behaviours of read operations.
type ReadOptions struct {
	// DontFillCache specifys whether data read in this operation
	// should be cached in memory. If true, data read from underlying
	// storage will not be cahced in memory for later reading, but
	// if the data is already cached in memory, it will be used by
	// this operation.
	DontFillCache bool

	// VerifyChecksums specifys whether data read from underlying
	// storage should be verified against saved checksums. Note that
	// it never verify data cached in memory.
	VerifyChecksums bool
}

func convertReadOptions(opts *ReadOptions) *options.ReadOptions {
	if opts == nil {
		return &options.DefaultReadOptions
	}
	return (*options.ReadOptions)(unsafe.Pointer(opts))
}

// WriteOptions contains options controlling write operations: Put, Delete,
// and Write.
type WriteOptions struct {
	// Sync specifys whether to synchronize the write from OS cache to
	// underlying storage before the write is considered complete.
	// Setting Sync to true may result in slower writes.
	//
	// If Sync is false, and the machine crashs, some recent writes may
	// be lost. Note that if it is just the process crashs, no writes will
	// be lost.
	//
	// In other words, a write with false Sync has similar crash semantics
	// as the "write()" system call. A write with true Sync has similar crash
	/// semantics to a "write()" system call followed by "fsync()".
	Sync bool
}

func convertWriteOptions(opts *WriteOptions) *options.WriteOptions {
	if opts == nil {
		return &options.DefaultWriteOptions
	}
	return (*options.WriteOptions)(unsafe.Pointer(opts))
}

func (opts *WriteOptions) getSync() bool {
	if opts == nil {
		return false
	}
	return opts.Sync
}

func init() {
	if !util.SameLayoutStructs(reflect.TypeOf(ReadOptions{}), reflect.TypeOf(options.ReadOptions{})) {
		panic("leveldb: layout of ReadOptions differs from internalFilter one")
	}
	if !util.SameLayoutStructs(reflect.TypeOf(WriteOptions{}), reflect.TypeOf(options.WriteOptions{})) {
		panic("leveldb: layout of WriteOptions differs from internal one")
	}
}
