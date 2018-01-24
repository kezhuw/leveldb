package leveldb

import (
	"unsafe"

	"github.com/kezhuw/leveldb/internal/compaction"
	"github.com/kezhuw/leveldb/internal/compress"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/filter"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/logger"
	"github.com/kezhuw/leveldb/internal/options"
)

const (
	// MaxCompactionConcurrency maximize compaction concurrency as possible.
	// Caution that compaction is a disk drive sensitive task, max compaction
	// concurrency usually doesn't mean good performance.
	MaxCompactionConcurrency = compaction.MaxCompactionConcurrency
)

// CompressionType defines compression methods to compress a table block.
type CompressionType int

const (
	// DefaultCompression defaults to SnappyCompression for now.
	DefaultCompression CompressionType = iota
	// NoCompression means no compression for table block.
	NoCompression
	// SnappyCompression uses snappy compression to compress table block
	// before store it to file.
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

	// BlockCacheCapacity specifys the capacity in bytes for block cache.
	//
	// The default value is 8MiB.
	BlockCacheCapacity int

	// CompactionConcurrency specifies max allowed concurrent compactions.
	//
	// The default value is 1, use MaxCompactionConcurrency to maximize compaction
	// concurrency as poosible.
	CompactionConcurrency int

	// CompactionBytesPerSeek states that one seek cost approximately equal time
	// to compact specified number of data.
	//
	// We decide to compact a file after a certain number of overlap seeks, this
	// way for keys in range we reduce potential seeks by one after compaction.
	// We use CompactionBytesPerSeek and MinimalAllowedOverlapSeeks to calculate
	// the number of allowed overlap seeks for a file.
	//
	// The default value is 16KiB, which means that one seek cost approximately
	// equal time to compact 16KiB data.
	CompactionBytesPerSeek int

	// MinimalAllowedOverlapSeeks specifies minimal allowed overlap seeks per table file.
	//
	// The default value is 100.
	MinimalAllowedOverlapSeeks int

	// IterationBytesPerSampleSeek specifies average iteration bytes for one sample
	// seek to detect overlap file.
	//
	// The default value is 1MiB.
	IterationBytesPerSampleSeek int

	// Level0CompactionFiles specifies that a compaction for level-0 is triggered if
	// there are more than this number of files in level-0.
	//
	// The default value is 4.
	Level0CompactionFiles int

	// Level0SlowdownWriteFiles specifies that writes will be slowdown if there are
	// more than this number of files in level-0.
	//
	// The default value is Level0CompactionFiles + 4.
	Level0SlowdownWriteFiles int

	// Level0StopWriteFiles specifies that writes will be stopped if there are more
	// than this number of files in level-0.
	//
	// The default value is Level0SlowdownWriteFiles + 4.
	Level0StopWriteFiles int

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
	if opts.Logger == nil {
		return nil
	}
	return logger.NopCloser(opts.Logger)
}

func (opts *Options) getFilter() filter.Filter {
	if opts.Filter == nil {
		return nil
	}
	if f, ok := opts.Filter.(internalFilter); ok {
		return f.Filter
	}
	return wrappedFilter{opts.Filter}
}

func (opts *Options) getFileSystem() file.FileSystem {
	if opts.FileSystem == nil {
		return file.DefaultFileSystem
	}
	if fs, ok := opts.FileSystem.(internalFileSystem); ok {
		return fs.FileSystem
	}
	return wrappedFileSystem{opts.FileSystem}
}

func (opts *Options) getComparator() *keys.InternalComparator {
	if opts.Comparator == nil || opts.Comparator == keys.BytewiseComparator {
		return &options.DefaultInternalComparator
	}
	return &keys.InternalComparator{UserKeyComparator: opts.Comparator}
}

func (opts *Options) getCompression() compress.Type {
	switch opts.Compression {
	case NoCompression:
		return compress.NoCompression
	case SnappyCompression:
		return compress.SnappyCompression
	}
	return options.DefaultCompression
}

func (opts *Options) getBlockSize() int {
	if opts.BlockSize <= 0 {
		return options.DefaultBlockSize
	}
	return opts.BlockSize
}

func (opts *Options) getBlockRestartInterval() int {
	if opts.BlockRestartInterval <= 0 {
		return options.DefaultBlockRestartInterval
	}
	return opts.BlockRestartInterval
}

func (opts *Options) getWriteBufferSize() int {
	if opts.WriteBufferSize <= 0 {
		return options.DefaultWriteBufferSize
	}
	return opts.WriteBufferSize
}

func (opts *Options) getMaxOpenFiles() int {
	if opts.MaxOpenFiles <= 0 {
		return options.DefaultMaxOpenFiles
	}
	return opts.MaxOpenFiles
}

func (opts *Options) getBlockCacheCapacity() int {
	if opts.BlockCacheCapacity <= 0 {
		return options.DefaultBlockCacheCapacity
	}
	return opts.BlockCacheCapacity
}

func (opts *Options) getCompactionConcurrency() int {
	switch {
	case opts.CompactionConcurrency == 0:
		return options.DefaultCompactionConcurrency
	case opts.CompactionConcurrency < 0:
		return compaction.MaxCompactionConcurrency
	default:
		return opts.CompactionConcurrency
	}
}

func (opts *Options) getCompactionBytesPerSeek() int {
	if opts.CompactionBytesPerSeek <= 0 {
		return options.DefaultCompactionBytesPerSeek
	}
	return opts.CompactionBytesPerSeek
}

func (opts *Options) getMinimalAllowedOverlapSeeks() int {
	if opts.MinimalAllowedOverlapSeeks <= 0 {
		return options.DefaultMinimalAllowedOverlapSeeks
	}
	return opts.MinimalAllowedOverlapSeeks
}

func (opts *Options) getIterationBytesPerSampleSeek() int {
	if opts.IterationBytesPerSampleSeek <= 0 {
		return options.DefaultIterationBytesPerSampleSeek
	}
	return opts.IterationBytesPerSampleSeek
}

func (opts *Options) getLevel0CompactionFiles() int {
	if opts.Level0CompactionFiles <= 0 {
		return options.DefaultLevel0CompactionFiles
	}
	return opts.Level0CompactionFiles
}

func (opts *Options) getLevel0SlowdownWriteFiles() int {
	if opts.Level0SlowdownWriteFiles <= opts.getLevel0CompactionFiles() {
		return opts.getLevel0CompactionFiles() + options.DefaultLevel0ThrottleStepFiles
	}
	return opts.Level0SlowdownWriteFiles
}

func (opts *Options) getLevel0StopWriteFiles() int {
	if opts.Level0StopWriteFiles <= opts.getLevel0SlowdownWriteFiles() {
		return opts.getLevel0SlowdownWriteFiles() + options.DefaultLevel0ThrottleStepFiles
	}
	return opts.Level0StopWriteFiles
}

func convertOptions(opts *Options) *options.Options {
	if opts == nil {
		return &options.DefaultOptions
	}
	var iopts options.Options
	iopts.Comparator = opts.getComparator()
	iopts.Compression = opts.getCompression()
	iopts.BlockSize = opts.getBlockSize()
	iopts.BlockRestartInterval = opts.getBlockRestartInterval()
	iopts.WriteBufferSize = opts.getWriteBufferSize()
	iopts.MaxOpenFiles = opts.getMaxOpenFiles()
	iopts.BlockCacheCapacity = opts.getBlockCacheCapacity()
	iopts.CompactionConcurrency = opts.getCompactionConcurrency()
	iopts.CompactionBytesPerSeek = opts.getCompactionBytesPerSeek()
	iopts.MinimalAllowedOverlapSeeks = opts.getMinimalAllowedOverlapSeeks()
	iopts.IterationBytesPerSampleSeek = opts.getIterationBytesPerSampleSeek()
	iopts.Level0CompactionFiles = opts.getLevel0CompactionFiles()
	iopts.Level0SlowdownWriteFiles = opts.getLevel0SlowdownWriteFiles()
	iopts.Level0StopWriteFiles = opts.getLevel0StopWriteFiles()
	iopts.Filter = opts.getFilter()
	iopts.Logger = opts.getLogger()
	iopts.FileSystem = opts.getFileSystem()
	iopts.CreateIfMissing = opts.CreateIfMissing
	iopts.ErrorIfExists = opts.ErrorIfExists
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
