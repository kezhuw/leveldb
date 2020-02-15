package options

import (
	"github.com/kezhuw/leveldb/internal/compress"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/filter"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/logger"
)

const (
	DefaultMaxFileSize           = 2 * 1024 * 1024
	DefaultBlockSize             = 4096
	DefaultBlockRestartInterval  = 16
	DefaultBlockCompressionRatio = 8.0 / 7.0
	DefaultWriteBufferSize       = 4 * 1024 * 1024
	DefaultCompression           = compress.SnappyCompression
	DefaultMaxOpenFiles          = 1000
	DefaultBlockCacheCapacity    = 8 * 1024 * 1024
	DefaultCompactionConcurrency = 1

	// DefaultCompactionBytesPerSeek gives a default value for
	// CompactionBytesPerSeek option based on following assumptions:
	//   (1) One seek costs 10ms
	//   (2) Writing or reading 1MB costs 10ms (100MB/s)
	//   (3) A compaction of 1MB does 25MB of IO:
	//         1MB read from this level
	//         10-12MB read from next level (boundaries may be misaligned)
	//         10-12MB written to next level
	// This implies that 25 seeks cost the same as the compaction
	// of 1MB of data.  I.e., one seek costs approximately the
	// same as the compaction of 40KB of data. We are a little
	// conservative and allow approximately one seek for every 16KB
	// of data before triggering a compaction.    -- LevelDB (C++)
	DefaultCompactionBytesPerSeek = 16 * 1024

	DefaultMinimalAllowedOverlapSeeks  = 100
	DefaultIterationBytesPerSampleSeek = 1024 * 1024

	DefaultLevel0ThrottleStepFiles  = 4
	DefaultLevel0CompactionFiles    = 4
	DefaultLevel0SlowdownWriteFiles = DefaultLevel0CompactionFiles + DefaultLevel0ThrottleStepFiles
	DefaultLevel0StopWriteFiles     = DefaultLevel0SlowdownWriteFiles + DefaultLevel0ThrottleStepFiles
)

var DefaultInternalComparator keys.InternalComparator = keys.InternalComparator{UserKeyComparator: keys.BytewiseComparator}

func MaxGrandparentOverlapBytes(maxFileSize int64) int64 {
	return 10 * maxFileSize
}

func MaxExpandedCompactionBytes(maxFileSize int64) int64 {
	return 25 * maxFileSize
}

type Options struct {
	Comparator  *keys.InternalComparator
	Compression compress.Type
	Filter      filter.Filter
	Logger      logger.LogCloser
	FileSystem  file.FileSystem

	MaxFileSize                int64
	MaxGrandparentOverlapBytes int64
	MaxExpandedCompactionBytes int64

	BlockSize                   int
	BlockRestartInterval        int
	BlockCompressionRatio       float64
	WriteBufferSize             int
	MaxOpenFiles                int
	BlockCacheCapacity          int
	CompactionConcurrency       int
	CompactionBytesPerSeek      int
	MinimalAllowedOverlapSeeks  int
	IterationBytesPerSampleSeek int
	Level0CompactionFiles       int
	Level0SlowdownWriteFiles    int
	Level0StopWriteFiles        int

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
	Comparator:                  &DefaultInternalComparator,
	Compression:                 compress.SnappyCompression,
	FileSystem:                  file.DefaultFileSystem,
	MaxFileSize:                 DefaultMaxFileSize,
	MaxGrandparentOverlapBytes:  MaxGrandparentOverlapBytes(DefaultMaxFileSize),
	MaxExpandedCompactionBytes:  MaxExpandedCompactionBytes(DefaultMaxFileSize),
	BlockSize:                   DefaultBlockSize,
	BlockRestartInterval:        DefaultBlockRestartInterval,
	BlockCompressionRatio:       DefaultBlockCompressionRatio,
	WriteBufferSize:             DefaultWriteBufferSize,
	MaxOpenFiles:                DefaultMaxOpenFiles,
	BlockCacheCapacity:          DefaultBlockCacheCapacity,
	CompactionConcurrency:       DefaultCompactionConcurrency,
	CompactionBytesPerSeek:      DefaultCompactionBytesPerSeek,
	MinimalAllowedOverlapSeeks:  DefaultMinimalAllowedOverlapSeeks,
	IterationBytesPerSampleSeek: DefaultIterationBytesPerSampleSeek,
	Level0CompactionFiles:       DefaultLevel0CompactionFiles,
	Level0SlowdownWriteFiles:    DefaultLevel0SlowdownWriteFiles,
	Level0StopWriteFiles:        DefaultLevel0StopWriteFiles,
}
var DefaultReadOptions = ReadOptions{}
var DefaultWriteOptions = WriteOptions{}
