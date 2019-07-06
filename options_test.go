package leveldb

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/kezhuw/leveldb/internal/compaction"
	"github.com/kezhuw/leveldb/internal/compress"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/filter"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/logger"
	"github.com/kezhuw/leveldb/internal/options"
)

var (
	filterBuffer = new(bytes.Buffer)
	loggerBuffer = new(bytes.Buffer)
	fsBuffer     = new(bytes.Buffer)
)

type bufferFilter struct {
	buf *bytes.Buffer
}

func (f *bufferFilter) Name() string {
	name := "dummy-filter"
	f.buf.WriteString(name)
	return name
}

func (f *bufferFilter) Append(buf *bytes.Buffer, keys [][]byte) {
}

func (f *bufferFilter) Contains(data, key []byte) bool {
	return true
}

func (f *bufferFilter) NewGenerator() Generator {
	return nil
}

func newBufferFilter(buf *bytes.Buffer) Filter {
	return &bufferFilter{buf: buf}
}

func matchFilter(filter filter.Filter, buf *bytes.Buffer) bool {
	if filter == nil {
		return buf == nil
	}
	buf.Reset()
	name := filter.Name()
	return name == buf.String()
}

type bufferLogger struct {
	buf *bytes.Buffer
}

func (l *bufferLogger) Debugf(format string, args ...interface{}) {
	l.buf.WriteString(fmt.Sprintf("debug: "+format, args...))
}

func (l *bufferLogger) Infof(format string, args ...interface{}) {
	l.buf.WriteString(fmt.Sprintf("info: "+format, args...))
}

func (l *bufferLogger) Warnf(format string, args ...interface{}) {
	l.buf.WriteString(fmt.Sprintf("warn: "+format, args...))
}

func (l *bufferLogger) Errorf(format string, args ...interface{}) {
	l.buf.WriteString(fmt.Sprintf("error: "+format, args...))
}

func newBufferLogger(buf *bytes.Buffer) Logger {
	return &bufferLogger{buf: buf}
}

func matchLogger(l logger.Logger, buf *bytes.Buffer) bool {
	if l == nil {
		return buf == nil
	}
	buf.Reset()
	l.Debugf("logging %s", "matching")
	want := fmt.Sprintf("debug: logging %s", "matching")
	got := buf.String()
	return got == want
}

type bufferFileSystem struct {
	buf *bytes.Buffer
}

func (fs *bufferFileSystem) Open(name string, flat int) (File, error) {
	fs.buf.WriteString("open: " + name)
	return nil, nil
}

func (fs *bufferFileSystem) Lock(name string) (io.Closer, error) {
	return nil, nil
}

func (fs *bufferFileSystem) Exists(name string) bool {
	return false
}

func (fs *bufferFileSystem) MkdirAll(path string) error {
	return nil
}

func (fs *bufferFileSystem) List(dir string) ([]string, error) {
	return nil, nil
}

func (fs *bufferFileSystem) Remove(filename string) error {
	return nil
}

func (fs *bufferFileSystem) Rename(oldpath, newpath string) error {
	return nil
}

func newBufferFileSystem(buf *bytes.Buffer) FileSystem {
	return &bufferFileSystem{buf: buf}
}

func matchFileSystem(fs file.FileSystem, buf *bytes.Buffer) bool {
	if buf == nil {
		return fs == file.DefaultFileSystem
	}
	buf.Reset()
	fs.Open("file 1", 0)
	return buf.String() == "open: file 1"
}

type optionsTest struct {
	options                     *Options
	comparator                  keys.Comparator
	compression                 compress.Type
	blockSize                   int
	blockRestartInterval        int
	blockCompressionRatio       float64
	writeBufferSize             int
	maxOpenFiles                int
	blockCacheCapacity          int
	compactionConcurrency       int
	compactionBytesPerSeek      int
	minimalAllowedOverlapSeeks  int
	iterationBytesPerSampleSeek int
	level0CompactionFiles       int
	level0SlowdownWriteFiles    int
	level0StopWriteFiles        int
	filterBuffer                *bytes.Buffer
	loggerBuffer                *bytes.Buffer
	fsBuffer                    *bytes.Buffer
}

var optionsTests = []optionsTest{
	{
		comparator:                  keys.BytewiseComparator,
		compression:                 compress.SnappyCompression,
		blockSize:                   options.DefaultBlockSize,
		blockRestartInterval:        options.DefaultBlockRestartInterval,
		blockCompressionRatio:       options.DefaultBlockCompressionRatio,
		writeBufferSize:             options.DefaultWriteBufferSize,
		maxOpenFiles:                options.DefaultMaxOpenFiles,
		blockCacheCapacity:          options.DefaultBlockCacheCapacity,
		compactionConcurrency:       options.DefaultCompactionConcurrency,
		compactionBytesPerSeek:      options.DefaultCompactionBytesPerSeek,
		minimalAllowedOverlapSeeks:  options.DefaultMinimalAllowedOverlapSeeks,
		iterationBytesPerSampleSeek: options.DefaultIterationBytesPerSampleSeek,
		level0CompactionFiles:       options.DefaultLevel0CompactionFiles,
		level0SlowdownWriteFiles:    options.DefaultLevel0SlowdownWriteFiles,
		level0StopWriteFiles:        options.DefaultLevel0StopWriteFiles,
	},
	{
		options: &Options{
			Compression:           SnappyCompression,
			BlockCompressionRatio: 7.0 / 10.0,
			CompactionConcurrency: MaxCompactionConcurrency,
			Level0CompactionFiles: 10,
		},
		comparator:                  keys.BytewiseComparator,
		compression:                 compress.SnappyCompression,
		blockSize:                   options.DefaultBlockSize,
		blockRestartInterval:        options.DefaultBlockRestartInterval,
		blockCompressionRatio:       options.DefaultBlockCompressionRatio,
		writeBufferSize:             options.DefaultWriteBufferSize,
		maxOpenFiles:                options.DefaultMaxOpenFiles,
		blockCacheCapacity:          options.DefaultBlockCacheCapacity,
		compactionConcurrency:       compaction.MaxCompactionConcurrency,
		compactionBytesPerSeek:      options.DefaultCompactionBytesPerSeek,
		minimalAllowedOverlapSeeks:  options.DefaultMinimalAllowedOverlapSeeks,
		iterationBytesPerSampleSeek: options.DefaultIterationBytesPerSampleSeek,
		level0CompactionFiles:       10,
		level0SlowdownWriteFiles:    10 + options.DefaultLevel0ThrottleStepFiles,
		level0StopWriteFiles:        10 + options.DefaultLevel0ThrottleStepFiles + options.DefaultLevel0ThrottleStepFiles,
	},
	{
		options: &Options{
			Comparator:                  keys.BytewiseComparator,
			Compression:                 NoCompression,
			BlockSize:                   options.DefaultBlockSize * 4,
			BlockRestartInterval:        options.DefaultBlockRestartInterval + 2,
			BlockCompressionRatio:       10.0 / 7.0,
			WriteBufferSize:             options.DefaultWriteBufferSize + 4096,
			MaxOpenFiles:                options.DefaultMaxOpenFiles + 512,
			BlockCacheCapacity:          options.DefaultBlockCacheCapacity + 4096,
			CompactionConcurrency:       5,
			Filter:                      newBufferFilter(filterBuffer),
			Logger:                      newBufferLogger(loggerBuffer),
			FileSystem:                  newBufferFileSystem(fsBuffer),
			CompactionBytesPerSeek:      32 * 1024,
			MinimalAllowedOverlapSeeks:  50,
			IterationBytesPerSampleSeek: 64 * 1024,
			Level0CompactionFiles:       10,
			Level0SlowdownWriteFiles:    12,
			Level0StopWriteFiles:        14,
		},
		comparator:                  keys.BytewiseComparator,
		compression:                 compress.NoCompression,
		blockSize:                   options.DefaultBlockSize * 4,
		blockRestartInterval:        options.DefaultBlockRestartInterval + 2,
		blockCompressionRatio:       10.0 / 7.0,
		writeBufferSize:             options.DefaultWriteBufferSize + 4096,
		maxOpenFiles:                options.DefaultMaxOpenFiles + 512,
		blockCacheCapacity:          options.DefaultBlockCacheCapacity + 4096,
		compactionConcurrency:       5,
		compactionBytesPerSeek:      32 * 1024,
		minimalAllowedOverlapSeeks:  50,
		iterationBytesPerSampleSeek: 64 * 1024,
		level0CompactionFiles:       10,
		level0SlowdownWriteFiles:    12,
		level0StopWriteFiles:        14,
		filterBuffer:                filterBuffer,
		loggerBuffer:                loggerBuffer,
		fsBuffer:                    fsBuffer,
	},
}

func TestOptions(t *testing.T) {
	for i, test := range optionsTests {
		opts := test.options
		if opts == nil {
			opts = &Options{}
		}
		if icmp := opts.getComparator(); icmp.UserKeyComparator != test.comparator {
			t.Errorf("test=%d-Comparator got=%#v want=%v", i, icmp.UserKeyComparator, test.comparator)
		}
		if compression := opts.getCompression(); compression != test.compression {
			t.Errorf("test=%d-Compression got=%v want=%v", i, compression, test.compression)
		}
		if blockSize := opts.getBlockSize(); blockSize != test.blockSize {
			t.Errorf("test=%d-BlockSize got=%d want=%v", i, blockSize, test.blockSize)
		}
		if blockRestartInterval := opts.getBlockRestartInterval(); blockRestartInterval != test.blockRestartInterval {
			t.Errorf("test=%d-BlockRestartInterval got=%d want=%v", i, blockRestartInterval, test.blockRestartInterval)
		}
		if blockCompressionRatio := opts.getBlockCompressionRatio(); blockCompressionRatio != test.blockCompressionRatio {
			t.Errorf("test=%d-BlockCompressionRatio got=%v want=%v", i, blockCompressionRatio, test.blockCompressionRatio)
		}
		if writeBufferSize := opts.getWriteBufferSize(); writeBufferSize != test.writeBufferSize {
			t.Errorf("test=%d-WriteBufferSize got=%d want=%v", i, writeBufferSize, test.writeBufferSize)
		}
		if maxOpenFiles := opts.getMaxOpenFiles(); maxOpenFiles != test.maxOpenFiles {
			t.Errorf("test=%d-MaxOpenFiles got=%d want=%v", i, maxOpenFiles, test.maxOpenFiles)
		}
		if blockCacheCapacity := opts.getBlockCacheCapacity(); blockCacheCapacity != test.blockCacheCapacity {
			t.Errorf("test=%d-BlockCacheCapacity got=%d want=%v", i, blockCacheCapacity, test.blockCacheCapacity)
		}
		if compactionConcurrency := opts.getCompactionConcurrency(); compactionConcurrency != test.compactionConcurrency {
			t.Errorf("test=%d-CompactionConcurrency got=%d want=%v", i, compactionConcurrency, test.compactionConcurrency)
		}
		if compactionBytesPerSeek := opts.getCompactionBytesPerSeek(); compactionBytesPerSeek != test.compactionBytesPerSeek {
			t.Errorf("test=%d-CompactionBytesPerSeek got=%d want=%v", i, compactionBytesPerSeek, test.compactionBytesPerSeek)
		}
		if minimalAllowedOverlapSeeks := opts.getMinimalAllowedOverlapSeeks(); minimalAllowedOverlapSeeks != test.minimalAllowedOverlapSeeks {
			t.Errorf("test=%d-MinimalAllowedOverlapSeeks got=%d want=%v", i, minimalAllowedOverlapSeeks, test.minimalAllowedOverlapSeeks)
		}
		if iterationBytesPerSampleSeek := opts.getIterationBytesPerSampleSeek(); iterationBytesPerSampleSeek != test.iterationBytesPerSampleSeek {
			t.Errorf("test=%d-IterationBytesPerSampleSeek got=%d want=%v", i, iterationBytesPerSampleSeek, test.iterationBytesPerSampleSeek)
		}
		if level0CompactionFiles := opts.getLevel0CompactionFiles(); level0CompactionFiles != test.level0CompactionFiles {
			t.Errorf("test=%d-Level0CompactionFiles got=%d want=%d", i, level0CompactionFiles, test.level0CompactionFiles)
		}
		if level0SlowdownWriteFiles := opts.getLevel0SlowdownWriteFiles(); level0SlowdownWriteFiles != test.level0SlowdownWriteFiles {
			t.Errorf("test=%d-Level0SlowdownWriteFiles got=%d want=%d", i, level0SlowdownWriteFiles, test.level0SlowdownWriteFiles)
		}
		if level0StopWriteFiles := opts.getLevel0StopWriteFiles(); level0StopWriteFiles != test.level0StopWriteFiles {
			t.Errorf("test=%d-Level0StopWriteFiles got=%d want=%d", i, level0StopWriteFiles, test.level0StopWriteFiles)
		}
		if filter := opts.getFilter(); !matchFilter(filter, test.filterBuffer) {
			t.Errorf("test=%d-Filter got=%v", i, filter)
		}
		if logger := opts.getLogger(); !matchLogger(logger, test.loggerBuffer) {
			t.Errorf("test=%d-Logger got=%v", i, logger)
		}
		if fs := opts.getFileSystem(); !matchFileSystem(fs, test.fsBuffer) {
			t.Errorf("test=%d-FileSystem got=%v", i, fs)
		}
	}
}

func TestConvertOptions(t *testing.T) {
	for i, test := range optionsTests {
		opts := convertOptions(test.options)
		if icmp := opts.Comparator; icmp.UserKeyComparator != test.comparator {
			t.Errorf("test=%d-Comparator got=%#v want=%v", i, icmp.UserKeyComparator, test.comparator)
		}
		if compression := opts.Compression; compression != test.compression {
			t.Errorf("test=%d-Compression got=%v want=%v", i, compression, test.compression)
		}
		if blockSize := opts.BlockSize; blockSize != test.blockSize {
			t.Errorf("test=%d-BlockSize got=%d want=%v", i, blockSize, test.blockSize)
		}
		if blockRestartInterval := opts.BlockRestartInterval; blockRestartInterval != test.blockRestartInterval {
			t.Errorf("test=%d-BlockRestartInterval got=%d want=%v", i, blockRestartInterval, test.blockRestartInterval)
		}
		if blockCompressionRatio := opts.BlockCompressionRatio; blockCompressionRatio != test.blockCompressionRatio {
			t.Errorf("test=%d-BlockCompressionRatio got=%v want=%v", i, blockCompressionRatio, test.blockCompressionRatio)
		}
		if writeBufferSize := opts.WriteBufferSize; writeBufferSize != test.writeBufferSize {
			t.Errorf("test=%d-WriteBufferSize got=%d want=%v", i, writeBufferSize, test.writeBufferSize)
		}
		if maxOpenFiles := opts.MaxOpenFiles; maxOpenFiles != test.maxOpenFiles {
			t.Errorf("test=%d-MaxOpenFiles got=%d want=%v", i, maxOpenFiles, test.maxOpenFiles)
		}
		if blockCacheCapacity := opts.BlockCacheCapacity; blockCacheCapacity != test.blockCacheCapacity {
			t.Errorf("test=%d-BlockCacheCapacity got=%d want=%v", i, blockCacheCapacity, test.blockCacheCapacity)
		}
		if compactionConcurrency := opts.CompactionConcurrency; compactionConcurrency != test.compactionConcurrency {
			t.Errorf("test=%d-CompactionConcurrency got=%d want=%v", i, compactionConcurrency, test.compactionConcurrency)
		}
		if compactionBytesPerSeek := opts.CompactionBytesPerSeek; compactionBytesPerSeek != test.compactionBytesPerSeek {
			t.Errorf("test=%d-CompactionBytesPerSeek got=%d want=%v", i, compactionBytesPerSeek, test.compactionBytesPerSeek)
		}
		if minimalAllowedOverlapSeeks := opts.MinimalAllowedOverlapSeeks; minimalAllowedOverlapSeeks != test.minimalAllowedOverlapSeeks {
			t.Errorf("test=%d-MinimalAllowedOverlapSeeks got=%d want=%v", i, minimalAllowedOverlapSeeks, test.minimalAllowedOverlapSeeks)
		}
		if iterationBytesPerSampleSeek := opts.IterationBytesPerSampleSeek; iterationBytesPerSampleSeek != test.iterationBytesPerSampleSeek {
			t.Errorf("test=%d-IterationBytesPerSampleSeek got=%d want=%v", i, iterationBytesPerSampleSeek, test.iterationBytesPerSampleSeek)
		}
		if level0CompactionFiles := opts.Level0CompactionFiles; level0CompactionFiles != test.level0CompactionFiles {
			t.Errorf("test=%d-Level0CompactionFiles got=%d want=%d", i, level0CompactionFiles, test.level0CompactionFiles)
		}
		if level0SlowdownWriteFiles := opts.Level0SlowdownWriteFiles; level0SlowdownWriteFiles != test.level0SlowdownWriteFiles {
			t.Errorf("test=%d-Level0SlowdownWriteFiles got=%d want=%d", i, level0SlowdownWriteFiles, test.level0SlowdownWriteFiles)
		}
		if level0StopWriteFiles := opts.Level0StopWriteFiles; level0StopWriteFiles != test.level0StopWriteFiles {
			t.Errorf("test=%d-Level0StopWriteFiles got=%d want=%d", i, level0StopWriteFiles, test.level0StopWriteFiles)
		}
		if filter := opts.Filter; !matchFilter(filter, test.filterBuffer) {
			t.Errorf("test=%d-Filter got=%v", i, filter)
		}
		if logger := opts.Logger; !matchLogger(logger, test.loggerBuffer) {
			t.Errorf("test=%d-Logger got=%v", i, logger)
		}
		if fs := opts.FileSystem; !matchFileSystem(fs, test.fsBuffer) {
			t.Errorf("test=%d-FileSystem got=%v", i, fs)
		}
	}
}

type readOptionsTest struct {
	options *ReadOptions
	want    options.ReadOptions
}

var readOptionsTests = []readOptionsTest{
	{
		want: options.DefaultReadOptions,
	},
	{
		options: &ReadOptions{},
		want:    options.DefaultReadOptions,
	},
	{
		options: &ReadOptions{
			DontFillCache:   true,
			VerifyChecksums: true,
		},
		want: options.ReadOptions{
			DontFillCache:   true,
			VerifyChecksums: true,
		},
	},
}

func TestConvertReadOptions(t *testing.T) {
	for i, test := range readOptionsTests {
		opts := convertReadOptions(test.options)
		if *opts != test.want {
			t.Errorf("test=%d got=%v want=%v", i, *opts, test.want)
		}
	}
}

type writeOptionsTest struct {
	options *WriteOptions
	want    options.WriteOptions
}

var writeOptionsTests = []writeOptionsTest{
	{
		want: options.DefaultWriteOptions,
	},
	{
		options: &WriteOptions{
			Sync: true,
		},
		want: options.WriteOptions{
			Sync: true,
		},
	},
}

func TestConvertWriteOptions(t *testing.T) {
	for i, test := range writeOptionsTests {
		opts := convertWriteOptions(test.options)
		if *opts != test.want {
			t.Errorf("test=%d got=%v want=%v", i, *opts, test.want)
		}
	}
}

func buildFieldMap(typ reflect.Type) map[string]reflect.StructField {
	fields := make(map[string]reflect.StructField)
	for i, n := 0, typ.NumField(); i < n; i++ {
		field := typ.Field(i)
		fields[field.Name] = field
	}
	return fields
}

func testFieldType(t *testing.T, typ reflect.Type, field reflect.StructField, target reflect.Type) {
	if field.Type != target {
		t.Errorf("%s.%s got type %s, want %s", typ, field.Name, field.Type, target)
	}
}

func TestOptionsType(t *testing.T) {
	unmatchedFields := map[string]struct {
		apiType      reflect.Type
		internalType reflect.Type
	}{
		"Comparator": {
			apiType:      reflect.TypeOf((*Comparator)(nil)).Elem(),
			internalType: reflect.TypeOf((*keys.InternalComparator)(nil)),
		},
		"Compression": {
			apiType:      reflect.TypeOf(DefaultCompression),
			internalType: reflect.TypeOf(compress.NoCompression),
		},
		"Filter": {
			apiType:      reflect.TypeOf((*Filter)(nil)).Elem(),
			internalType: reflect.TypeOf((*filter.Filter)(nil)).Elem(),
		},
		"Logger": {
			apiType:      reflect.TypeOf((*Logger)(nil)).Elem(),
			internalType: reflect.TypeOf((*logger.LogCloser)(nil)).Elem(),
		},
		"FileSystem": {
			apiType:      reflect.TypeOf((*FileSystem)(nil)).Elem(),
			internalType: reflect.TypeOf((*file.FileSystem)(nil)).Elem(),
		},
	}
	apiType := reflect.TypeOf(Options{})
	internalType := reflect.TypeOf(options.Options{})
	internalFields := buildFieldMap(internalType)
	for i, n := 0, apiType.NumField(); i < n; i++ {
		field := apiType.Field(i)
		if field.PkgPath != "" {
			t.Errorf("Field %s.%s is unexported", apiType, field.Name)
		}
		internalField, ok := internalFields[field.Name]
		if !ok {
			t.Errorf("%s has field %s but %s does not", apiType, field.Name, internalType)
			continue
		}
		delete(internalFields, field.Name)
		unmatched, ok := unmatchedFields[field.Name]
		if ok {
			testFieldType(t, apiType, field, unmatched.apiType)
			testFieldType(t, internalType, internalField, unmatched.internalType)
			continue
		}
		if field.Type != internalField.Type {
			t.Errorf("%s.%s has type %s, but %s.%s has type %s", apiType, field.Name, field.Type, internalType, internalField.Name, internalField.Type)
		}
	}
	for name, _ := range internalFields {
		t.Errorf("%s has field %s but %s does not", internalType, name, apiType)
	}
}

func testOptionsLayout(t *testing.T, apiType reflect.Type, internalType reflect.Type) {
	n, m := apiType.NumField(), internalType.NumField()
	i := 0
	for ; i < n && i < m; i++ {
		field := apiType.Field(i)
		if field.PkgPath != "" {
			t.Errorf("Field %s.%s is unexported", apiType, field.Name)
		}
		internalField := internalType.Field(i)
		if field.Name != internalField.Name {
			t.Fatalf("Mismatch field name at index %d: %s.%s and %s.%s", i+1, apiType, field.Name, internalType, internalField.Name)
		}
		if field.Type != internalField.Type {
			t.Errorf("Field %s.%s has type %s, but %s.%s got type %s", apiType, field.Name, field.Type, internalType, internalField.Name, internalField.Type)
		}
	}
	for j := i; j < n; j++ {
		field := apiType.Field(j)
		t.Errorf("%s has field %s but %s does not", apiType, field.Name, internalType)
	}
	for j := i; j < m; j++ {
		field := internalType.Field(j)
		t.Errorf("%s has field %s but %s does not", internalType, field.Name, apiType)
	}
}

func TestReadOptionsType(t *testing.T) {
	apiType := reflect.TypeOf(ReadOptions{})
	internalType := reflect.TypeOf(options.ReadOptions{})
	testOptionsLayout(t, apiType, internalType)
}

func TestWriteOptionsType(t *testing.T) {
	apiType := reflect.TypeOf(WriteOptions{})
	internalType := reflect.TypeOf(options.WriteOptions{})
	testOptionsLayout(t, apiType, internalType)
}
