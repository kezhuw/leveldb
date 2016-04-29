package version

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/log"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/table"
)

type State struct {
	dbname      string
	currentName string
	options     *options.Options
	fs          file.FileSystem

	tableCache *table.Cache

	lastSequence   keys.Sequence
	logFileNumber  uint64
	nextFileNumber uint64

	manifestMu             sync.Mutex
	manifestBig            bool
	manifestLog            *log.Writer
	manifestFile           file.File
	manifestNumber         uint64
	manifestNextNumber     uint64
	manifestLastSequence   keys.Sequence
	manifestLogFileNumber  uint64
	manifestNextFileNumber uint64

	current    *Version
	versions   map[*Version]struct{}
	versionsMu sync.Mutex

	scratch []byte
}

func (s *State) Current() *Version {
	return s.current
}

func (s *State) LogFileNumber() uint64 {
	return s.logFileNumber
}

func (s *State) LastSequence() keys.Sequence {
	return s.lastSequence
}

func (s *State) SetLastSequence(seq keys.Sequence) {
	s.lastSequence = seq
}

func (s *State) NextFileNumber() uint64 {
	return atomic.LoadUint64(&s.nextFileNumber)
}

// ManifestFileNumber returns current manifest number, possibly expired
// due to switching to new manifest file.
func (s *State) ManifestFileNumber() uint64 {
	return s.manifestNumber
}

func (s *State) NewFileNumber() uint64 {
	return atomic.AddUint64(&s.nextFileNumber, 1) - 1
}

func (s *State) ReuseFileNumber(number uint64) {
	atomic.CompareAndSwapUint64(&s.nextFileNumber, number+1, number)
}

func (s *State) MarkFileNumberUsed(number uint64) {
	if s.nextFileNumber <= number {
		s.nextFileNumber = number + 1
	}
}

func (s *State) AddLiveFiles(files map[uint64]struct{}) {
	s.AddLiveTables(files)
}

func (s *State) AddLiveTables(tables map[uint64]struct{}) {
	s.versionsMu.Lock()
	defer s.versionsMu.Unlock()
	for v, _ := range s.versions {
		v.addLiveTables(tables)
	}
}

func (s *State) tryResetCurrentManifest() {
	if !s.manifestBig {
		return
	}
	manifestNumber := s.manifestNextNumber
	if manifestNumber == 0 {
		manifestNumber = s.NewFileNumber()
		s.manifestNextNumber = manifestNumber
	}
	manifestName := files.ManifestFileName(s.dbname, manifestNumber)
	manifestFile, err := s.fs.Open(manifestName, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_TRUNC)
	if err != nil {
		return
	}
	manifestLog := log.NewWriter(manifestFile, 0)
	err = s.writeSnapshot(manifestLog, manifestFile)
	if err != nil {
		manifestFile.Close()
		s.fs.Remove(manifestName)
		return
	}
	err = files.SetCurrentManifest(s.fs, s.dbname, s.currentName, manifestNumber)
	if err != nil {
		manifestFile.Close()
		s.fs.Remove(manifestName)
		return
	}
	s.manifestFile.Close()
	s.manifestBig = false
	s.manifestNextNumber = 0
	s.manifestLog = manifestLog
	s.manifestFile = manifestFile
	s.manifestNumber = manifestNumber
}

func (s *State) writeSnapshot(log *log.Writer, file file.File) error {
	var edit Edit
	edit.ComparatorName = s.options.Comparator.UserKeyComparator.Name()
	edit.LogNumber = s.logFileNumber
	edit.NextFileNumber = s.nextFileNumber
	edit.LastSequence = s.lastSequence
	v := s.current
	for level := 0; level < configs.NumberLevels; level++ {
		if len(v.CompactionPointers[level]) != 0 {
			edit.CompactPointers = append(edit.CompactPointers, LevelCompactPointer{Level: level, Largest: v.CompactionPointers[level]})
		}
		for _, f := range v.Levels[level] {
			edit.AddedFiles = append(edit.AddedFiles, LevelFileMeta{Level: level, FileMeta: f})
		}
	}
	return s.writeEdit(log, file, &edit)
}

func (s *State) writeEdit(log *log.Writer, file file.File, edit *Edit) error {
	s.scratch = edit.Encode(s.scratch[:0])
	if err := log.Write(s.scratch); err != nil {
		return err
	}
	return file.Sync()
}

func (s *State) ReleaseVersion(v *Version) {
	if atomic.AddInt64(&v.refs, -1) == 0 {
		s.versionsMu.Lock()
		delete(s.versions, v)
		s.versionsMu.Unlock()
	}
}

func (s *State) RetainCurrent() *Version {
	v := s.current
	atomic.AddInt64(&v.refs, 1)
	return v
}

func (s *State) installCurrent(v *Version) {
	v.refs = 1
	s.versionsMu.Lock()
	s.versions[v] = struct{}{}
	s.versionsMu.Unlock()
	s.current, v = v, s.current
	s.ReleaseVersion(v)
}

func (s *State) Log(edit *Edit) error {
	s.manifestMu.Lock()
	defer s.manifestMu.Unlock()
	s.tryResetCurrentManifest()
	if edit.LastSequence < s.manifestLastSequence {
		edit.LastSequence = s.manifestLastSequence
	}
	if edit.LogNumber < s.manifestLogFileNumber {
		edit.LogNumber = s.manifestLogFileNumber
	}
	if edit.NextFileNumber < s.manifestNextFileNumber {
		edit.NextFileNumber = s.manifestNextFileNumber
	}
	err := s.writeEdit(s.manifestLog, s.manifestFile, edit)
	if err == nil {
		s.manifestLastSequence = edit.LastSequence
		s.manifestLogFileNumber = edit.LogNumber
		s.manifestNextFileNumber = edit.NextFileNumber
	}
	return err
}

func (s *State) Apply(edit *Edit) {
	v, err := s.current.edit(edit)
	if err != nil {
		panic(fmt.Errorf("%s:\nversion:\n%s\n\nedit:%s\n\n", err, s.current, edit))
	}
	if edit.LogNumber != 0 && edit.LogNumber > s.logFileNumber {
		s.logFileNumber = edit.LogNumber
	}
	s.installCurrent(v)
}

func (s *State) PickCompaction() *Compaction {
	return s.current.pickCompaction()
}

func Create(dbname string, opts *options.Options) (state *State, err error) {
	fs := opts.FileSystem

	filenames, err := fs.List(dbname)
	if err != nil {
		return nil, err
	}
	for _, filename := range filenames {
		kind, _ := files.Parse(filename)
		if kind == files.Invalid || kind == files.Lock || kind == files.Temp || kind == files.InfoLog {
			continue
		}
		return nil, fmt.Errorf("leveldb: %s file exists: %s", kind, filename)
	}

	const manifestNumber = 1
	manifestName := files.ManifestFileName(dbname, manifestNumber)
	manifestFile, err := fs.Open(manifestName, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			manifestFile.Close()
			fs.Remove(manifestName)
		}
	}()

	manifestLog := log.NewWriter(manifestFile, 0)
	var edit Edit
	edit.ComparatorName = opts.Comparator.UserKeyComparator.Name()
	// Zero value of LogNumber/LastSequence/NextFileNumber are not encoded.
	// Assigned with some non harmful value to circumvent it.
	edit.LastSequence = 1
	edit.LogNumber = 2
	edit.NextFileNumber = 3
	record := edit.Encode(nil)
	if err := manifestLog.Write(record); err != nil {
		return nil, err
	}
	if err := manifestFile.Sync(); err != nil {
		return nil, err
	}

	currentName := files.CurrentFileName(dbname)
	if err := files.SetCurrentManifest(fs, dbname, currentName, 1); err != nil {
		return nil, err
	}

	cache := table.NewCache(dbname, opts)
	current := &Version{refs: 1, icmp: opts.Comparator, cache: cache}
	return &State{
		dbname:         dbname,
		currentName:    currentName,
		fs:             fs,
		options:        opts,
		lastSequence:   edit.LastSequence,
		logFileNumber:  edit.LogNumber,
		nextFileNumber: edit.NextFileNumber,
		manifestLog:    manifestLog,
		manifestFile:   manifestFile,
		manifestNumber: manifestNumber,
		current:        current,
		versions:       map[*Version]struct{}{current: struct{}{}},
		scratch:        record,
		tableCache:     cache,
	}, nil
}

func Recover(dbname string, opts *options.Options) (state *State, err error) {
	fs := opts.FileSystem
	currentName := files.CurrentFileName(dbname)
	manifestName, err := files.GetCurrentManifest(fs, dbname, currentName)
	if err != nil {
		return nil, err
	}

	kind, manifestNumber := files.Parse(manifestName)
	if kind != files.Manifest {
		return nil, errors.NewCorruption(0, "CURRENT", 0, "invalid manifest name")
	}

	manifestFile, err := fs.Open(manifestName, os.O_RDWR)
	if err != nil {
		return nil, err
	}

	var builder builder
	builder.Comparator = opts.Comparator
	builder.ManifestFile = manifestFile
	builder.ManifestNumber = manifestNumber

	current := &Version{refs: 1, icmp: opts.Comparator}
	offset, err := builder.Build(current)
	if err != nil {
		manifestFile.Close()
		return nil, err
	}

	cache := table.NewCache(dbname, opts)
	current.cache = cache
	state = &State{
		dbname:         dbname,
		currentName:    currentName,
		fs:             fs,
		options:        opts,
		lastSequence:   builder.LastSequence,
		logFileNumber:  builder.LogNumber,
		nextFileNumber: builder.NextFileNumber,
		manifestLog:    log.NewWriter(manifestFile, offset),
		manifestFile:   manifestFile,
		manifestNumber: manifestNumber,
		current:        current,
		versions:       map[*Version]struct{}{current: struct{}{}},
		scratch:        builder.Scratch,
		tableCache:     cache,
	}
	state.MarkFileNumberUsed(state.logFileNumber)

	if offset >= 2*1024*1024 {
		state.manifestBig = true
	}

	return state, nil
}
