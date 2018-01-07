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
	manifestLog            *log.Writer
	manifestFile           file.File
	manifestNumber         uint64
	manifestVersion        *Version
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

// ManifestFileNumber returns current manifest number, possibly expired
// due to switching to new manifest file.
func (s *State) ManifestFileNumber() uint64 {
	return s.manifestNumber
}

// NewFileNumber returns a new file number and a next file number.
func (s *State) NewFileNumber() (uint64, uint64) {
	next := atomic.AddUint64(&s.nextFileNumber, 1)
	return next - 1, next
}

func (s *State) NextFileNumber() uint64 {
	return atomic.LoadUint64(&s.nextFileNumber)
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
	for v := range s.versions {
		v.addLiveTables(tables)
	}
}

func (s *State) resetCurrentManifest(snapshot *Edit) error {
	if s.manifestNextNumber == 0 {
		s.manifestNextNumber, s.manifestNextFileNumber = s.NewFileNumber()
		snapshot.NextFileNumber = s.manifestNextFileNumber
	}
	manifestNumber := s.manifestNextNumber

	manifestName := files.ManifestFileName(s.dbname, manifestNumber)
	manifestFile, err := s.fs.Open(manifestName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}

	manifestLog := log.NewWriter(manifestFile, 0)
	err = s.writeEdit(manifestLog, manifestFile, snapshot)
	if err != nil {
		manifestFile.Close()
		s.fs.Remove(manifestName)
		return err
	}

	err = files.SetCurrentManifest(s.fs, s.dbname, s.currentName, manifestNumber)
	if err != nil {
		manifestFile.Close()
		s.fs.Remove(manifestName)
		return err
	}

	s.manifestFile.Close()
	s.manifestNextNumber = 0
	s.manifestLog = manifestLog
	s.manifestFile = manifestFile
	s.manifestNumber = manifestNumber
	return nil
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

// Log writes edit to manifest file.
func (s *State) Log(edit *Edit) error {
	s.manifestMu.Lock()
	defer s.manifestMu.Unlock()

	v, err := s.manifestVersion.edit(edit)
	if err != nil {
		panic(fmt.Errorf("%s:\nversion:\n%s\n\nedit:%s\n\n", err, s.manifestVersion, edit))
	}

	if edit.LastSequence < s.manifestLastSequence {
		edit.LastSequence = s.manifestLastSequence
	}
	if edit.LogNumber < s.manifestLogFileNumber {
		edit.LogNumber = s.manifestLogFileNumber
	}
	if edit.NextFileNumber < s.manifestNextFileNumber {
		edit.NextFileNumber = s.manifestNextFileNumber
	}

	switch {
	case s.manifestLog.Offset() >= configs.TargetFileSize:
		var snapshot Edit
		v.snapshot(&snapshot)
		snapshot.ComparatorName = s.options.Comparator.UserKeyComparator.Name()
		snapshot.LogNumber = edit.LogNumber
		snapshot.LastSequence = edit.LastSequence
		snapshot.NextFileNumber = edit.NextFileNumber
		err := s.resetCurrentManifest(&snapshot)
		edit.NextFileNumber = snapshot.NextFileNumber
		if err == nil {
			break
		}
		fallthrough
	default:
		err := s.writeEdit(s.manifestLog, s.manifestFile, edit)
		if err != nil {
			return err
		}
	}
	s.manifestVersion = v
	s.manifestLastSequence = edit.LastSequence
	s.manifestLogFileNumber = edit.LogNumber
	s.manifestNextFileNumber = edit.NextFileNumber
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
		dbname:                dbname,
		currentName:           currentName,
		fs:                    fs,
		options:               opts,
		lastSequence:          edit.LastSequence,
		logFileNumber:         edit.LogNumber,
		nextFileNumber:        edit.NextFileNumber,
		manifestLog:           manifestLog,
		manifestFile:          manifestFile,
		manifestNumber:        manifestNumber,
		manifestVersion:       current,
		manifestLogFileNumber: edit.LogNumber,
		current:               current,
		versions:              map[*Version]struct{}{current: {}},
		scratch:               record,
		tableCache:            cache,
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
		dbname:                dbname,
		currentName:           currentName,
		fs:                    fs,
		options:               opts,
		lastSequence:          builder.LastSequence,
		logFileNumber:         builder.LogNumber,
		nextFileNumber:        builder.NextFileNumber,
		manifestLog:           log.NewWriter(manifestFile, offset),
		manifestFile:          manifestFile,
		manifestNumber:        manifestNumber,
		manifestLogFileNumber: builder.LogNumber,
		manifestVersion:       current,
		current:               current,
		versions:              map[*Version]struct{}{current: {}},
		scratch:               builder.Scratch,
		tableCache:            cache,
	}
	state.MarkFileNumberUsed(state.logFileNumber)

	return state, nil
}
