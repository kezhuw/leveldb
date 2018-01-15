package manifest

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

type Manifest struct {
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

func (m *Manifest) Current() *Version {
	return m.current
}

func (m *Manifest) LogFileNumber() uint64 {
	return m.logFileNumber
}

func (m *Manifest) LastSequence() keys.Sequence {
	return m.lastSequence
}

func (m *Manifest) SetLastSequence(seq keys.Sequence) {
	m.lastSequence = seq
}

// ManifestFileNumber returns current manifest number, possibly expired
// due to switching to new manifest file.
func (m *Manifest) ManifestFileNumber() uint64 {
	return m.manifestNumber
}

// NewFileNumber returns a new file number and a next file number.
func (m *Manifest) NewFileNumber() (uint64, uint64) {
	next := atomic.AddUint64(&m.nextFileNumber, 1)
	return next - 1, next
}

func (m *Manifest) NextFileNumber() uint64 {
	return atomic.LoadUint64(&m.nextFileNumber)
}

func (m *Manifest) ReuseFileNumber(number uint64) {
	atomic.CompareAndSwapUint64(&m.nextFileNumber, number+1, number)
}

func (m *Manifest) MarkFileNumberUsed(number uint64) {
	if m.nextFileNumber <= number {
		m.nextFileNumber = number + 1
	}
}

func (m *Manifest) AddLiveFiles(files map[uint64]struct{}) map[uint64]struct{} {
	m.AddLiveTables(files)
	return files
}

func (m *Manifest) AddLiveTables(tables map[uint64]struct{}) {
	m.versionsMu.Lock()
	defer m.versionsMu.Unlock()
	for v := range m.versions {
		v.addLiveTables(tables)
	}
}

func (m *Manifest) resetCurrentManifest(snapshot *Edit) error {
	if m.manifestNextNumber == 0 {
		m.manifestNextNumber, m.manifestNextFileNumber = m.NewFileNumber()
		snapshot.NextFileNumber = m.manifestNextFileNumber
	}
	manifestNumber := m.manifestNextNumber

	manifestName := files.ManifestFileName(m.dbname, manifestNumber)
	manifestFile, err := m.fs.Open(manifestName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}

	manifestLog := log.NewWriter(manifestFile, 0)
	err = m.writeEdit(manifestLog, manifestFile, snapshot)
	if err != nil {
		manifestFile.Close()
		m.fs.Remove(manifestName)
		return err
	}

	err = files.SetCurrentManifest(m.fs, m.dbname, m.currentName, manifestNumber)
	if err != nil {
		manifestFile.Close()
		m.fs.Remove(manifestName)
		return err
	}

	m.manifestFile.Close()
	m.manifestNextNumber = 0
	m.manifestLog = manifestLog
	m.manifestFile = manifestFile
	m.manifestNumber = manifestNumber
	return nil
}

func (m *Manifest) writeEdit(log *log.Writer, file file.File, edit *Edit) error {
	m.scratch = edit.Encode(m.scratch[:0])
	if err := log.Write(m.scratch); err != nil {
		return err
	}
	return file.Sync()
}

func (m *Manifest) ReleaseVersion(v *Version) {
	if atomic.AddInt64(&v.refs, -1) == 0 {
		m.versionsMu.Lock()
		delete(m.versions, v)
		m.versionsMu.Unlock()
	}
}

func (m *Manifest) RetainCurrent() *Version {
	v := m.current
	atomic.AddInt64(&v.refs, 1)
	return v
}

func (m *Manifest) installCurrent(v *Version) {
	v.refs = 1
	m.versionsMu.Lock()
	m.versions[v] = struct{}{}
	m.versionsMu.Unlock()
	m.current, v = v, m.current
	m.ReleaseVersion(v)
}

// Log writes edit to manifest file.
func (m *Manifest) Log(edit *Edit) error {
	m.manifestMu.Lock()
	defer m.manifestMu.Unlock()

	v, err := m.manifestVersion.edit(edit)
	if err != nil {
		panic(fmt.Errorf("%s:\nversion:\n%s\n\nedit:%s\n\n", err, m.manifestVersion, edit))
	}

	if edit.LastSequence < m.manifestLastSequence {
		edit.LastSequence = m.manifestLastSequence
	}
	if edit.LogNumber < m.manifestLogFileNumber {
		edit.LogNumber = m.manifestLogFileNumber
	}
	if edit.NextFileNumber < m.manifestNextFileNumber {
		edit.NextFileNumber = m.manifestNextFileNumber
	}

	switch {
	case m.manifestLog.Offset() >= configs.TargetFileSize:
		var snapshot Edit
		v.snapshot(&snapshot)
		snapshot.ComparatorName = m.options.Comparator.UserKeyComparator.Name()
		snapshot.LogNumber = edit.LogNumber
		snapshot.LastSequence = edit.LastSequence
		snapshot.NextFileNumber = edit.NextFileNumber
		err := m.resetCurrentManifest(&snapshot)
		edit.NextFileNumber = snapshot.NextFileNumber
		if err == nil {
			break
		}
		fallthrough
	default:
		err := m.writeEdit(m.manifestLog, m.manifestFile, edit)
		if err != nil {
			return err
		}
	}
	m.manifestVersion = v
	m.manifestLastSequence = edit.LastSequence
	m.manifestLogFileNumber = edit.LogNumber
	m.manifestNextFileNumber = edit.NextFileNumber
	return err
}

func (m *Manifest) Apply(edit *Edit) {
	v, err := m.current.edit(edit)
	if err != nil {
		panic(fmt.Errorf("%s:\nversion:\n%s\n\nedit:%s\n\n", err, m.current, edit))
	}
	if edit.LogNumber != 0 && edit.LogNumber > m.logFileNumber {
		m.logFileNumber = edit.LogNumber
	}
	m.installCurrent(v)
}

func (m *Manifest) PickCompaction() *Compaction {
	return m.current.pickCompaction()
}

func Create(dbname string, opts *options.Options) (manifest *Manifest, err error) {
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
	return &Manifest{
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

func Recover(dbname string, opts *options.Options) (manifest *Manifest, err error) {
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
	manifest = &Manifest{
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
	manifest.MarkFileNumberUsed(manifest.logFileNumber)

	return manifest, nil
}
