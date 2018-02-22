package manifest

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kezhuw/leveldb/internal/compaction"
	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/record"
	"github.com/kezhuw/leveldb/internal/table"
)

type Manifest struct {
	dbname      string
	currentName string
	options     *options.Options
	fs          file.FileSystem

	version *Version

	tableCache *table.Cache

	lastSequence   keys.Sequence
	nextFileNumber uint64

	// Update only after successful manifest logging, which means that
	// memtable log files older than logFileNumber and manifest files older
	// than manifestNumber are obsolete and eligible to be deleted.
	logFileNumber  uint64
	manifestNumber uint64

	manifestLog        *record.Writer
	manifestFile       file.File
	manifestNextNumber uint64

	liveFiles   map[uint64]int
	liveFilesMu sync.Mutex

	scratch []byte
}

func (m *Manifest) LogFileNumber() uint64 {
	return atomic.LoadUint64(&m.logFileNumber)
}

func (m *Manifest) LastSequence() keys.Sequence {
	return m.lastSequence
}

func (m *Manifest) LoadLastSequence() keys.Sequence {
	return keys.Sequence(atomic.LoadUint64((*uint64)(&m.lastSequence)))
}

func (m *Manifest) StoreLastSequence(seq keys.Sequence) {
	atomic.StoreUint64((*uint64)(&m.lastSequence), uint64(seq))
}

// ManifestFileNumber returns current manifest number, possibly expired
// due to switching to new manifest file.
func (m *Manifest) ManifestFileNumber() uint64 {
	return atomic.LoadUint64(&m.manifestNumber)
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
	m.liveFilesMu.Lock()
	defer m.liveFilesMu.Unlock()
	for f := range m.liveFiles {
		files[f] = struct{}{}
	}
	return files
}

func (m *Manifest) resetCurrentManifest(snapshot *Edit) error {
	if m.manifestNextNumber == 0 {
		m.manifestNextNumber, snapshot.NextFileNumber = m.NewFileNumber()
	}
	manifestNumber := m.manifestNextNumber

	manifestName := files.ManifestFileName(m.dbname, manifestNumber)
	manifestFile, err := m.fs.Open(manifestName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}

	manifestLog := record.NewWriter(manifestFile, 0)
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
	atomic.StoreUint64(&m.manifestNumber, manifestNumber)
	return nil
}

func (m *Manifest) writeEdit(log *record.Writer, file file.File, edit *Edit) error {
	m.scratch = edit.Encode(m.scratch[:0])
	if err := log.Write(m.scratch); err != nil {
		return err
	}
	return file.Sync()
}

// Log writes edit to manifest file.
func (m *Manifest) Log(tip *Version, edit *Edit) (*Version, error) {
	if v := m.version; v.number > tip.number {
		panic("current version number > tip version number")
	}

	next, err := tip.edit(edit)
	if err != nil {
		panic(fmt.Errorf("leveldb: fail to edit version:\nversion:\n%s\n\nedit:%s", tip, edit))
	}

	edit.LastSequence = m.LoadLastSequence()
	// logFileNumber will only be written here later, so there is no need
	// to do atomic loading here.
	if logNumber := m.logFileNumber; edit.LogNumber < logNumber {
		edit.LogNumber = logNumber
	}
	if nextFileNumber := m.NextFileNumber(); edit.NextFileNumber < nextFileNumber {
		edit.NextFileNumber = nextFileNumber
	}

	switch {
	case m.manifestLog.Offset() >= configs.TargetFileSize:
		var snapshot Edit
		next.snapshot(&snapshot)
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
			return nil, err
		}
	}
	atomic.StoreUint64(&m.logFileNumber, edit.LogNumber)
	return next, nil
}

func (m *Manifest) mountVersion(v *Version) {
	m.liveFilesMu.Lock()
	defer m.liveFilesMu.Unlock()
	v.refFiles(m.liveFiles)
	runtime.SetFinalizer(v, (*Version).finalize)
}

func (m *Manifest) unmountVersion(v *Version) {
	m.liveFilesMu.Lock()
	defer m.liveFilesMu.Unlock()
	v.unrefFiles(m.liveFiles)
}

func (m *Manifest) Version() *Version {
	return (*Version)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&m.version))))
}

func (m *Manifest) Append(tip *Version) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&m.version)), unsafe.Pointer(tip))
	m.mountVersion(tip)
}

func (m *Manifest) Apply(edit *Edit) error {
	next, err := m.Log(m.version, edit)
	if err != nil {
		return err
	}
	m.Append(next)
	return nil
}

func (m *Manifest) PickCompactions(registry *compaction.Registry, pendingFiles []FileList) []*Compaction {
	return m.version.pickCompactions(registry, pendingFiles, m.NextFileNumber())
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

	manifestLog := record.NewWriter(manifestFile, 0)
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

	manifest = &Manifest{
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
		liveFiles:      make(map[uint64]int),
		scratch:        record,
		tableCache:     table.NewCache(dbname, opts),
	}
	version := &Version{options: opts, cache: manifest.tableCache, manifest: manifest}
	manifest.Append(version)
	return manifest, nil
}

func Recover(dbname string, opts *options.Options) (*Manifest, error) {
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

	version := &Version{options: opts}
	offset, err := builder.Build(version)
	if err != nil {
		manifestFile.Close()
		return nil, err
	}

	manifest := &Manifest{
		dbname:         dbname,
		currentName:    currentName,
		fs:             fs,
		options:        opts,
		lastSequence:   builder.LastSequence,
		logFileNumber:  builder.LogNumber,
		nextFileNumber: builder.NextFileNumber,
		manifestLog:    record.NewWriter(manifestFile, offset),
		manifestFile:   manifestFile,
		manifestNumber: manifestNumber,
		liveFiles:      make(map[uint64]int),
		scratch:        builder.Scratch,
		tableCache:     table.NewCache(dbname, opts),
	}
	version.cache = manifest.tableCache
	version.manifest = manifest
	manifest.Append(version)
	manifest.MarkFileNumberUsed(manifest.logFileNumber)

	return manifest, nil
}
