package leveldb

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/kezhuw/leveldb/internal/batch"
	"github.com/kezhuw/leveldb/internal/compactor"
	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/logger"
	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/memtable"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/record"
	"github.com/kezhuw/leveldb/internal/request"
)

type DB struct {
	name string

	requestc chan request.Request
	requests chan request.Request
	requestw chan struct{}

	bundle *bundle

	manifest *manifest.Manifest

	closing uintptr
	closed  chan struct{}

	bgClosing chan struct{}
	bgGroup   sync.WaitGroup

	fs      file.FileSystem
	options *options.Options
	locker  io.Closer

	log       *record.Writer
	logFile   file.File
	logNumber uint64

	// logErr and manifestErr are unrecoverable errors generated from
	// writing to memtable log and manifest log.
	logErr      error
	manifestErr error

	compactionErr error

	manifestErrChan   chan error
	compactionErrChan chan error

	nextLogFile    chan file.File
	nextLogNumber  uint64
	nextLogFileErr chan error

	snapshots   snapshotList
	snapshotsMu sync.Mutex

	compactionFile        chan manifest.LevelFileMeta
	compactionRequestChan chan *compactionRequest

	memtableEdit     chan compactionEdit
	compactionEdit   chan compactionEdit
	compactionResult chan compactionResult

	obsoleteFilesChan chan uint64
}

func Open(dbname string, opts *options.Options) (db *DB, err error) {
	fs := opts.FileSystem
	fs.MkdirAll(dbname)

	locker, err := fs.Lock(files.LockFileName(dbname))
	if err != nil {
		return nil, err
	}

	if opts.Logger == nil {
		infoLogName := files.InfoLogFileName(dbname)
		fs.Rename(infoLogName, files.OldInfoLogFileName(dbname))
		f, err := fs.Open(infoLogName, os.O_WRONLY|os.O_APPEND|os.O_CREATE)
		switch err {
		case nil:
			opts.Logger = logger.FileLogger(f)
		default:
			opts.Logger = logger.Discard
		}
	}

	defer func() {
		if err != nil {
			locker.Close()
			opts.Logger.Close()
		}
	}()

	current := files.CurrentFileName(dbname)
	switch fs.Exists(current) {
	case false:
		if !opts.CreateIfMissing {
			return nil, errors.ErrDBMissing
		}
		return createDB(dbname, locker, opts)
	default:
		if opts.ErrorIfExists {
			return nil, errors.ErrDBExists
		}
		return recoverDB(dbname, locker, opts)
	}
}

func (db *DB) newLogFile() (file.File, uint64, error) {
	logNumber, _ := db.manifest.NewFileNumber()
	logName := files.LogFileName(db.name, logNumber)
	logFile, err := db.fs.Open(logName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		db.manifest.ReuseFileNumber(logNumber)
		return nil, 0, err
	}
	return logFile, logNumber, nil
}

func (db *DB) loadLog(mem *memtable.MemTable, logNumber uint64, flag int, maxSequence *keys.Sequence) (file.File, int64, error) {
	logName := files.LogFileName(db.name, logNumber)
	logFile, err := db.fs.Open(logName, flag)
	if err != nil {
		return nil, 0, err
	}
	r := record.NewReader(logFile)
	var batch batch.Batch
	var buf []byte
	for {
		buf, err = r.AppendRecord(buf[:0])
		switch err {
		case nil:
		case io.EOF:
			return logFile, r.Offset(), nil
		case record.ErrIncompleteRecord:
			offset := r.Offset()
			logFile.Truncate(offset)
			_, err = logFile.Seek(offset, io.SeekStart)
			return logFile, offset, err
		default:
			logFile.Close()
			return nil, 0, err
		}
		batch.Reset(buf)
		err = batch.Iterate(mem)
		if err != nil {
			logFile.Close()
			return nil, 0, err
		}
		if lastSequence := batch.Sequence().Next(uint64(batch.Count()) - 1); lastSequence > *maxSequence {
			*maxSequence = lastSequence
		}
	}
}

type byOldestFileNumber []uint64

func (files byOldestFileNumber) Len() int {
	return len(files)
}

func (files byOldestFileNumber) Less(i, j int) bool {
	return files[i] < files[j]
}

func (files byOldestFileNumber) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}

func (db *DB) recoverLogs(logs []uint64) error {
	n := len(logs)
	if n == 0 {
		logFile, logNumber, err := db.newLogFile()
		if err != nil {
			return err
		}
		db.bundle.mem = memtable.New(db.options.Comparator)
		db.openLog(logFile, 0, logNumber)
		return nil
	}
	maxSequence := db.manifest.LastSequence()
	if n != 1 {
		sort.Sort(byOldestFileNumber(logs))
		var edit manifest.Edit
		for _, logNumber := range logs[:n-1] {
			mem := memtable.New(db.options.Comparator)
			logFile, _, err := db.loadLog(mem, logNumber, os.O_RDONLY, &maxSequence)
			if err != nil {
				return err
			}
			logFile.Close()
			fileNumber, _ := db.manifest.NewFileNumber()
			fileName := files.TableFileName(db.name, fileNumber)
			file, err := compactor.CompactMemTable(fileNumber, fileName, keys.MaxSequence, mem, db.options)
			if err != nil {
				return err
			}
			if file != nil {
				edit.AddedFiles = append(edit.AddedFiles[:0], manifest.LevelFileMeta{Level: 0, FileMeta: file})
			}
		}
		if len(edit.AddedFiles) != 0 {
			edit.LogNumber = logs[n-1]
			edit.NextFileNumber = db.manifest.NextFileNumber()
			edit.LastSequence = maxSequence
			err := db.manifest.Apply(&edit)
			if err != nil {
				return err
			}
		}
	}
	mem := memtable.New(db.options.Comparator)
	logNumber := logs[n-1]
	logFile, offset, err := db.loadLog(mem, logNumber, os.O_RDWR, &maxSequence)
	if err != nil {
		return err
	}
	db.bundle.mem = mem
	db.openLog(logFile, offset, logNumber)
	db.manifest.StoreLastSequence(maxSequence)
	db.manifest.MarkFileNumberUsed(logNumber)
	return nil
}

func (db *DB) NewSnapshot() *Snapshot {
	ss := &Snapshot{db: db, refs: 1}
	db.snapshotsMu.Lock()
	ss.seq = db.manifest.LoadLastSequence()
	db.snapshots.PushBack(ss)
	db.snapshotsMu.Unlock()
	return ss
}

func (db *DB) releaseSnapshot(ss *Snapshot) {
	db.snapshotsMu.Lock()
	db.snapshots.Remove(ss)
	db.snapshotsMu.Unlock()
}

func (db *DB) getSmallestSnapshot() keys.Sequence {
	db.snapshotsMu.Lock()
	defer db.snapshotsMu.Unlock()
	if db.snapshots.Empty() {
		return db.manifest.LoadLastSequence()
	}
	return db.snapshots.Oldest()
}

func (db *DB) Put(key, value []byte, opts *options.WriteOptions) error {
	var batch batch.Batch
	batch.Put(key, value)
	return db.Write(batch, opts)
}

func (db *DB) Delete(key []byte, opts *options.WriteOptions) error {
	var batch batch.Batch
	batch.Delete(key)
	return db.Write(batch, opts)
}

func (db *DB) Write(b batch.Batch, opts *options.WriteOptions) error {
	replyc := make(chan error, 1)
	db.requestc <- request.Request{Sync: opts.Sync, Batch: b, Reply: replyc}
	return <-replyc
}

func (db *DB) Close() error {
	if !atomic.CompareAndSwapUintptr(&db.closing, 0, 1) {
		<-db.closed
		return nil
	}
	defer close(db.closed)

	close(db.bgClosing)
	db.bgGroup.Wait()
	db.storeBundle(nil)
	if db.locker != nil {
		db.locker.Close()
		db.locker = nil
	}
	db.closeLog(nil)
	db.options.Logger.Close()
	close(db.compactionRequestChan)
	return nil
}

func (db *DB) Get(key []byte, opts *options.ReadOptions) ([]byte, error) {
	return db.get(key, db.manifest.LoadLastSequence(), opts)
}

func (db *DB) get(key []byte, seq keys.Sequence, opts *options.ReadOptions) ([]byte, error) {
	bundle := db.loadBundle()
	if bundle == nil {
		return nil, errors.ErrDBClosed
	}
	ikey := keys.NewInternalKey(key, seq, keys.Seek)
	memtables := [2]*memtable.MemTable{bundle.mem, bundle.imm}
	for _, mem := range memtables {
		if mem == nil {
			continue
		}
		value, err, ok := mem.Get(ikey)
		if ok {
			return value, err
		}
	}
	value, seekThroughFile, err := bundle.version.Get(ikey, opts)
	if seekThroughFile.FileMeta != nil {
		db.tryCompactFile(seekThroughFile)
	}
	return value, err
}

func (db *DB) All(opts *options.ReadOptions) iterator.Iterator {
	return db.between(nil, nil, db.manifest.LoadLastSequence(), opts)
}

func (db *DB) Find(start []byte, opts *options.ReadOptions) iterator.Iterator {
	return db.between(start, nil, db.manifest.LoadLastSequence(), opts)
}

func (db *DB) Range(start, limit []byte, opts *options.ReadOptions) iterator.Iterator {
	return db.between(start, limit, db.manifest.LoadLastSequence(), opts)
}

func (db *DB) Prefix(prefix []byte, opts *options.ReadOptions) iterator.Iterator {
	return db.prefix(prefix, db.manifest.LoadLastSequence(), opts)
}

func (db *DB) prefix(prefix []byte, seq keys.Sequence, opts *options.ReadOptions) iterator.Iterator {
	limit := db.options.Comparator.UserKeyComparator.MakePrefixSuccessor(prefix)
	return db.between(prefix, limit, seq, opts)
}

func (db *DB) between(start, limit []byte, seq keys.Sequence, opts *options.ReadOptions) iterator.Iterator {
	bundle := db.loadBundle()
	if bundle == nil {
		return iterator.Error(errors.ErrDBClosed)
	}
	iters := make([]iterator.Iterator, 1, 16)
	iters[0] = bundle.mem.NewIterator()
	if bundle.imm != nil {
		iters = append(iters, bundle.imm.NewIterator())
	}
	iters = bundle.version.AppendIterators(iters, opts)
	mergeIt := iterator.NewMergeIterator(db.options.Comparator, iters...)
	dbIt := newDBIterator(db, bundle.version, seq, mergeIt)
	ucmp := db.options.Comparator.UserKeyComparator
	return iterator.NewRangeIterator(start, limit, ucmp, dbIt)
}

func (db *DB) finalize() {
	close(db.requestc)
}

func initDB(db *DB, name string, m *manifest.Manifest, locker io.Closer, opts *options.Options) {
	db.name = name
	db.manifest = m
	db.locker = locker
	db.fs = opts.FileSystem
	db.options = opts
	db.closed = make(chan struct{})
	db.bgClosing = make(chan struct{})
	db.bundle = &bundle{version: m.Version()}
	db.requestc = make(chan request.Request, 1024)
	db.requests = make(chan request.Request)
	db.requestw = make(chan struct{}, 1)
	db.nextLogFile = make(chan file.File, 1)
	db.nextLogFileErr = make(chan error, 1)
	db.manifestErrChan = make(chan error, 1)
	db.compactionErrChan = make(chan error, 1)
	db.memtableEdit = make(chan compactionEdit, 1)
	db.compactionEdit = make(chan compactionEdit, configs.NumberLevels)
	db.compactionResult = make(chan compactionResult, configs.NumberLevels)
	db.compactionFile = make(chan manifest.LevelFileMeta, 128)
	db.compactionRequestChan = make(chan *compactionRequest, 10)
	db.obsoleteFilesChan = make(chan uint64, configs.NumberLevels)
	db.snapshots.Init()
	runtime.SetFinalizer(db, (*DB).finalize)
}

func createDB(dbname string, locker io.Closer, opts *options.Options) (*DB, error) {
	manifest, err := manifest.Create(dbname, opts)
	if err != nil {
		return nil, err
	}
	logNumber, _ := manifest.NewFileNumber()
	logName := files.LogFileName(dbname, logNumber)
	logFile, err := opts.FileSystem.Open(logName, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return nil, fmt.Errorf("leveldb: fail to create log file: %s", err)
	}
	db := &DB{}
	initDB(db, dbname, manifest, locker, opts)
	db.bundle.mem = memtable.New(opts.Comparator)
	db.openLog(logFile, 0, logNumber)
	db.bgGroup.Add(1)
	go db.serveWrite()
	return db, nil
}

func recoverDB(dbname string, locker io.Closer, opts *options.Options) (db *DB, err error) {
	manifest, err := manifest.Recover(dbname, opts)
	if err != nil {
		return nil, err
	}
	filenames, err := opts.FileSystem.List(dbname)
	if err != nil {
		return nil, err
	}
	logNumber := manifest.LogFileNumber()
	var logs []uint64
	tables := manifest.AddLiveFiles(make(map[uint64]struct{}))
	for _, filename := range filenames {
		kind, number := files.Parse(filename)
		switch kind {
		case files.Table:
			delete(tables, number)
		case files.Log:
			if number >= logNumber {
				logs = append(logs, number)
			}
		}
	}
	if len(tables) != 0 {
		return nil, fmt.Errorf("leveldb: missing tables: %v", tables)
	}
	db = &DB{}
	initDB(db, dbname, manifest, locker, opts)
	if err := db.recoverLogs(logs); err != nil {
		db.closeLog(nil)
		return nil, err
	}
	db.bgGroup.Add(1)
	go db.serveWrite()
	return db, nil
}
