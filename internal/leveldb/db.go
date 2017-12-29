package leveldb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/kezhuw/leveldb/internal/batch"
	"github.com/kezhuw/leveldb/internal/compact"
	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/log"
	"github.com/kezhuw/leveldb/internal/logger"
	"github.com/kezhuw/leveldb/internal/memtable"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/version"
)

type DB struct {
	name string

	requestc chan batch.Request
	requests chan batch.Request

	mu    sync.RWMutex
	mem   *memtable.MemTable
	imm   *memtable.MemTable
	state *version.State

	closing bool
	closed  chan struct{}

	bgClosing chan struct{}
	bgGroup   sync.WaitGroup

	fs      file.FileSystem
	options *options.Options
	locker  io.Closer

	log       *log.Writer
	logErr    error
	logFile   file.File
	logNumber uint64

	nextLogFile   chan file.File
	nextLogNumber uint64

	// background jobs:
	// * level compaction
	// * memory compaction
	// * obsolete files collection
	//
	// File collection can't be run concurrently with compactions.
	// Level and memory compactions can run concurrently with each other.
	collectionFiles  bool
	compactionLevel  int
	compactionMemory bool

	collectionDone chan struct{}

	compactionErr     error
	compactionResultc chan compactionResult

	snapshots   snapshotList
	snapshotsMu sync.Mutex
}

type compactionResult struct {
	level   int
	err     error
	edit    *version.Edit
	aborted bool
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
	logNumber, _ := db.state.NewFileNumber()
	logName := files.LogFileName(db.name, logNumber)
	logFile, err := db.fs.Open(logName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		db.state.ReuseFileNumber(logNumber)
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
	r := log.NewReader(logFile)
	var ok bool
	var seq keys.Sequence
	var items []batch.Item
	var batch batch.Batch
	var record []byte
	for {
		record, err = r.AppendRecord(record[:0])
		switch err {
		case nil:
		case io.EOF:
			return logFile, r.Offset(), nil
		case log.ErrIncompleteRecord:
			offset := r.Offset()
			logFile.Truncate(offset)
			_, err = logFile.Seek(offset, 0)
			return logFile, offset, err
		default:
			logFile.Close()
			return nil, 0, err
		}
		batch.Reset(record)
		seq, items, ok = batch.Split(items)
		if !ok || len(items) == 0 {
			logFile.Close()
			return nil, 0, errors.ErrCorruptWriteBatch
		}
		mem.Batch(seq, items)
		if lastSequence := seq.Next(uint64(len(items) - 1)); lastSequence > *maxSequence {
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
		db.mem = memtable.New(db.options.Comparator)
		db.log = log.NewWriter(logFile, 0)
		db.logFile = logFile
		db.logNumber = logNumber
		return nil
	}
	var imm *memtable.MemTable
	maxSequence := db.state.LastSequence()
	if n != 1 {
		sort.Sort(byOldestFileNumber(logs))
		imm = memtable.New(db.options.Comparator)
		for _, logNumber := range logs[:n-1] {
			logFile, _, err := db.loadLog(imm, logNumber, os.O_RDONLY, &maxSequence)
			if err != nil {
				return err
			}
			logFile.Close()
		}
	}
	mem := memtable.New(db.options.Comparator)
	logNumber := logs[n-1]
	logFile, offset, err := db.loadLog(mem, logNumber, os.O_RDWR, &maxSequence)
	if err != nil {
		return err
	}
	db.mem = mem
	db.imm = imm
	db.log = log.NewWriter(logFile, offset)
	db.logFile = logFile
	db.logNumber = logNumber
	db.state.SetLastSequence(maxSequence)
	db.state.MarkFileNumberUsed(logNumber)
	return nil
}

func (db *DB) removeTableFiles(numbers []uint64) {
	for _, tableNumber := range numbers {
		db.fs.Remove(files.TableFileName(db.name, tableNumber))
	}
}

func (db *DB) compactAndLog(c compact.Compactor, edit *version.Edit) {
	level := c.Level()
	compacted := false
	var err error
	var timeout time.Duration
	for {
		switch compacted {
		case false:
			err = c.Compact(edit)
			if err != nil {
				db.options.Logger.Warnf("level %d compaction: fail to compact: %s", level, err)
				break
			}
			compacted = true
			timeout = 0
			fallthrough
		default:
			err = db.state.Log(edit)
			if err == nil {
				db.compactionResultc <- compactionResult{level: level, edit: edit}
				return
			}
			db.options.Logger.Warnf("level %d compaction: fail to log: %s", level, err)
		}
		db.compactionResultc <- compactionResult{level: level, err: err}
		timeout += timeout/2 + time.Second
		select {
		case <-db.bgClosing:
			db.removeTableFiles(c.FileNumbers())
			db.compactionResultc <- compactionResult{level: level, err: err, aborted: true}
			return
		case <-time.After(timeout):
		}
	}
}

func (db *DB) applyCompaction(level int, edit *version.Edit) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.state.Apply(edit)
	if level == -1 {
		db.imm = nil
	}
}

func (db *DB) completeCompaction(level int, err error, edit *version.Edit, aborted bool) {
	if err == nil || aborted {
		defer db.bgGroup.Done()
		switch level {
		case -1:
			db.compactionMemory = false
		default:
			db.compactionLevel = -1
		}
	}
	db.compactionErr = err
	if err != nil {
		return
	}
	db.applyCompaction(level, edit)
	db.tryLevelCompaction()
	db.tryRemoveObsoleteFiles()
}

func (db *DB) completeCollectionFiles() {
	defer db.bgGroup.Done()
	db.collectionDone = nil
	db.collectionFiles = false
	db.tryMemoryCompaction()
	db.tryLevelCompaction()
}

// removeObsoleteFiles removes obsolete files in database directory. If done is not nil,
// it will be closed after done.
func (db *DB) removeObsoleteFiles(logNumber, manifestNumber uint64, done chan struct{}) {
	if done != nil {
		defer close(done)
	}
	lives := make(map[uint64]struct{})
	db.state.AddLiveFiles(lives)
	filenames, _ := db.fs.List(db.name)
	for _, name := range filenames {
		kind, number := files.Parse(name)
		switch kind {
		case files.Invalid, files.Lock, files.Current, files.InfoLog, files.Temp:
			continue
		case files.Log:
			if number >= logNumber {
				continue
			}
		case files.Table, files.SSTTable:
			if _, ok := lives[number]; ok {
				continue
			}
		case files.Manifest:
			if number >= manifestNumber {
				continue
			}
		}
		db.fs.Remove(filepath.Join(db.name, name))
	}
}

func (db *DB) tryStartBackground() {
	db.tryMemoryCompaction()
	db.tryLevelCompaction()
	db.tryRemoveObsoleteFiles()
}

func (db *DB) tryRemoveObsoleteFiles() {
	if db.compactionLevel >= 0 || db.compactionMemory {
		return
	}
	db.collectionDone = make(chan struct{})
	db.collectionFiles = true
	db.bgGroup.Add(1)
	go db.removeObsoleteFiles(db.state.LogFileNumber(), db.state.ManifestFileNumber(), db.collectionDone)
}

func (db *DB) tryLevelCompaction() {
	if db.collectionFiles || db.compactionLevel >= 0 || db.closing {
		return
	}
	compaction := db.state.PickCompaction()
	if compaction == nil {
		return
	}
	db.compactionLevel = compaction.Level
	var edit version.Edit
	edit.LogNumber = db.logNumber
	edit.LastSequence = db.state.LastSequence()
	edit.NextFileNumber = db.state.NextFileNumber()
	c := compact.NewLevelCompaction(db.name, db.getSmallestSnapshot(), compaction, db.state, db.options)
	db.bgGroup.Add(1)
	go db.compactAndLog(c, &edit)
}

func (db *DB) tryMemoryCompaction() {
	if db.imm == nil || db.collectionFiles || db.closing {
		return
	}
	db.compactionMemory = true
	s := db.state
	fileNumber, nextFileNumber := s.NewFileNumber()
	c := compact.NewMemTableCompaction(db.name, db.getSmallestSnapshot(), fileNumber, db.compactionLevel, db.imm, s.Current(), db.options)
	var edit version.Edit
	edit.LogNumber = db.logNumber
	edit.NextFileNumber = nextFileNumber
	edit.LastSequence = s.LastSequence()
	db.bgGroup.Add(1)
	go db.compactAndLog(c, &edit)
}

func (db *DB) tryOpenNextLog() {
	if db.imm == nil && db.nextLogNumber == 0 {
		db.nextLogNumber, _ = db.state.NewFileNumber()
		db.bgGroup.Add(1)
		go db.openNextLog()
	}
}

func (db *DB) openNextLog() {
	fileName := files.LogFileName(db.name, db.nextLogNumber)
	var timeout time.Duration
	for {
		f, err := db.fs.Open(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		if err == nil {
			db.nextLogFile <- f
			return
		}
		select {
		case <-db.bgClosing:
			db.nextLogFile <- nil
			return
		case <-time.After(timeout):
		}
	}
}

func (db *DB) NewSnapshot() *Snapshot {
	ss := &Snapshot{db: db, refs: 1}
	db.mu.RLock()
	if db.closing {
		db.mu.RUnlock()
		return nil
	}
	ss.seq = db.state.LastSequence()
	db.mu.RUnlock()
	db.snapshotsMu.Lock()
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
		return db.state.LastSequence()
	}
	return db.snapshots.Oldest()
}

func (db *DB) switchMemTable() {
	if db.mem.Empty() {
		return
	}
	mem := memtable.New(db.options.Comparator)
	db.mu.Lock()
	db.imm = db.mem
	db.mem = mem
	db.mu.Unlock()
	db.tryMemoryCompaction()
}

func (db *DB) openLog(f file.File, number uint64) {
	db.log = log.NewWriter(f, 0)
	db.logErr = nil
	if db.logFile != nil {
		db.logFile.Close()
	}
	db.logFile = f
	db.logNumber = number
	db.switchMemTable()
}

func (db *DB) closeLog(err error) {
	db.log = nil
	db.logErr = err
	if db.logFile != nil {
		db.logFile.Close()
		db.logFile = nil
	}
	db.logNumber = 0
}

func (db *DB) writeLog(sync bool, b []byte) error {
	err := db.log.Write(b)
	if err == nil && sync {
		return db.logFile.Sync()
	}
	return err
}

func (db *DB) writeBatch(sync bool, batch batch.Batch, reply chan error) {
	switch {
	case db.logErr != nil:
		reply <- db.logErr
		return
	case db.compactionErr != nil:
		reply <- db.compactionErr
		return
	}
	lastSequence := db.state.LastSequence()
	batch.SetSequence(lastSequence + 1)
	lastSequence = lastSequence.Next(uint64(batch.Count()))
	err := db.writeLog(sync, batch.Bytes())
	if err != nil {
		reply <- err
		// We don't known whether last batch was written successfully
		// or not. We will try to switch to a new log file, and compact
		// current memtable to Level-0. If this process success, the
		// last batch is lost. It is ok, we never report a success to
		// clients. But if server is shutdown in this process and the
		// last batch was written to log, we should guarantee that
		// entries in new log don't conflict with old log in sequence
		// number.
		//
		// So an error write may indicate a successful write ?
		db.state.SetLastSequence(lastSequence)
		db.closeLog(err)
		return
	}
	batch.Iterate(db.mem)
	// Setting last sequence happens before sending reply, which happens
	// before completion of receiving. Readers will observe the new last
	// sequence eventually. So we don't do any sychronization here.
	db.state.SetLastSequence(lastSequence)
	reply <- err
}

func drainRequests(requestc chan batch.Request, err error) {
	for req := range requestc {
		req.Reply <- err
	}
}

func (db *DB) merge() {
	var group batch.Group
	var requests chan batch.Request
	requestc := db.requestc
	defer db.bgGroup.Done()
	for {
		select {
		case <-db.bgClosing:
			group.Close(errors.ErrDBClosed)
			close(db.requests)
			go drainRequests(db.requestc, errors.ErrDBClosed)
			return
		case req := <-requestc:
			group.Push(req)
			if group.HasPending() {
				requestc = nil
			}
			requests = db.requests
		case requests <- group.Head:
			group.Rewind()
			if group.Empty() {
				requests = nil
			}
			requestc = db.requestc
		}
	}
}

func (db *DB) throttle() (chan batch.Request, <-chan time.Time) {
	bufSize := db.options.WriteBufferSize
	bufUsage := db.mem.ApproximateMemoryUsage()
	level0NumFiles := len(db.state.Current().Levels[0])
	switch {
	case db.log == nil:
		db.tryOpenNextLog()
		return db.requests, nil
	case db.compactionErr != nil:
		return db.requests, nil
	case bufUsage <= bufSize:
		return db.requests, nil
	case level0NumFiles >= configs.L0StopWritesFiles:
		return nil, nil
	}
	db.tryOpenNextLog()
	switch {
	case bufUsage >= bufSize+bufSize/4:
		return nil, nil
	case level0NumFiles >= configs.L0SlowdownFiles:
		return nil, time.After(time.Millisecond)
	}
	return db.requests, nil
}

func (db *DB) serve() {
	go db.merge()
mainLoop:
	for {
		requests, slowdown := db.throttle()
		for {
			select {
			case <-db.closed:
				return
			case logFile := <-db.nextLogFile:
				db.bgGroup.Done()
				if logFile == nil {
					break
				}
				db.openLog(logFile, db.nextLogNumber)
				db.nextLogNumber = 0
				continue mainLoop
			case <-db.collectionDone:
				db.completeCollectionFiles()
			case result := <-db.compactionResultc:
				db.completeCompaction(result.level, result.err, result.edit, result.aborted)
				continue mainLoop
			case <-slowdown:
				requests = db.requests
				slowdown = nil
			case req, ok := <-requests:
				if !ok {
					db.requests = nil
					continue mainLoop
				}
				db.writeBatch(req.Sync, req.Batch, req.Reply)
				continue mainLoop
			}
		}
	}
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
	if db.closing {
		return errors.ErrDBClosed
	}
	replyc := make(chan error, 1)
	db.requestc <- batch.Request{Sync: opts.Sync, Batch: b, Reply: replyc}
	return <-replyc
}

func (db *DB) Close() error {
	db.mu.Lock()
	closing := db.closing
	db.closing = true
	db.mu.Unlock()

	if closing {
		<-db.closed
		return nil
	}
	defer close(db.closed)

	close(db.bgClosing)
	db.bgGroup.Wait()

	if db.locker != nil {
		db.locker.Close()
		db.locker = nil
	}
	db.closeLog(nil)
	db.options.Logger.Close()
	return nil
}

func (db *DB) Get(key []byte, opts *options.ReadOptions) ([]byte, error) {
	return db.get(key, keys.MaxSequence, opts)
}

func (db *DB) get(key []byte, seq keys.Sequence, opts *options.ReadOptions) ([]byte, error) {
	db.mu.RLock()
	if db.closing {
		db.mu.RUnlock()
		return nil, errors.ErrDBClosed
	}
	lastSequence := db.state.LastSequence()
	ver := db.state.RetainCurrent()
	memtables := [2]*memtable.MemTable{db.mem, db.imm}
	db.mu.RUnlock()
	defer db.state.ReleaseVersion(ver)
	if seq == keys.MaxSequence {
		seq = lastSequence
	}
	ikey := keys.NewInternalKey(key, seq, keys.Seek)
	for _, mem := range memtables {
		if mem == nil {
			continue
		}
		value, err, ok := mem.Get(ikey)
		if ok {
			return value, err
		}
	}
	return ver.Get(ikey, opts)
}

func (db *DB) All(opts *options.ReadOptions) iterator.Iterator {
	return db.between(nil, nil, keys.MaxSequence, opts)
}

func (db *DB) Find(start []byte, opts *options.ReadOptions) iterator.Iterator {
	return db.between(start, nil, keys.MaxSequence, opts)
}

func (db *DB) Range(start, limit []byte, opts *options.ReadOptions) iterator.Iterator {
	return db.between(start, limit, keys.MaxSequence, opts)
}

func (db *DB) Prefix(prefix []byte, opts *options.ReadOptions) iterator.Iterator {
	return db.prefix(prefix, keys.MaxSequence, opts)
}

func (db *DB) prefix(prefix []byte, seq keys.Sequence, opts *options.ReadOptions) iterator.Iterator {
	limit := db.options.Comparator.UserKeyComparator.MakePrefixSuccessor(prefix)
	return db.between(prefix, limit, seq, opts)
}

func (db *DB) between(start, limit []byte, seq keys.Sequence, opts *options.ReadOptions) iterator.Iterator {
	db.mu.RLock()
	if db.closing {
		db.mu.RUnlock()
		return iterator.Error(errors.ErrDBClosed)
	}
	lastSequence := db.state.LastSequence()
	mem := db.mem
	imm := db.imm
	ver := db.state.RetainCurrent()
	db.mu.RUnlock()
	var iters []iterator.Iterator
	iters = append(iters, mem.NewIterator())
	if imm != nil {
		iters = append(iters, imm.NewIterator())
	}
	iters = ver.AppendIterators(iters, opts)
	mergeIt := iterator.NewMergeIterator(db.options.Comparator, iters...)
	if seq == keys.MaxSequence {
		seq = lastSequence
	}
	dbIt := newDBIterator(db, ver, seq, mergeIt)
	ucmp := db.options.Comparator.UserKeyComparator
	return iterator.NewRangeIterator(start, limit, ucmp, dbIt)
}

func (db *DB) finalize() {
	close(db.requestc)
}

func initDB(db *DB, name string, s *version.State, locker io.Closer, opts *options.Options) {
	db.name = name
	db.state = s
	db.locker = locker
	db.fs = opts.FileSystem
	db.options = opts
	db.closed = make(chan struct{})
	db.bgClosing = make(chan struct{})
	db.requestc = make(chan batch.Request, 1024)
	db.requests = make(chan batch.Request)
	db.nextLogFile = make(chan file.File, 1)
	db.compactionLevel = -1
	db.compactionResultc = make(chan compactionResult, 16)
	db.snapshots.Init()
	runtime.SetFinalizer(db, (*DB).finalize)
}

func createDB(dbname string, locker io.Closer, opts *options.Options) (*DB, error) {
	state, err := version.Create(dbname, opts)
	if err != nil {
		return nil, err
	}
	logNumber, _ := state.NewFileNumber()
	logName := files.LogFileName(dbname, logNumber)
	logFile, err := opts.FileSystem.Open(logName, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return nil, fmt.Errorf("leveldb: fail to create log file: %s", err)
	}
	db := &DB{
		log:       log.NewWriter(logFile, 0),
		mem:       memtable.New(opts.Comparator),
		logFile:   logFile,
		logNumber: logNumber,
	}
	initDB(db, dbname, state, locker, opts)
	db.bgGroup.Add(1)
	go db.serve()
	return db, nil
}

func recoverDB(dbname string, locker io.Closer, opts *options.Options) (db *DB, err error) {
	state, err := version.Recover(dbname, opts)
	if err != nil {
		return nil, err
	}
	filenames, err := opts.FileSystem.List(dbname)
	if err != nil {
		return nil, err
	}
	logNumber := state.LogFileNumber()
	var logs []uint64
	tables := make(map[uint64]struct{})
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
	initDB(db, dbname, state, locker, opts)
	if err := db.recoverLogs(logs); err != nil {
		db.closeLog(nil)
		return nil, err
	}
	db.bgGroup.Add(1)
	go db.serve()
	db.tryStartBackground()
	return db, nil
}
