package leveldb

import (
	"os"
	"time"

	"github.com/kezhuw/leveldb/internal/batch"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/memtable"
	"github.com/kezhuw/leveldb/internal/record"
	"github.com/kezhuw/leveldb/internal/request"
)

var elapsedSlowDown = make(chan time.Time)

func (db *DB) tryOpenNextLog() {
	if db.nextLogNumber != 0 || db.loadBundle().imm != nil {
		return
	}
	db.nextLogNumber, _ = db.manifest.NewFileNumber()
	go db.openNextLog()
}

func (db *DB) openNextLog() {
	fileName := files.LogFileName(db.name, db.nextLogNumber)
	var timeout time.Duration
	var lastErr error
	for {
		f, err := db.fs.Open(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
		if err == nil {
			db.nextLogFile <- f
			return
		}
		if lastErr == nil {
			db.nextLogFileErr <- err
			lastErr = err
		}
		timeout += timeout + time.Second
		select {
		case <-db.bgClosing:
			db.nextLogFile <- nil
			return
		case <-time.After(timeout):
		}
	}
}

func (db *DB) openLog(f file.File, offset int64, number uint64) {
	db.log = record.NewWriter(f, offset)
	db.logErr = nil
	if db.logFile != nil {
		db.logFile.Close()
	}
	db.logFile = f
	db.logNumber = number
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

func (db *DB) writeBatch(mem *memtable.MemTable, sync bool, batch batch.Batch, reply chan error) error {
	switch {
	case db.logErr != nil:
		reply <- db.logErr
		return db.logErr
	case db.manifestErr != nil:
		reply <- db.manifestErr
		return db.manifestErr
	case db.compactionErr != nil:
		reply <- db.compactionErr
		return db.compactionErr
	}
	lastSequence := db.manifest.LastSequence()
	batch.SetSequence(lastSequence + 1)
	lastSequence = lastSequence.Next(uint64(batch.Count()))
	err := db.writeLog(sync, batch.Bytes())
	if err != nil {
		db.closeLog(err)
		reply <- err
		return err
	}
	if err = batch.Iterate(mem); err != nil {
		db.logErr = err
		reply <- err
		return err
	}
	// Setting last sequence happens before sending reply, which happens
	// before completion of receiving. Readers will observe the new last
	// sequence eventually. So we don't do any sychronization here.
	db.manifest.StoreLastSequence(lastSequence)
	reply <- nil
	if mem.ApproximateMemoryUsage() >= db.options.WriteBufferSize {
		db.tryOpenNextLog()
	}
	return nil
}

func (db *DB) wakeupWrite(level int) {
	if level != 0 {
		return
	}
	select {
	case db.requestw <- struct{}{}:
	default:
	}
}

func (db *DB) slowdownLog(c <-chan time.Time) (chan request.Request, <-chan time.Time) {
	switch {
	case c == nil:
		return nil, time.After(time.Millisecond)
	case c == elapsedSlowDown:
		return db.requests, elapsedSlowDown
	default:
		return nil, c
	}
}

func (db *DB) throttleLog(version *manifest.Version, slowdown <-chan time.Time) (chan request.Request, <-chan time.Time) {
	level0NumFiles := len(version.Levels[0])
	switch {
	case db.logErr != nil || db.compactionErr != nil || db.manifestErr != nil:
		return db.requests, nil
	case level0NumFiles >= db.options.Level0SlowdownWriteFiles:
		return db.slowdownLog(slowdown)
	case level0NumFiles >= db.options.Level0StopWriteFiles:
		return nil, nil
	default:
		return db.requests, nil
	}
}

func drainRequests(requestc chan request.Request, err error) {
	for req := range requestc {
		req.Reply <- err
	}
}

func (db *DB) serveMerge() {
	var group request.Group
	var requests chan request.Request
	requestc := db.requestc
	for {
		select {
		case <-db.bgClosing:
			group.Close(errors.ErrDBClosed)
			close(db.requests)
			go drainRequests(db.requestc, errors.ErrDBClosed)
			return
		case req := <-requestc:
			group.Push(req)
			if group.Full() {
				requestc = nil
			}
			requests = db.requests
		case requests <- group.Head():
			group.Rewind()
			if group.Empty() {
				requests = nil
			}
			requestc = db.requestc
		}
	}
}

type manualCompaction struct {
	start []byte
	limit []byte
	reply chan error
}

func (db *DB) CompactRange(start, limit []byte) error {
	compaction := &manualCompaction{
		start: start,
		limit: limit,
		reply: make(chan error, 1),
	}
	db.manualCompactionChan <- compaction
	return <-compaction.reply
}

func (db *DB) serveWrite() {
	defer db.bgGroup.Done()
	go db.serveMerge()
	compactionContext := newCompactionContext(db)
	go compactionContext.serveCompaction()
	var lastErr error
	var requests chan request.Request
	var slowdown <-chan time.Time
	var pendingManualCompaction *manualCompaction
	mem := db.bundle.mem
	version := db.bundle.version
	manualCompactionChan := db.manualCompactionChan
	compactionVersionChan := compactionContext.CompactionVersionChan
	// There may be too many files in level-0 to throttle writes, fire
	// an level compaction to solve this.
	db.tryLevelCompaction()
	for db.requests != nil || db.nextLogNumber != 0 || compactionVersionChan != nil {
		requests, slowdown = db.throttleLog(version, slowdown)
		select {
		case manualCompaction := <-manualCompactionChan:
			if lastErr != nil {
				manualCompaction.reply <- lastErr
				break
			}
			if !mem.Overlap(manualCompaction.start, manualCompaction.limit) {
				db.compactionRequestChan <- &compactionRequest{
					Start:  manualCompaction.start,
					Limit:  manualCompaction.limit,
					Reply:  manualCompaction.reply,
					Manual: true,
				}
				break
			}
			db.tryOpenNextLog()
			pendingManualCompaction = manualCompaction
			manualCompactionChan = nil
		case compactionVersion, ok := <-compactionVersionChan:
			if !ok {
				compactionVersionChan = nil
				continue
			}
			version = compactionVersion.version
			db.switchVersion(compactionVersion.level, version)
		case logFile := <-db.nextLogFile:
			if logFile == nil {
				db.nextLogNumber = 0
				break
			}
			db.openLog(logFile, 0, db.nextLogNumber)
			imm := mem
			mem = db.switchMemTable(mem)
			switch {
			case pendingManualCompaction != nil:
				db.compactionRequestChan <- &compactionRequest{
					MemTable: imm,
					Start:    pendingManualCompaction.start,
					Limit:    pendingManualCompaction.limit,
					Reply:    pendingManualCompaction.reply,
					Manual:   true,
				}
				pendingManualCompaction = nil
				manualCompactionChan = db.manualCompactionChan
			default:
				db.compactionRequestChan <- &compactionRequest{MemTable: imm}
			}
			db.nextLogNumber = 0
			lastErr = nil
		case lastErr = <-db.nextLogFileErr:
		case lastErr = <-db.manifestErrChan:
			db.manifestErr = lastErr
		case lastErr = <-db.compactionErrChan:
			db.compactionErr = lastErr
		case <-db.requestw:
		case <-slowdown:
			slowdown = elapsedSlowDown
		case req, ok := <-requests:
			switch {
			case !ok:
				db.requests = nil
			case lastErr != nil:
				req.Reply <- lastErr
			default:
				lastErr = db.writeBatch(mem, req.Sync, req.Batch, req.Reply)
			}
			slowdown = nil
		}
	}
	if pendingManualCompaction != nil {
		pendingManualCompaction.reply <- errors.ErrDBClosed
	}
}
