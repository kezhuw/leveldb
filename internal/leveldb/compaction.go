package leveldb

import (
	"container/list"

	"github.com/kezhuw/leveldb/internal/compaction"
	"github.com/kezhuw/leveldb/internal/compactor"
	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/memtable"
)

type compactionResult struct {
	err     error
	level   int
	request *compactionRequest
	edit    *manifest.Edit
	version *manifest.Version
}

type compactionEdit struct {
	level   int
	request *compactionRequest
	*manifest.Edit
}

func (db *DB) tryCompactFile(file manifest.LevelFileMeta) {
	select {
	case db.compactionFile <- file:
	default:
	}
}

func (db *DB) tryLevelCompaction() {
	select {
	case db.compactionRequestChan <- &compactionRequest{}:
	default:
	}
}

func (db *DB) compact(request *compactionRequest, c compactor.Compactor, edit *manifest.Edit) {
	level, err := c.Level(), c.Compact(edit)
	if err != nil {
		db.compactionResult <- compactionResult{level: level, request: request, err: err}
		return
	}
	if level == -1 {
		db.memtableEdit <- compactionEdit{level: level, request: request, Edit: edit}
		return
	}
	db.compactionEdit <- compactionEdit{level: level, request: request, Edit: edit}
}

func (db *DB) startMemTableCompaction(request *compactionRequest, registry *compaction.Registry, mem *memtable.MemTable) bool {
	registration := registry.Register(-1, 0)
	if registration == nil {
		return false
	}
	m := db.manifest
	fileNumber, nextFileNumber := m.NewFileNumber()
	fileName := files.TableFileName(db.name, fileNumber)
	compactor := compactor.NewMemTableCompactor(fileNumber, fileName, db.getSmallestSnapshot(), mem, db.options)
	edit := &manifest.Edit{
		LogNumber:      db.logNumber,
		NextFileNumber: nextFileNumber,
	}
	registration.NextFileNumber = fileNumber
	go db.compact(request, compactor, edit)
	return true
}

func (db *DB) startLevelCompactions(request *compactionRequest, compactions []*manifest.Compaction) {
	if len(compactions) == 0 {
		return
	}
	smallestSequence := db.getSmallestSnapshot()
	for _, c := range compactions {
		edit := &manifest.Edit{
			NextFileNumber: c.Registration.NextFileNumber,
		}
		compactor := compactor.NewLevelCompactor(db.name, smallestSequence, c, db.manifest, db.options)
		go db.compact(request, compactor, edit)
	}
}

type compactionRequest struct {
	MemTable *memtable.MemTable
	Start    []byte
	Limit    []byte
	Reply    chan error
	Manual   bool
}

var fileCompactionRequest = &compactionRequest{}
var levelCompactionRequest = &compactionRequest{}

type compactionContext struct {
	db *DB

	registry    compaction.Registry
	compactions list.List

	pendingCompactionFiles [configs.NumberLevels - 1]manifest.FileList

	pendingObsoleteFiles uint64
	ongoingObsoleteFiles chan struct{}

	compactionErr error
	manifestErr   error
}

func newCompactionContext(db *DB) *compactionContext {
	ctx := &compactionContext{db: db}
	ctx.registry.Recap(db.options.CompactionConcurrency)
	return ctx
}

func (ctx *compactionContext) startCompactionConcurrently(request *compactionRequest) bool {
	db := ctx.db
	switch {
	case request.Manual:
		switch {
		case request.MemTable != nil:
			db.startMemTableCompaction(request, &ctx.registry, request.MemTable)
		case !ctx.startRangeCompaction(0, request, db.manifest.Version()):
			return true
		}
		return false
	case request.MemTable != nil:
		return db.startMemTableCompaction(request, &ctx.registry, request.MemTable)
	default:
		compactions := db.manifest.PickCompactions(&ctx.registry, ctx.pendingCompactionFiles[:])
		db.startLevelCompactions(request, compactions)
		return true
	}
}

func (ctx *compactionContext) AddCompaction(request *compactionRequest) {
	if ctx.manifestErr != nil {
		if request.Reply != nil {
			request.Reply <- ctx.manifestErr
		}
		return
	}
	if ctx.compactions.Len() != 0 || !ctx.startCompactionConcurrently(request) {
		ctx.compactions.PushBack(request)
	} else if request.Manual && request.Reply != nil {
		request.Reply <- nil
	}
}

func (ctx *compactionContext) tryCompaction(request *compactionRequest) {
	if ctx.compactions.Len() == 0 {
		ctx.AddCompaction(request)
	}
}

func (ctx *compactionContext) checkNextCompaction() {
	front := ctx.compactions.Front()
	if front == nil {
		return
	}
	request := front.Value.(*compactionRequest)
	if ctx.startCompactionConcurrently(request) {
		ctx.compactions.Remove(front)
	}
}

func (ctx *compactionContext) triggerNextCompaction() {
	ctx.checkNextCompaction()
	ctx.tryCompaction(levelCompactionRequest)
}

func (ctx *compactionContext) AddCompactionFile(level int, file *manifest.FileMeta) {
	ctx.pendingCompactionFiles[level] = append(ctx.pendingCompactionFiles[level], file)
	ctx.tryCompaction(fileCompactionRequest)
}

func (ctx *compactionContext) startRangeCompaction(level int, request *compactionRequest, version *manifest.Version) bool {
	fromLevel := version.NextOverlapingLevel(level, request.Start, request.Limit)
	if fromLevel < 0 {
		return false
	}
	nextOverlappingLevel := version.NextOverlapingLevel(fromLevel+1, request.Start, request.Limit)
	if nextOverlappingLevel < 0 {
		return false
	}
	c := version.PickRangeCompaction(&ctx.registry, fromLevel, request.Start, request.Limit)
	ctx.db.startLevelCompactions(request, []*manifest.Compaction{c})
	return true
}

func (ctx *compactionContext) handleSuccessfulCompaction(level int, request *compactionRequest, version *manifest.Version) {
	db := ctx.db
	db.switchVersion(level, version)
	ctx.registry.Complete(level)
	ctx.UpdateObsoleteFiles(ctx.registry.NextFileNumber(0))
	if request.Manual {
		if ctx.startRangeCompaction(level+1, request, version) {
			return
		}
		ctx.compactions.Remove(ctx.compactions.Front())
	}
	if request.Reply != nil {
		request.Reply <- nil
	}
	ctx.triggerNextCompaction()
}

func (ctx *compactionContext) handleFailedCompaction(err error, level int, request *compactionRequest, version *manifest.Version) {
	db := ctx.db
	if request.Manual {
		ctx.compactions.Remove(ctx.compactions.Front())
	}
	if request.Reply != nil {
		request.Reply <- err
	}
	if version == nil {
		if ctx.compactionErr == nil {
			ctx.compactionErr = err
			db.compactionErrChan <- err
		}
		ctx.registry.Complete(level)
		ctx.UpdateObsoleteFiles(ctx.registry.NextFileNumber(0))
		ctx.checkNextCompaction()
	} else {
		if ctx.manifestErr == nil {
			ctx.manifestErr = err
			db.manifestErrChan <- err
		}
		// We don't known whether version edit was written or not,
		// so we can't collect table files generated by this compaction.
		ctx.registry.Corrupt(level)
		ctx.purgePendingCompactions(err)
	}
}

func (ctx *compactionContext) purgePendingCompactions(err error) {
	element := ctx.compactions.Front()
	for element != nil {
		request := element.Value.(*compactionRequest)
		if request.Reply != nil {
			request.Reply <- err
		}
		ctx.compactions.Remove(element)
		element = ctx.compactions.Front()
	}
}

func (ctx *compactionContext) CompleteCompaction(result *compactionResult) {
	switch {
	case result.err == nil:
		ctx.handleSuccessfulCompaction(result.level, result.request, result.version)
	default:
		ctx.handleFailedCompaction(result.err, result.level, result.request, result.version)
	}
}

func (ctx *compactionContext) UpdateObsoleteFiles(obsoleteFiles uint64) {
	ctx.pendingObsoleteFiles = ctx.db.updateObsoleteTableNumber(ctx.pendingObsoleteFiles, obsoleteFiles)
}

func (ctx *compactionContext) RemoveObsoleteFiles() {
	if ctx.pendingObsoleteFiles != 0 && ctx.ongoingObsoleteFiles == nil {
		db := ctx.db
		done := make(chan struct{})
		go db.removeObsoleteFiles(ctx.pendingObsoleteFiles, db.manifest.LogFileNumber(), db.manifest.ManifestFileNumber(), done)
		ctx.pendingObsoleteFiles = 0
		ctx.ongoingObsoleteFiles = done
	}
}

func (db *DB) serveCompaction(done chan struct{}) {
	go db.serveVersionEdit(db.manifest.Version())
	defer close(done)
	defer close(db.compactionEdit)
	closing := db.bgClosing
	ctx := newCompactionContext(db)
	db.removeObsoleteFilesAsync(0)
	for !(closing == nil && ctx.registry.Concurrency() == 0 && ctx.ongoingObsoleteFiles == nil && ctx.pendingObsoleteFiles == 0) {
		select {
		case tableNumber := <-db.obsoleteFilesChan:
			ctx.UpdateObsoleteFiles(tableNumber)
		case <-ctx.ongoingObsoleteFiles:
			ctx.ongoingObsoleteFiles = nil
		case <-closing:
			closing = nil
		case edit := <-db.memtableEdit:
			output := edit.AddedFiles[0].FileMeta
			maxLevel := db.manifest.Version().PickLevelForMemTableOutput(output.Smallest, output.Largest)
			if maxLevel > 0 {
				edit.AddedFiles[0].Level = ctx.registry.ExpandTo(-1, maxLevel)
			}
			db.compactionEdit <- edit
		case request := <-db.compactionRequestChan:
			ctx.AddCompaction(request)
		case file := <-db.compactionFile:
			ctx.AddCompactionFile(file.Level, file.FileMeta)
		case result := <-db.compactionResult:
			ctx.CompleteCompaction(&result)
		}
		ctx.RemoveObsoleteFiles()
	}
	ctx.purgePendingCompactions(errors.ErrDBClosed)
}

func (db *DB) serveVersionEdit(tip *manifest.Version) {
	var lastErr error
	for edit := range db.compactionEdit {
		next, err := db.manifest.Log(tip, edit.Edit)
		if err != nil {
			lastErr = err
			db.compactionResult <- compactionResult{err: err, level: edit.level, request: edit.request, version: tip}
			break
		}
		tip = next
		db.compactionResult <- compactionResult{level: edit.level, request: edit.request, version: tip}
	}
	if lastErr != nil {
		for edit := range db.compactionEdit {
			db.compactionResult <- compactionResult{err: lastErr, level: edit.level, request: edit.request, version: tip}
		}
	}
}
