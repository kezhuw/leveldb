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

type compactionVersion struct {
	level   int
	version *manifest.Version
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
	db       *DB
	manifest *manifest.Manifest

	version             *manifest.Version
	loggingManifestEdit *manifest.Edit

	manifestLogChan       chan compactionEdit
	compactionEditChan    chan compactionEdit
	compactionResultChan  chan compactionResult
	CompactionVersionChan chan compactionVersion

	registry    compaction.Registry
	compactions list.List

	pendingCompactionFiles [configs.NumberLevels - 1]manifest.FileList

	pendingObsoleteFiles uint64
	ongoingObsoleteFiles chan struct{}

	compactionErr error
	manifestErr   error
}

func newCompactionContext(db *DB) *compactionContext {
	ctx := &compactionContext{
		db:                    db,
		manifest:              db.manifest,
		version:               db.manifest.Version(),
		manifestLogChan:       make(chan compactionEdit, 1),
		compactionEditChan:    make(chan compactionEdit, configs.NumberLevels),
		compactionResultChan:  make(chan compactionResult, configs.NumberLevels),
		CompactionVersionChan: make(chan compactionVersion, configs.NumberLevels),
	}
	ctx.registry.Recap(db.options.CompactionConcurrency)
	return ctx
}

func (ctx *compactionContext) Close() error {
	close(ctx.manifestLogChan)
	close(ctx.CompactionVersionChan)
	return nil
}

func (ctx *compactionContext) compact(request *compactionRequest, c compactor.Compactor, edit *manifest.Edit) {
	level, err := c.Level(), c.Compact(edit)
	if err != nil {
		ctx.compactionResultChan <- compactionResult{level: level, request: request, err: err}
		return
	}
	ctx.compactionEditChan <- compactionEdit{level: level, request: request, Edit: edit}
}

func (ctx *compactionContext) startMemTableCompaction(request *compactionRequest, mem *memtable.MemTable) bool {
	db := ctx.db
	registration := ctx.registry.Register(-1, 0)
	if registration == nil {
		return false
	}
	fileNumber, nextFileNumber := ctx.manifest.NewFileNumber()
	fileName := files.TableFileName(db.name, fileNumber)
	compactor := compactor.NewMemTableCompactor(fileNumber, fileName, db.getSmallestSnapshot(), mem, db.options)
	edit := &manifest.Edit{
		LogNumber:      db.logNumber,
		NextFileNumber: nextFileNumber,
	}
	registration.NextFileNumber = fileNumber
	go ctx.compact(request, compactor, edit)
	return true
}

func (ctx *compactionContext) startLevelCompactions(request *compactionRequest, compactions []*manifest.Compaction) {
	if len(compactions) == 0 {
		return
	}
	db := ctx.db
	smallestSequence := db.getSmallestSnapshot()
	for _, c := range compactions {
		edit := &manifest.Edit{
			NextFileNumber: c.Registration.NextFileNumber,
		}
		compactor := compactor.NewLevelCompactor(db.name, smallestSequence, c, db.manifest, db.options)
		go ctx.compact(request, compactor, edit)
	}
}

func (ctx *compactionContext) startCompactionConcurrently(request *compactionRequest) bool {
	switch {
	case request.Manual:
		switch {
		case request.MemTable != nil:
			ctx.startMemTableCompaction(request, request.MemTable)
		case !ctx.startRangeCompaction(0, request):
			return true
		}
		return false
	case request.MemTable != nil:
		return ctx.startMemTableCompaction(request, request.MemTable)
	default:
		compactions := ctx.version.PickCompactions(&ctx.registry, ctx.pendingCompactionFiles[:], ctx.manifest.NextFileNumber())
		ctx.startLevelCompactions(request, compactions)
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

func (ctx *compactionContext) startRangeCompaction(level int, request *compactionRequest) bool {
	version := ctx.version
	fromLevel := version.NextOverlapingLevel(level, request.Start, request.Limit)
	if fromLevel < 0 {
		return false
	}
	nextOverlappingLevel := version.NextOverlapingLevel(fromLevel+1, request.Start, request.Limit)
	if nextOverlappingLevel < 0 {
		return false
	}
	c := version.PickRangeCompaction(&ctx.registry, fromLevel, request.Start, request.Limit)
	ctx.startLevelCompactions(request, []*manifest.Compaction{c})
	return true
}

func (ctx *compactionContext) handleSuccessfulCompaction(level int, request *compactionRequest, version *manifest.Version) {
	ctx.version = version
	ctx.manifest.Append(version)
	ctx.registry.Complete(level)
	ctx.CompactionVersionChan <- compactionVersion{level: level, version: version}
	ctx.UpdateObsoleteFiles(ctx.registry.NextFileNumber(0))
	if request.Manual {
		if ctx.startRangeCompaction(level+1, request) {
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

func (ctx *compactionContext) AddCompactionEdit(edit compactionEdit) {
	if edit.level == -1 {
		output := edit.AddedFiles[0].FileMeta
		maxLevel := ctx.version.PickLevelForMemTableOutput(output.Smallest, output.Largest)
		if maxLevel > 0 {
			edit.AddedFiles[0].Level = ctx.registry.ExpandTo(-1, maxLevel)
		}
	}
	ctx.manifestLogChan <- edit
	ctx.loggingManifestEdit = edit.Edit
}

func (ctx *compactionContext) NextCompactionEditChan() chan compactionEdit {
	if ctx.loggingManifestEdit != nil {
		return nil
	}
	return ctx.compactionEditChan
}

func (ctx *compactionContext) CompleteCompaction(result *compactionResult) {
	if ctx.loggingManifestEdit == result.edit {
		ctx.loggingManifestEdit = nil
	}
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

func (ctx *compactionContext) serveCompaction() {
	db := ctx.db
	closing := db.bgClosing
	defer ctx.Close()
	go ctx.serveManifestLog()
	db.removeObsoleteFilesAsync(0)
	for !(closing == nil && ctx.registry.Concurrency() == 0 && ctx.ongoingObsoleteFiles == nil && ctx.pendingObsoleteFiles == 0) {
		select {
		case tableNumber := <-db.obsoleteFilesChan:
			ctx.UpdateObsoleteFiles(tableNumber)
		case <-ctx.ongoingObsoleteFiles:
			ctx.ongoingObsoleteFiles = nil
		case <-closing:
			closing = nil
		case edit := <-ctx.NextCompactionEditChan():
			ctx.AddCompactionEdit(edit)
		case request := <-db.compactionRequestChan:
			ctx.AddCompaction(request)
		case file := <-db.compactionFile:
			ctx.AddCompactionFile(file.Level, file.FileMeta)
		case result := <-ctx.compactionResultChan:
			ctx.CompleteCompaction(&result)
		}
		ctx.RemoveObsoleteFiles()
	}
	ctx.purgePendingCompactions(errors.ErrDBClosed)
}

func (ctx *compactionContext) serveManifestLog() {
	m := ctx.manifest
	tip := ctx.version
	editChan := ctx.manifestLogChan
	resultChan := ctx.compactionResultChan
	var lastErr error
	for edit := range editChan {
		next, err := m.Log(tip, edit.Edit)
		if err != nil {
			lastErr = err
			resultChan <- compactionResult{err: err, level: edit.level, request: edit.request, edit: edit.Edit, version: tip}
			break
		}
		tip = next
		resultChan <- compactionResult{level: edit.level, request: edit.request, edit: edit.Edit, version: tip}
	}
	if lastErr != nil {
		for edit := range editChan {
			resultChan <- compactionResult{err: lastErr, level: edit.level, request: edit.request, edit: edit.Edit, version: tip}
		}
	}
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
