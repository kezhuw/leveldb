package compactor

import (
	"os"
	"sort"

	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/table"
)

func NewLevelCompactor(dbname string, seq keys.Sequence, compaction *manifest.Compaction, m *manifest.Manifest, opts *options.Options) Compactor {
	if compaction.IsTrivialMove() {
		return &moveCompactor{c: compaction}
	}
	c := &levelCompactor{
		dbname:           dbname,
		manifest:         m,
		options:          opts,
		fs:               opts.FileSystem,
		Compaction:       compaction,
		smallestSequence: seq,
	}
	return c
}

type moveCompactor struct {
	c *manifest.Compaction
}

type levelCompactor struct {
	*manifest.Compaction

	manifest *manifest.Manifest
	dbname   string
	options  *options.Options
	fs       file.FileSystem

	smallestSequence keys.Sequence

	outputs manifest.FileList

	tableName   string
	tableMeta   manifest.FileMeta
	tableFile   file.WriteCloser
	tableWriter table.Writer

	fileNumbers    []uint64
	fileNumbersOff int
	nextFileNumber uint64

	grandparentsIndex           int
	grandparentsSeenKey         bool
	grandparentsOverlappedBytes uint64

	levelFilePointers [configs.NumberLevels]int
}

func (c *moveCompactor) Level() int {
	return c.c.Level
}

func (c *moveCompactor) Rewind() {
}

func (c *moveCompactor) Compact(edit *manifest.Edit) error {
	input := c.c.Inputs[0][0]
	output := &manifest.FileMeta{
		Number:   input.Number,
		Size:     input.Size,
		Smallest: input.Smallest,
		Largest:  input.Largest,
	}
	edit.AddedFiles = append(edit.AddedFiles[:0], manifest.LevelFileMeta{Level: c.c.Level + 1, FileMeta: output})
	edit.DeletedFiles = append(edit.DeletedFiles[:0], manifest.LevelFileNumber{Level: c.c.Level, Number: input.Number})
	return nil
}

var zeroLevelFilePointers [configs.NumberLevels]int

func (c *levelCompactor) Level() int {
	return c.Compaction.Level
}

func (c *levelCompactor) Rewind() {
	if c.tableFile != nil {
		c.tableFile.Close()
		c.tableFile = nil
	}
	c.outputs = c.outputs[:0]
	c.fileNumbersOff = 0
	c.grandparentsIndex = 0
	c.grandparentsSeenKey = false
	c.grandparentsOverlappedBytes = 0
	copy(c.levelFilePointers[:], zeroLevelFilePointers[:])
}

func (c *levelCompactor) closeCurrentTable() error {
	f := c.tableFile
	if f == nil {
		return nil
	}
	c.tableFile = nil
	defer f.Close()

	c.tableMeta.Largest = append(c.tableMeta.Largest[:0], c.tableWriter.LastKey()...)
	err := c.tableWriter.Finish()
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	c.outputs = append(c.outputs, &manifest.FileMeta{
		Number:   c.tableMeta.Number,
		Size:     uint64(c.tableWriter.FileSize()),
		Smallest: c.tableMeta.Smallest.Dup(),
		Largest:  c.tableMeta.Largest.Dup(),
	})
	return nil
}

func (c *levelCompactor) newFileNumber() uint64 {
	if c.fileNumbersOff < len(c.fileNumbers) {
		x := c.fileNumbers[c.fileNumbersOff]
		c.fileNumbersOff++
		return x
	}
	var fileNumber uint64
	fileNumber, c.nextFileNumber = c.manifest.NewFileNumber()
	c.fileNumbers = append(c.fileNumbers, fileNumber)
	c.fileNumbersOff = len(c.fileNumbers)
	return fileNumber
}

func (c *levelCompactor) openTableFile() error {
	err := c.closeCurrentTable()
	if err != nil {
		return err
	}
	tableNumber := c.newFileNumber()
	tableName := files.TableFileName(c.dbname, tableNumber)
	f, err := c.fs.Open(tableName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	c.tableName = tableName
	c.tableFile = f
	c.tableMeta.Number = tableNumber
	c.tableWriter.Reset(f, c.options)
	c.grandparentsOverlappedBytes = 0
	return nil
}

func (c *levelCompactor) shouldStopBefore(ikey []byte) bool {
	icmp := c.options.Comparator
	for c.grandparentsIndex < len(c.Grandparents) && icmp.Compare(ikey, c.Grandparents[c.grandparentsIndex].Largest) > 0 {
		if c.grandparentsSeenKey {
			c.grandparentsOverlappedBytes += c.Grandparents[c.grandparentsIndex].Size
		}
		c.grandparentsIndex++
	}
	c.grandparentsSeenKey = true
	return c.grandparentsOverlappedBytes > configs.MaxGrandparentOverlappingBytes
}

func (c *levelCompactor) isBaseLevelForKey(ukey []byte) bool {
	ucmp := c.options.Comparator.UserKeyComparator
	v := c.Base
	for level := c.Level() + 2; level < configs.NumberLevels; level++ {
		k := c.levelFilePointers[level]
		files := v.Levels[level][k:]
		n := len(files)
		if n == 0 {
			continue
		}
		i := sort.Search(n, func(i int) bool { return ucmp.Compare(ukey, files[i].Largest.UserKey()) <= 0 })
		c.levelFilePointers[level] = k + i
		if i != n && ucmp.Compare(ukey, files[i].Smallest) >= 0 {
			return false
		}
	}
	return true
}

func (c *levelCompactor) add(key, value []byte, firstTime bool) error {
	// We save keys with same user key in same table, so tables in level+1
	// will not overlap with each other in user key space.
	switch {
	case c.tableFile == nil:
		fallthrough
	case firstTime && (c.tableWriter.FileSize() >= c.MaxOutputFileSize || c.shouldStopBefore(key)):
		err := c.openTableFile()
		if err != nil {
			return err
		}
	}
	if c.tableWriter.Empty() {
		c.tableMeta.Smallest = append(c.tableMeta.Smallest[:0], key...)
	}
	return c.tableWriter.Add(key, value)
}

func (c *levelCompactor) compact() error {
	it := c.NewIterator()
	defer it.Close()

	ucmp := c.options.Comparator.UserKeyComparator
	var lastKey keys.InternalKey
	var lastSequence keys.Sequence
	for it.Next() {
		ikey, ok := keys.ToInternalKey(it.Key())
		if !ok {
			return errors.ErrCorruptInternalKey
		}
		currentUserKey, currentSequence, kind := ikey.Split()
		if len(lastKey) == 0 || ucmp.Compare(lastKey.UserKey(), currentUserKey) != 0 {
			lastKey = append(lastKey[:0], currentUserKey...)
			lastSequence = keys.MaxSequence
		}
		switch {
		case lastSequence <= c.smallestSequence:
		case kind == keys.Delete && currentSequence <= c.smallestSequence && c.isBaseLevelForKey(currentUserKey):
		default:
			err := c.add(ikey, it.Value(), lastSequence == keys.MaxSequence)
			if err != nil {
				return err
			}
		}
		lastSequence = currentSequence
	}

	if err := c.closeCurrentTable(); err != nil {
		return err
	}

	return it.Err()
}

func (c *levelCompactor) record(edit *manifest.Edit) {
	edit.AddedFiles = edit.AddedFiles[:0]
	edit.DeletedFiles = edit.DeletedFiles[:0]
	edit.CompactPointers = edit.CompactPointers[:0]
	level := c.Level()
	for which := 0; which < 2; which++ {
		files := c.Inputs[which]
		for _, f := range files {
			edit.DeletedFiles = append(edit.DeletedFiles, manifest.LevelFileNumber{Level: level + which, Number: f.Number})
		}
	}
	for _, f := range c.outputs {
		edit.AddedFiles = append(edit.AddedFiles, manifest.LevelFileMeta{Level: level + 1, FileMeta: f})
	}
	edit.CompactPointers = append(edit.CompactPointers, manifest.LevelCompactPointer{Level: level, Largest: c.NextCompactPointer})
	if c.nextFileNumber > edit.NextFileNumber {
		edit.NextFileNumber = c.nextFileNumber
	}
}

func (c *levelCompactor) Compact(edit *manifest.Edit) error {
	err := c.compact()
	if err != nil {
		return err
	}
	c.record(edit)
	return nil
}
