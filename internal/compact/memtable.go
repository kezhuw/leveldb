package compact

import (
	"os"

	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/memtable"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/table"
)

func NewMemTableCompaction(fileNumber uint64, fileName string, smallestSequence keys.Sequence, mem *memtable.MemTable, opts *options.Options) Compactor {
	c := &memtableCompaction{
		mem:              mem,
		fileNumbers:      make([]uint64, 1),
		smallestSequence: smallestSequence,
		fs:               opts.FileSystem,
		options:          opts,
		tableName:        fileName,
	}
	c.fileNumbers[0] = fileNumber
	c.tableMeta.Number = fileNumber
	c.tableMeta.Size = 0
	return c
}

type memtableCompaction struct {
	mem              *memtable.MemTable
	smallestSequence keys.Sequence
	fileNumbers      []uint64

	fs      file.FileSystem
	options *options.Options

	tableMeta   manifest.FileMeta
	tableName   string
	tableWriter table.Writer
}

func (c *memtableCompaction) Level() int {
	return -1
}

func (c *memtableCompaction) Rewind() {
	c.tableMeta.Size = 0
}

func (c *memtableCompaction) FileNumbers() []uint64 {
	return c.fileNumbers
}

func (c *memtableCompaction) compact() (err error) {
	f, err := c.fs.Open(c.tableName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		if err != nil {
			c.fs.Remove(c.tableName)
		}
	}()

	it := c.mem.NewIterator()
	defer it.Release()

	if !it.First() {
		return it.Err()
	}

	w := &c.tableWriter
	w.Reset(f, c.options)

	w.Add(it.Key(), it.Value())
	c.tableMeta.Smallest = append(c.tableMeta.Smallest[:0], it.Key()...)
	c.tableMeta.Largest = append(c.tableMeta.Largest[:0], c.tableMeta.Smallest...)
	lastUserKey, lastSequence, _ := c.tableMeta.Smallest.Split()
	ucmp := c.options.Comparator.UserKeyComparator
	for it.Next() {
		key := it.Key()
		currentUserKey, currentSequence, _ := keys.InternalKey(key).Split()
		if lastSequence <= c.smallestSequence && ucmp.Compare(lastUserKey, currentUserKey) == 0 {
			continue
		}
		w.Add(key, it.Value())
		c.tableMeta.Largest = append(c.tableMeta.Largest[:0], key...)
		lastUserKey, lastSequence = c.tableMeta.Largest.UserKey(), currentSequence
	}
	if err := it.Err(); err != nil {
		return err
	}
	if err := w.Finish(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	c.tableMeta.Size = uint64(w.FileSize())
	return nil
}

func (c *memtableCompaction) record(edit *manifest.Edit) {
	if c.tableMeta.Size == 0 {
		return
	}
	fmeta := &manifest.FileMeta{
		Number:   c.tableMeta.Number,
		Size:     c.tableMeta.Size,
		Smallest: c.tableMeta.Smallest.Dup(),
		Largest:  c.tableMeta.Largest.Dup(),
	}
	edit.AddedFiles = append(edit.AddedFiles[:0], manifest.LevelFileMeta{Level: 0, FileMeta: fmeta})
}

func (c *memtableCompaction) Compact(edit *manifest.Edit) error {
	err := c.compact()
	if err != nil {
		return err
	}
	c.record(edit)
	return nil
}
