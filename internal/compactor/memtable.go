package compactor

import (
	"os"

	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/manifest"
	"github.com/kezhuw/leveldb/internal/memtable"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/table"
)

// CompactMemTable compacts memtable to file.
func CompactMemTable(fileNumber uint64, fileName string, smallestSequence keys.Sequence, mem *memtable.MemTable, opts *options.Options) (*manifest.FileMeta, error) {
	compactor := memtableCompactor{
		mem:              mem,
		smallestSequence: smallestSequence,
		fs:               opts.FileSystem,
		options:          opts,
		tableName:        fileName,
	}
	compactor.tableMeta.Number = fileNumber
	compactor.tableMeta.Size = 0
	return compactor.compact()
}

func NewMemTableCompactor(fileNumber uint64, fileName string, smallestSequence keys.Sequence, mem *memtable.MemTable, opts *options.Options) Compactor {
	c := &memtableCompactor{
		mem:              mem,
		smallestSequence: smallestSequence,
		fs:               opts.FileSystem,
		options:          opts,
		tableName:        fileName,
	}
	c.tableMeta.Number = fileNumber
	c.tableMeta.Size = 0
	return c
}

type memtableCompactor struct {
	mem              *memtable.MemTable
	smallestSequence keys.Sequence

	fs      file.FileSystem
	options *options.Options

	tableMeta   manifest.FileMeta
	tableName   string
	tableWriter table.Writer
}

func (c *memtableCompactor) Level() int {
	return -1
}

func (c *memtableCompactor) Rewind() {
	c.tableMeta.Size = 0
}

func (c *memtableCompactor) compact() (*manifest.FileMeta, error) {
	f, err := c.fs.Open(c.tableName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, err
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
		return nil, it.Err()
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
		return nil, err
	}
	if err := w.Finish(); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	c.tableMeta.Size = uint64(w.FileSize())
	return &manifest.FileMeta{
		Number:   c.tableMeta.Number,
		Size:     c.tableMeta.Size,
		Smallest: c.tableMeta.Smallest.Dup(),
		Largest:  c.tableMeta.Largest.Dup(),
	}, nil
}

func (c *memtableCompactor) Compact(edit *manifest.Edit) error {
	file, err := c.compact()
	if err != nil {
		return err
	}
	if file == nil {
		return errors.ErrEmptyMemTable
	}
	edit.AddedFiles = append(edit.AddedFiles[:0], manifest.LevelFileMeta{Level: 0, FileMeta: file})
	return nil
}
