package table

import (
	"io"

	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/table/block"
	"github.com/kezhuw/leveldb/internal/table/filter"
)

type Table struct {
	f          file.ReadCloser
	fileNumber uint64
	blocks     *BlockCache
	options    *options.Options
	dataIndex  *block.Block
	filter     *filter.Reader
}

func (t *Table) readMetaBlocks(metaIndexHandle block.Handle) {
	if t.options.Filter == nil {
		return
	}
	metaIndex, err := ReadDataBlock(t.f, t.fileNumber, metaIndexHandle, true)
	if err != nil {
		return
	}
	cmp := keys.BytewiseComparator
	it := metaIndex.NewIterator(cmp)
	defer it.Close()
	filterName := []byte("filter." + t.options.Filter.Name())
	if !it.Seek(filterName) || cmp.Compare(filterName, it.Key()) != 0 {
		return
	}
	h, n := block.DecodeHandle(it.Value())
	if n <= 0 {
		return
	}
	buf, err := ReadBlock(t.f, t.fileNumber, h, true)
	if err != nil {
		return
	}
	t.filter = filter.NewReader(t.options.Filter, buf)
}

func (t *Table) Get(ikey keys.InternalKey, opts *options.ReadOptions) ([]byte, error, bool) {
	indexIt := t.dataIndex.NewIterator(t.options.Comparator)
	if !indexIt.Seek(ikey) {
		err := indexIt.Close()
		return nil, err, err != nil
	}
	defer indexIt.Close()

	h, n := block.DecodeHandle(indexIt.Value())
	if n <= 0 {
		return nil, errors.NewCorruption(t.fileNumber, "table data index", -1, "invalid block handle"), true
	}
	if t.filter != nil && !t.filter.Contains(h.Offset, []byte(ikey)) {
		return nil, nil, false
	}

	dataIt := t.readBlockHandleIterator(h, opts)
	if !dataIt.Seek(ikey) {
		err := dataIt.Close()
		return nil, err, err != nil
	}
	defer dataIt.Close()

	ukey, _, kind := keys.InternalKey(dataIt.Key()).Split()
	if t.options.Comparator.UserKeyComparator.Compare(ukey, ikey.UserKey()) == 0 {
		switch kind {
		case keys.Delete:
			return nil, errors.ErrNotFound, true
		default:
			return dataIt.Value(), nil, true
		}
	}
	return nil, nil, false
}

func (t *Table) readBlockHandleIterator(h block.Handle, opts *options.ReadOptions) iterator.Iterator {
	b, err := t.blocks.Read(t.f, t.fileNumber, h, opts.VerifyChecksums, opts.DontFillCache)
	if err != nil {
		return iterator.Error(err)
	}
	return b.NewIterator(t.options.Comparator)
}

func (t *Table) readBlockIterator(handle []byte, opts *options.ReadOptions) iterator.Iterator {
	h, n := block.DecodeHandle(handle)
	if n <= 0 {
		return iterator.Error(errors.NewCorruption(t.fileNumber, "table data index", -1, "invalid block handle"))
	}
	return t.readBlockHandleIterator(h, opts)
}

func (t *Table) NewIterator(opts *options.ReadOptions) iterator.Iterator {
	index := t.dataIndex.NewIterator(t.options.Comparator)
	blockf := func(value []byte) iterator.Iterator {
		return t.readBlockIterator(value, opts)
	}
	return iterator.NewIndexIterator(index, blockf)
}

func OpenTable(f file.ReadCloser, blocks *BlockCache, opts *options.Options, number, size uint64) (t *Table, err error) {
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	if size < footerLength {
		return nil, errors.NewCorruption(number, "table", 0, "file too short")
	}
	var scratch [footerLength]byte
	_, err = f.ReadAt(scratch[:], int64(size-footerLength))
	if err != nil && err != io.EOF {
		return nil, err
	}
	var footer Footer
	err = footer.Unmarshal(scratch[:])
	if err != nil {
		return nil, err
	}
	dataIndex, err := ReadDataBlock(f, number, footer.DataIndexHandle, true)
	if err != nil {
		return nil, err
	}
	t = &Table{
		f:          f,
		fileNumber: number,
		blocks:     blocks,
		options:    opts,
		dataIndex:  dataIndex,
	}
	t.readMetaBlocks(footer.MetaIndexHandle)
	return t, nil
}
