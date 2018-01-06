package table

import (
	"os"
	"runtime"
	"sync"

	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
)

type node struct {
	f    uint64
	t    *Table
	err  error
	wg   sync.WaitGroup
	next *node
	prev *node
}

type nodeList struct {
	len  int
	root node
}

func (l *nodeList) init() {
	l.len = 0
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *nodeList) insertAfter(n *node, at *node) {
	n.next = at.next
	at.next.prev = n
	at.next = n
	n.prev = at
}

func (l *nodeList) PushBack(n *node) {
	l.insertAfter(n, l.root.prev)
	l.len++
}

func (l *nodeList) unlink(n *node) {
	n.next.prev = n.prev
	n.prev.next = n.next
}

func (l *nodeList) Remove(n *node) *node {
	l.unlink(n)
	l.len--
	return n
}

func (l *nodeList) RemoveFront() *node {
	n := l.root.next
	l.unlink(n)
	l.len--
	return n
}

func (l *nodeList) Len() int {
	return l.len
}

func (l *nodeList) MoveToBack(n *node) {
	l.unlink(n)
	l.insertAfter(n, l.root.prev)
}

type cachePool struct {
	mu     sync.Mutex
	tables map[uint64]*node

	cap     int
	nodes   nodeList
	evicts  chan *node
	updates chan *node
}

func newCachePool(cap int) *cachePool {
	p := &cachePool{
		tables:  make(map[uint64]*node),
		cap:     cap,
		evicts:  make(chan *node, 16),
		updates: make(chan *node, 512),
	}
	go p.collect()
	return p
}

func (p *cachePool) Close() {
	close(p.updates)
}

var deletedNode = new(node)

func (p *cachePool) touchNode(n *node) {
	select {
	case p.updates <- n:
	default:
	}
}

func (p *cachePool) updateNode(n *node) {
	switch n.next {
	case nil:
		p.nodes.PushBack(n)
		if p.nodes.Len() > p.cap {
			n := p.nodes.RemoveFront()
			p.mu.Lock()
			delete(p.tables, n.f)
			p.mu.Unlock()
			n.next = deletedNode
			n.prev = nil
		}
	case deletedNode:
	default:
		p.nodes.MoveToBack(n)
	}
}

func (p *cachePool) removeNode(n *node) {
	switch n.next {
	case nil:
		n.next = deletedNode
	case deletedNode:
	default:
		p.nodes.Remove(n)
		n.next = deletedNode
		n.prev = nil
	}
}

func (p *cachePool) collect() {
	p.nodes.init()
	evicts := p.evicts
	updates := p.updates
	for {
		select {
		case n, ok := <-updates:
			if !ok {
				return
			}
			p.updateNode(n)
		case n := <-evicts:
			p.removeNode(n)
		}
	}
}

type Cache struct {
	dbname  string
	fs      file.FileSystem
	blocks  *BlockCache
	options *options.Options
	*cachePool
}

func (c *Cache) load(fileNumber, fileSize uint64, n *node) (*Table, error) {
	defer n.wg.Done()
	tableName := files.TableFileName(c.dbname, fileNumber)
	tableFile, err := c.fs.Open(tableName, os.O_RDONLY)
	if os.IsNotExist(err) {
		tableName = files.SSTTableFileName(c.dbname, fileNumber)
		tableFile, err = c.fs.Open(tableName, os.O_RDONLY)
	}
	if err != nil {
		n.t = nil
		n.err = err
		return nil, err
	}
	n.t, n.err = OpenTable(tableFile, c.blocks, c.options, fileNumber, fileSize)
	return n.t, n.err
}

func (c *Cache) open(fileNumber, fileSize uint64) (*Table, error) {
	c.mu.Lock()
	n := c.tables[fileNumber]
	if n != nil {
		c.mu.Unlock()
		n.wg.Wait()
		c.touchNode(n)
		return n.t, n.err
	}
	n = &node{f: fileNumber}
	c.tables[fileNumber] = n
	n.wg.Add(1)
	c.mu.Unlock()
	c.touchNode(n)
	return c.load(fileNumber, fileSize, n)
}

func (c *Cache) Evict(fileNumber uint64) {
	c.mu.Lock()
	n := c.tables[fileNumber]
	delete(c.tables, fileNumber)
	c.mu.Unlock()
	if n != nil {
		c.evicts <- n
	}
}

func (c *Cache) Get(fileNumber uint64, fileSize uint64, ikey keys.InternalKey, opts *options.ReadOptions) ([]byte, error, bool) {
	t, err := c.open(fileNumber, fileSize)
	if err != nil {
		return nil, err, true
	}
	return t.Get(ikey, opts)
}

func (c *Cache) NewIterator(fileNumber uint64, fileSize uint64, opts *options.ReadOptions) iterator.Iterator {
	t, err := c.open(fileNumber, fileSize)
	if err != nil {
		return iterator.Error(err)
	}
	return t.NewIterator(opts)
}

func (c *Cache) finalize() {
	c.cachePool.Close()
}

func NewCache(dbname string, opts *options.Options) *Cache {
	c := &Cache{
		dbname:    dbname,
		fs:        opts.FileSystem,
		options:   opts,
		cachePool: newCachePool(opts.MaxOpenFiles),
		blocks:    NewBlockCache(opts.BlockCacheCapacity),
	}
	runtime.SetFinalizer(c, (*Cache).finalize)
	return c
}
