package table

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/files"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
)

type node struct {
	f    uint64
	t    *Table
	n    *node
	err  error
	wg   sync.WaitGroup
	next *node
	prev *node
	refs int32
}

var releasedNode = &node{}

func (n *node) Retain() *node {
	atomic.AddInt32(&n.refs, 1)
	return n
}

func (n *node) Release() error {
	if atomic.AddInt32(&n.refs, -1) == 0 && n.t != nil {
		return n.t.Close()
	}
	return nil
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
	evicts  chan uint64
	updates chan *node
}

func newCachePool(cap int) *cachePool {
	p := &cachePool{
		tables:  make(map[uint64]*node),
		cap:     cap,
		evicts:  make(chan uint64, 512),
		updates: make(chan *node, 512),
	}
	go p.collect()
	return p
}

func (p *cachePool) Close() error {
	close(p.updates)
	return nil
}

var deletedNode = new(node)

func (p *cachePool) touchNode(n *node) {
	select {
	case p.updates <- n:
	default:
	}
}

func (p *cachePool) getNode(fileNumber uint64) *node {
	p.mu.Lock()
	n := p.tables[fileNumber]
	p.mu.Unlock()
	return n
}

func (p *cachePool) releaseNode(n *node) {
	p.mu.Lock()
	delete(p.tables, n.f)
	p.mu.Unlock()
	n.next = deletedNode
	n.prev = nil
	n.Release()
}

func (p *cachePool) purgeNodes() {
	for p.nodes.Len() > 0 {
		n := p.nodes.RemoveFront()
		p.releaseNode(n)
	}
}

func (p *cachePool) updateNode(n *node) {
	switch n.next {
	case nil:
		p.nodes.PushBack(n)
		if p.nodes.Len() > p.cap {
			n := p.nodes.RemoveFront()
			p.releaseNode(n)
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
				p.purgeNodes()
				return
			}
			p.updateNode(n)
		case fileNumber := <-evicts:
			n := p.getNode(fileNumber)
			if n != nil {
				p.removeNode(n)
				p.releaseNode(n)
			}
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

func (c *Cache) openTable(fileNumber, fileSize uint64) (*Table, error) {
	tableName := files.TableFileName(c.dbname, fileNumber)
	tableFile, err := c.fs.Open(tableName, os.O_RDONLY)
	if os.IsNotExist(err) {
		tableName = files.SSTTableFileName(c.dbname, fileNumber)
		tableFile, err = c.fs.Open(tableName, os.O_RDONLY)
	}
	if err != nil {
		return nil, err
	}
	return OpenTable(tableFile, c.blocks, c.options, fileNumber, fileSize)
}

func (c *Cache) loadNode(fileNumber, fileSize uint64, n *node) (*node, error) {
	defer n.wg.Done()
	n.t, n.err = c.openTable(fileNumber, fileSize)
	if n.err == nil {
		n.n = n
	}
	return n.n.Retain(), n.err
}

func (c *Cache) load(fileNumber, fileSize uint64) (*node, error) {
	c.mu.Lock()
	n := c.tables[fileNumber]
	if n != nil {
		c.mu.Unlock()
		n.wg.Wait()
		c.touchNode(n)
		return n.n.Retain(), n.err
	}
	n = &node{f: fileNumber, n: releasedNode, refs: 1}
	c.tables[fileNumber] = n
	n.wg.Add(1)
	c.mu.Unlock()
	defer c.touchNode(n)
	return c.loadNode(fileNumber, fileSize, n)
}

func (c *Cache) Evict(fileNumber uint64) {
	c.evicts <- fileNumber
}

func (c *Cache) Get(fileNumber uint64, fileSize uint64, ikey keys.InternalKey, opts *options.ReadOptions) ([]byte, error, bool) {
	n, err := c.load(fileNumber, fileSize)
	if err != nil {
		return nil, err, true
	}
	defer n.Release()
	return n.t.Get(ikey, opts)
}

func (c *Cache) NewIterator(fileNumber uint64, fileSize uint64, opts *options.ReadOptions) iterator.Iterator {
	n, err := c.load(fileNumber, fileSize)
	if err != nil {
		return iterator.Error(err)
	}
	it := n.t.NewIterator(opts)
	return iterator.WithCleanup(it, n.Release)
}

func (c *Cache) Close() error {
	return c.cachePool.Close()
}

func NewCache(dbname string, opts *options.Options) *Cache {
	c := &Cache{
		dbname:    dbname,
		fs:        opts.FileSystem,
		options:   opts,
		cachePool: newCachePool(opts.MaxOpenFiles),
		blocks:    NewBlockCache(opts.BlockCacheCapacity),
	}
	return c
}
