package table

import (
	"io"
	"runtime"
	"sync"

	"github.com/kezhuw/leveldb/internal/table/block"
)

var emptyBlock block.Block

type blockNode struct {
	fileNumber uint64
	offset     uint64
	b          *block.Block
	err        error
	wg         sync.WaitGroup
	next       *blockNode
	prev       *blockNode
}

type blockNodeList struct {
	size int
	root blockNode
}

func (l *blockNodeList) init() {
	l.size = 0
	l.root.next = &l.root
	l.root.prev = &l.root
}

func (l *blockNodeList) insertAfter(n *blockNode, at *blockNode) {
	n.next = at.next
	n.next.prev = n
	at.next = n
	n.prev = at
}

func (l *blockNodeList) PushBack(n *blockNode) {
	l.insertAfter(n, l.root.prev)
	l.size += n.b.Len()
}

func (l *blockNodeList) unlink(n *blockNode) {
	n.next.prev = n.prev
	n.prev.next = n.next
}

func (l *blockNodeList) Remove(n *blockNode) *blockNode {
	l.unlink(n)
	l.size -= n.b.Len()
	return n
}

func (l *blockNodeList) RemoveFront() *blockNode {
	n := l.root.next
	l.unlink(n)
	l.size -= n.b.Len()
	return n
}

func (l *blockNodeList) Size() int {
	return l.size
}

func (l *blockNodeList) MoveToBack(n *blockNode) {
	l.unlink(n)
	l.insertAfter(n, l.root.prev)
}

var deletedBlockNode = new(blockNode)

type fileNode map[uint64]*blockNode

type blockCachePool struct {
	mu    sync.Mutex
	files map[uint64]fileNode

	cap     int
	nodes   blockNodeList
	evicts  chan fileNode
	updates chan *blockNode
}

func (p *blockCachePool) touchNode(n *blockNode) {
	select {
	case p.updates <- n:
	default:
	}
}

func (p *blockCachePool) updateNode(n *blockNode) {
	switch n.next {
	case nil:
		p.nodes.PushBack(n)
		for p.nodes.Size() > p.cap {
			n := p.nodes.RemoveFront()
			p.mu.Lock()
			delete(p.files[n.fileNumber], n.offset)
			p.mu.Unlock()
			n.next = deletedBlockNode
			n.prev = nil
		}
	case deletedBlockNode:
	default:
		p.nodes.MoveToBack(n)
	}
}

func (p *blockCachePool) removeNode(n *blockNode) {
	switch n.next {
	case nil:
		n.next = deletedBlockNode
	case deletedBlockNode:
	default:
		p.nodes.Remove(n)
		n.next = deletedBlockNode
		n.prev = nil
	}
}

func (p *blockCachePool) collect() {
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
		case nodes := <-evicts:
			for _, n := range nodes {
				p.removeNode(n)
			}
		}
	}
}

func (p *blockCachePool) release() {
	close(p.updates)
}

type BlockCache struct {
	*blockCachePool
}

func (c *BlockCache) Evict(fileNumber uint64) {
	c.mu.Lock()
	nodes := c.files[fileNumber]
	delete(c.files, fileNumber)
	c.mu.Unlock()
	if nodes != nil {
		c.evicts <- nodes
	}
}

func (c *BlockCache) load(r io.ReaderAt, h block.Handle, verifyChecksums bool, n *blockNode) (*block.Block, error) {
	defer n.wg.Done()
	n.b, n.err = ReadDataBlock(r, n.fileNumber, h, verifyChecksums)
	if n.b == nil {
		n.b = &emptyBlock
	}
	return n.b, n.err
}

func (c *BlockCache) nonFillRead(r io.ReaderAt, fileNumber uint64, h block.Handle, verifyChecksums bool) (*block.Block, error) {
	c.mu.Lock()
	n := c.files[fileNumber][h.Offset]
	if n == nil {
		c.mu.Unlock()
		return ReadDataBlock(r, fileNumber, h, verifyChecksums)
	}
	c.mu.Unlock()
	n.wg.Wait()
	c.touchNode(n)
	return n.b, n.err
}

func (c *BlockCache) Read(r io.ReaderAt, fileNumber uint64, h block.Handle, verifyChecksums bool, dontFill bool) (*block.Block, error) {
	if dontFill {
		return c.nonFillRead(r, fileNumber, h, verifyChecksums)
	}
	c.mu.Lock()
	f := c.files[fileNumber]
	n := f[h.Offset]
	switch {
	case f == nil:
		f = make(map[uint64]*blockNode)
		c.files[fileNumber] = f
		fallthrough
	case n == nil:
		n = new(blockNode)
		f[h.Offset] = n
		n.wg.Add(1)
		c.mu.Unlock()
		defer c.touchNode(n)
		return c.load(r, h, verifyChecksums, n)
	}
	c.mu.Unlock()
	n.wg.Wait()
	c.touchNode(n)
	return n.b, n.err
}

func (c *BlockCache) finalize() {
	c.blockCachePool.release()
}

func NewBlockCache(cap int) *BlockCache {
	p := &blockCachePool{
		cap:     cap,
		files:   make(map[uint64]fileNode),
		evicts:  make(chan fileNode, 16),
		updates: make(chan *blockNode, 1024),
	}
	go p.collect()
	c := &BlockCache{blockCachePool: p}
	runtime.SetFinalizer(c, (*BlockCache).finalize)
	return c
}
