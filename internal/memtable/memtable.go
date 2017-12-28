package memtable

import (
	"math/rand"

	"github.com/kezhuw/leveldb/internal/batch"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
)

const (
	maxHeight = 12
)

const (
	bytesBlockSize = 4096
	nextsBlockSize = 1024
	nodesBlockSize = 512

	largeBytesSize = 1024
)

type node struct {
	ikey  []byte
	value []byte // Nil if deleted.
	nexts []*node
}

type MemTable struct {
	rnd *rand.Rand

	height int
	head   *node
	prevs  [maxHeight]*node
	icmp   *keys.InternalComparator

	usage int
	bytes []byte
	nexts []*node
	nodes []node

	mutex rwmutex
}

func (m *MemTable) allocBytes(n int) []byte {
	m.usage += n
	len, cap := len(m.bytes), cap(m.bytes)
	size := len + n
	if size <= cap {
		m.bytes = m.bytes[:size]
		return m.bytes[len:size:size]
	}
	if n >= largeBytesSize {
		return make([]byte, n)
	}
	m.bytes = make([]byte, n, n+bytesBlockSize)
	return m.bytes[0:n:n]
}

func (m *MemTable) allocNexts(n int) []*node {
	i, j := len(m.nexts), cap(m.nexts)
	if k := i + n; k <= j {
		m.nexts = m.nexts[:k]
		return m.nexts[i:k]
	}
	m.nexts = make([]*node, n, n+nextsBlockSize)
	return m.nexts
}

func (m *MemTable) allocNode() *node {
	if i := len(m.nodes); i < cap(m.nodes) {
		m.nodes = m.nodes[:i+1]
		return &m.nodes[i]
	}
	m.nodes = make([]node, 1, 1+nodesBlockSize)
	return &m.nodes[0]
}

func (m *MemTable) newNode(h int) *node {
	n := m.allocNode()
	n.nexts = m.allocNexts(h)
	return n
}

func (m *MemTable) randomHeight() int {
	h := 1
	for h < maxHeight && m.rnd.Intn(4) == 0 {
		h++
	}
	return h
}

func (m *MemTable) findLast() *node {
	p := m.head
	for h := m.height - 1; h >= 0; h-- {
		for {
			n := p.Next(h)
			if n == nil {
				break
			}
			p = n
		}
	}
	if p == m.head {
		return nil
	}
	return p
}

func (m *MemTable) findLessThan(ikey []byte) *node {
	p := m.head
	for h := m.height - 1; h >= 0; h-- {
		for {
			n := p.Next(h)
			if n == nil {
				break
			}
			if m.icmp.Compare(n.ikey, ikey) >= 0 {
				break
			}
			p = n
		}
	}
	if p == m.head {
		return nil
	}
	return p
}

func (m *MemTable) findGreaterOrEqual(ikey []byte, prevs []*node) (n *node, hit bool) {
	p := m.head
	for h := m.height - 1; h >= 0; h-- {
		n = p.Next(h)
		for n != nil {
			if r := m.icmp.Compare(ikey, n.ikey); r <= 0 {
				hit = r == 0
				break
			}
			p, n = n, n.Next(h)
		}
		prevs[h] = p
	}
	return
}

func (m *MemTable) ApproximateMemoryUsage() int {
	return m.usage
}

func (m *MemTable) Empty() bool {
	return m.head.Next(0) == nil
}

func (m *MemTable) Batch(seq keys.Sequence, items []batch.Item) {
	for _, item := range items {
		switch item.Value {
		case nil:
			m.Add(seq, keys.Delete, item.Key, nil)
		default:
			m.Add(seq, keys.Value, item.Key, item.Value)
		}
		seq++
	}
}

func (m *MemTable) Add(seq keys.Sequence, kind keys.Kind, key, value []byte) {
	b := m.allocBytes(len(key) + keys.TagBytes + len(value))
	ikey := b[:len(key)+keys.TagBytes]
	keys.MakeInternalKey(ikey, key, seq, kind)

	prevs := m.prevs[:]
	_, hit := m.findGreaterOrEqual(ikey, prevs)
	if hit {
		panic("duplicated key in MemTable")
	}

	n := m.allocNode()
	n.ikey = ikey
	n.value = b[len(ikey):]
	switch kind {
	case keys.Delete:
		n.value = nil
	default:
		copy(n.value, value)
	}

	h := m.randomHeight()
	n.nexts = m.allocNexts(h)

	if m.height < h {
		for i := m.height; i < h; i++ {
			prevs[i] = m.head
		}
		m.height = h
	}

	m.mutex.Lock()
	for i := 0; i < h; i++ {
		n.nexts[i] = prevs[i].nexts[i]
		prevs[i].SetNext(i, n)
	}
	m.mutex.Unlock()
}

func (m *MemTable) Get(ikey keys.InternalKey) (value []byte, err error, ok bool) {
	var prevs [maxHeight]*node
	m.mutex.RLock()
	n, hit := m.findGreaterOrEqual(ikey, prevs[:])
	m.mutex.RUnlock()
	if n != nil && (hit || m.icmp.UserKeyComparator.Compare(ikey.UserKey(), keys.InternalKey(n.ikey).UserKey()) == 0) {
		if n.value == nil {
			err = errors.ErrNotFound
		}
		return n.value, err, true
	}
	return nil, nil, false
}

func (m *MemTable) NewIterator() iterator.Iterator {
	return &memtableIterator{m: m}
}

func New(icmp *keys.InternalComparator) *MemTable {
	m := &MemTable{
		rnd:    rand.New(rand.NewSource(rand.Int63())),
		height: 1,
		icmp:   icmp,
	}
	m.head = m.newNode(maxHeight)
	return m
}
