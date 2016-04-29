package memtable

import "sync"

type rwmutex struct {
	sync.RWMutex
}

func (n *node) Next(i int) *node {
	return n.nexts[i]
}

func (n *node) SetNext(i int, next *node) {
	n.nexts[i] = next
}
