// +build atomic

package memtable

import (
	"sync/atomic"
	"unsafe"
)

func (n *node) Next(i int) *node {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&n.nexts[i]))
	return (*node)(atomic.LoadPointer(addr))
}

func (n *node) SetNext(i int, next *node) {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&n.nexts[i]))
	atomic.StorePointer(addr, unsafe.Pointer(next))
}

type rwmutex struct{}

func (*rwmutex) Lock()    {}
func (*rwmutex) Unlock()  {}
func (*rwmutex) RLock()   {}
func (*rwmutex) RUnlock() {}
