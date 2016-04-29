package block

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/kezhuw/leveldb/internal/endian"
	"github.com/kezhuw/leveldb/internal/keys"
)

var ErrCorruptBlock = errors.New("leveldb: corrupt block")

type panicError struct {
	err error
}

type reader struct {
	comparer keys.Comparer

	contents       []byte
	restartsOffset uint32
	restartsNumber uint32

	err            error
	currentKey     []byte
	currentOffset  uint32
	currentRestart uint32

	lastValueStart uint32
	// Also is the next entry offset.
	lastValueLimit uint32
}

func (r *reader) getRestartPoint(i uint32) uint32 {
	if i == r.restartsNumber {
		return r.restartsOffset
	}
	offset := r.restartsOffset + 4*i
	return endian.Uint32(r.contents[offset : offset+4])
}

func readPrefix(buf []byte) (n, shared, keyEnd, valueEnd uint32) {
	shared = uint32(buf[0])
	nonShared := uint32(buf[1])
	valueLength := uint32(buf[2])
	if (shared | nonShared | valueLength) < 128 {
		return 3, shared, 3 + nonShared, 3 + nonShared + valueLength
	}
	shared64, i0 := binary.Uvarint(buf)
	if i0 <= 0 {
		return
	}
	nonShared64, i1 := binary.Uvarint(buf[i0:])
	if i1 <= 0 {
		return
	}
	valueLength64, i2 := binary.Uvarint(buf[i0+i1:])
	if i2 <= 0 {
		return
	}
	n = uint32(i0 + i1 + i2)
	return n, uint32(shared64), n + uint32(nonShared64), n + uint32(nonShared64+valueLength64)
}

func (r *reader) Valid() bool {
	return r.currentOffset < r.restartsOffset
}

func (r *reader) Key() []byte {
	return r.currentKey
}

func (r *reader) Value() []byte {
	return r.contents[r.lastValueStart:r.lastValueLimit]
}

func (r *reader) First() bool {
	r.err = nil
	r.moveToRestartPoint(0)
	return r.nextKey()
}

func (r *reader) moveToRestartPoint(i uint32) {
	r.currentKey = r.currentKey[:0]
	r.currentRestart = i
	r.lastValueLimit = r.getRestartPoint(i)
}

func (r *reader) Last() bool {
	r.err = nil
	r.moveToRestartPoint(r.restartsNumber - 1)
	for r.nextKey() && r.lastValueLimit < r.restartsOffset {
	}
	return r.Valid()
}

func (r *reader) invalidate(err error) {
	r.err = err
	r.currentOffset = r.restartsOffset
	r.currentRestart = r.restartsNumber
}

func (r *reader) catchPanicError() {
	if e := recover(); e != nil {
		if e, ok := e.(panicError); ok {
			r.invalidate(e.err)
			return
		}
		panic(e)
	}
}

func (r *reader) Seek(target []byte) bool {
	defer r.catchPanicError()
	i := sort.Search(int(r.restartsNumber), func(i int) bool {
		index := uint32(i)
		restart := r.getRestartPoint(index)
		sentinel := r.getRestartPoint(index + 1)
		n, shared, keyEnd, _ := readPrefix(r.contents[restart:sentinel])
		if n <= 0 || shared != 0 || restart+keyEnd > sentinel {
			panic(panicError{ErrCorruptBlock})
		}
		key := r.contents[restart+n : restart:keyEnd]
		return r.comparer.Compare(key, target) > 0
	})
	if i != 0 {
		i--
	}
	r.moveToRestartPoint(uint32(i))
	for r.nextKey() {
		if r.comparer.Compare(r.Key(), target) >= 0 {
			break
		}
	}
	return r.Valid()
}

func (r *reader) Next() bool {
	return r.nextKey()
}

func (r *reader) Prev() bool {
	current := r.currentOffset
	restart := r.currentRestart
	if r.getRestartPoint(restart) == current {
		if restart == 0 {
			r.currentOffset = r.restartsOffset
			r.currentRestart = r.restartsNumber
			return false
		}
		restart--
		r.currentRestart = restart
	}
	r.moveToRestartPoint(restart)
	for r.nextEntry(r.lastValueLimit, current) && r.lastValueLimit < current {
	}
	return r.Valid()
}

func (r *reader) nextEntry(current, sentinel uint32) bool {
	n, shared, keyEnd, valueEnd := readPrefix(r.contents[current:sentinel])
	switch {
	case n <= 0 || uint32(len(r.currentKey)) < shared:
		fallthrough
	case current+valueEnd > sentinel:
		r.err = ErrCorruptBlock
		r.currentOffset = r.restartsOffset
		return false
	}
	r.currentKey = r.currentKey[:shared]
	r.currentKey = append(r.currentKey, r.contents[current+n:current+keyEnd]...)
	r.currentOffset = current
	r.lastValueStart = current + keyEnd
	r.lastValueLimit = current + valueEnd
	return true
}

func (r *reader) nextKey() bool {
	nextRestart := r.currentRestart + 1
	current := r.lastValueLimit
	sentinel := r.getRestartPoint(nextRestart)
	if current == sentinel {
		if current == r.restartsOffset {
			r.currentOffset = r.restartsOffset
			r.currentRestart = r.restartsNumber
			return false
		}
		r.currentRestart = nextRestart
		nextRestart++
		sentinel = r.getRestartPoint(nextRestart)
	}
	return r.nextEntry(current, sentinel)
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Release() error {
	r.contents = nil
	return r.err
}
