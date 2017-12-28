package keys

import (
	"fmt"

	"github.com/kezhuw/leveldb/internal/endian"
)

type Sequence uint64

const (
	MaxSequence Sequence = (1 << 56) - 1
)

func (seq Sequence) Add(n uint64) Sequence {
	return seq + Sequence(n)
}

const (
	TagLen = 8
)

func PutTag(buf []byte, seq Sequence, kind Kind) {
	tag := PackTag(seq, kind)
	endian.PutUint64(buf, tag)
}

func PackTag(seq Sequence, kind Kind) uint64 {
	return (uint64(seq) << 8) | uint64(kind)
}

func ToInternalKey(key []byte) (InternalKey, bool) {
	if len(key) < TagLen {
		return nil, false
	}
	return key, true
}

func MakeInternalKey(buf []byte, key []byte, seq Sequence, kind Kind) InternalKey {
	copy(buf, key)
	PutTag(buf[len(key):], seq, kind)
	return InternalKey(buf)
}

func NewInternalKey(key []byte, seq Sequence, kind Kind) InternalKey {
	buf := make([]byte, len(key)+8)
	return MakeInternalKey(buf, key, seq, kind)
}

type InternalKey []byte

func (ikey InternalKey) Dup() InternalKey {
	dup := make([]byte, len(ikey))
	copy(dup, ikey)
	return dup
}

func (ikey InternalKey) UserKey() []byte {
	i := len(ikey) - 8
	return ikey[:i:i]
}

func (ikey InternalKey) Tag() uint64 {
	i := len(ikey) - 8
	return endian.Uint64(ikey[i:])
}

func (ikey InternalKey) Split() ([]byte, Sequence) {
	i := len(ikey) - 8
	return ikey[:i:i], Sequence(endian.Uint64(ikey[i:]))
}

func (ikey InternalKey) Split3() ([]byte, Sequence, Kind) {
	i := len(ikey) - 8
	seq := endian.Uint64(ikey[i:])
	return ikey[:i:i], Sequence(seq >> 8), Kind(seq & 0xFF)
}

type Kind int

const (
	Delete  = 0
	Value   = 1
	maxKind = Value
	Seek    = maxKind
)

func (k Kind) String() string {
	switch k {
	case Value:
		return "value setting"
	case Delete:
		return "value deletion"
	}
	return fmt.Sprintf("kind: %d", k)
}

type ParsedInternalKey struct {
	UserKey  []byte
	Kind     Kind
	Sequence Sequence
}

func (k *ParsedInternalKey) Append(dst []byte) []byte {
	var buf [8]byte
	endian.PutUint64(buf[:], k.Tag())
	dst = append(dst, k.UserKey...)
	return append(dst, buf[:]...)
}

func (k *ParsedInternalKey) Parse(key []byte) bool {
	i := len(key) - 8
	if i < 0 {
		return false
	}
	tag := endian.Uint64(key[i:])
	k.UserKey = key[:i:i]
	k.Kind = Kind(tag & 0xff)
	k.Sequence = Sequence(tag >> 8)
	return k.Kind <= maxKind
}

func (k *ParsedInternalKey) Tag() uint64 {
	return (uint64(k.Sequence) << 8) | uint64(k.Kind)
}
