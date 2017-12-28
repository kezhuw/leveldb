package keys

import "fmt"

type Sequence uint64

const (
	MaxSequence Sequence = (1 << 56) - 1
)

func (seq Sequence) Add(n uint64) Sequence {
	return seq + Sequence(n)
}

func ToInternalKey(key []byte) (InternalKey, bool) {
	if len(key) < TagBytes {
		return nil, false
	}
	return key, true
}

func MakeInternalKey(buf []byte, key []byte, seq Sequence, kind Kind) InternalKey {
	copy(buf, key)
	CombineTag(buf[len(key):], seq, kind)
	return InternalKey(buf)
}

func NewInternalKey(key []byte, seq Sequence, kind Kind) InternalKey {
	buf := make([]byte, len(key)+TagBytes)
	return MakeInternalKey(buf, key, seq, kind)
}

type InternalKey []byte

func (ikey InternalKey) Dup() InternalKey {
	dup := make([]byte, len(ikey))
	copy(dup, ikey)
	return dup
}

func (ikey InternalKey) UserKey() []byte {
	i := len(ikey) - TagBytes
	return ikey[:i:i]
}

func (ikey InternalKey) Tag() uint64 {
	i := len(ikey) - TagBytes
	return GetTag(ikey[i:])
}

func (ikey InternalKey) Split() ([]byte, Sequence, Kind) {
	i := len(ikey) - TagBytes
	tag := GetTag(ikey[i:])
	return ikey[:i:i], Sequence(tag >> kindBits), Kind(tag & 0xFF)
}

func (ikey InternalKey) Split2() ([]byte, Sequence) {
	i := len(ikey) - TagBytes
	return ikey[:i:i], Sequence(GetTag(ikey[i:]))
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
	var buf [TagBytes]byte
	CombineTag(buf[:], k.Sequence, k.Kind)
	dst = append(dst, k.UserKey...)
	return append(dst, buf[:]...)
}

func (k *ParsedInternalKey) Parse(key []byte) bool {
	i := len(key) - TagBytes
	if i < 0 {
		return false
	}
	k.UserKey = key[:i:i]
	k.Sequence, k.Kind = ExtractTag(key[i:])
	return k.Kind <= maxKind
}

func (k *ParsedInternalKey) Tag() uint64 {
	return PackTag(k.Sequence, k.Kind)
}
