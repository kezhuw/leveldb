package keys

type InternalKey []byte

func ToInternalKey(key []byte) (InternalKey, bool) {
	n := len(key)
	if n < TagBytes {
		return nil, false
	}
	return key[:n:n], true
}

func MakeInternalKey(buf []byte, key []byte, seq Sequence, kind Kind) InternalKey {
	n := len(key)
	copy(buf, key)
	CombineTag(buf[n:], seq, kind)
	n += TagBytes
	return InternalKey(buf[:n:n])
}

func NewInternalKey(key []byte, seq Sequence, kind Kind) InternalKey {
	buf := make([]byte, len(key)+TagBytes)
	return MakeInternalKey(buf, key, seq, kind)
}

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
	seq, kind := ExtractTag(ikey[i:])
	return ikey[:i:i], seq, kind
}

func (ikey InternalKey) Split2() ([]byte, Sequence) {
	i := len(ikey) - TagBytes
	return ikey[:i:i], Sequence(GetTag(ikey[i:]))
}
