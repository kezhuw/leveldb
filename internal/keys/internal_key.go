package keys

// InternalKey is a combination of user key, which comes from APIs such as
// DB.Get, DB.Put, etc. and Tag which is also a combination of Sequence and Kind.
type InternalKey []byte

// ToInternalKey converts key to InternalKey.
func ToInternalKey(key []byte) (InternalKey, bool) {
	n := len(key)
	if n < TagBytes {
		return nil, false
	}
	return key[:n:n], true
}

// MakeInternalKey makes and stores a InternalKey which combines from key,
// seq and kind in buf. Make sure length of buf is equal to or greater than
// len(key) plus TagBytes.
func MakeInternalKey(buf []byte, key []byte, seq Sequence, kind Kind) InternalKey {
	n := len(key)
	copy(buf, key)
	CombineTag(buf[n:], seq, kind)
	n += TagBytes
	return InternalKey(buf[:n:n])
}

// NewInternalKey creates a InternalKey from combination of key, seq and kind.
func NewInternalKey(key []byte, seq Sequence, kind Kind) InternalKey {
	buf := make([]byte, len(key)+TagBytes)
	return MakeInternalKey(buf, key, seq, kind)
}

// Dup creates a InternalKey from this one.
func (ikey InternalKey) Dup() InternalKey {
	dup := make([]byte, len(ikey))
	copy(dup, ikey)
	return dup
}

// UserKey returns key part of internal key.
func (ikey InternalKey) UserKey() []byte {
	i := len(ikey) - TagBytes
	return ikey[:i:i]
}

// Tag returns tag part of internal key.
func (ikey InternalKey) Tag() Tag {
	i := len(ikey) - TagBytes
	return GetTag(ikey[i:])
}

// Split splits this internal key to user key, sequence and kind parts.
func (ikey InternalKey) Split() ([]byte, Sequence, Kind) {
	i := len(ikey) - TagBytes
	seq, kind := ExtractTag(ikey[i:])
	return ikey[:i:i], seq, kind
}
