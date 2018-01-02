package keys

// ParsedInternalKey is a parsed or splited internal representation.
type ParsedInternalKey struct {
	UserKey  []byte
	Kind     Kind
	Sequence Sequence
}

// Append appends this internal key to destination buffer.
func (k *ParsedInternalKey) Append(dst []byte) []byte {
	var buf [TagBytes]byte
	CombineTag(buf[:], k.Sequence, k.Kind)
	dst = append(dst, k.UserKey...)
	return append(dst, buf[:]...)
}

// Parse parses input key as internal key and returns true for valid internal
// key. It is illegal to access other fields and methods after returning false.
func (k *ParsedInternalKey) Parse(key []byte) bool {
	i := len(key) - TagBytes
	if i < 0 {
		return false
	}
	k.UserKey = key[:i:i]
	k.Sequence, k.Kind = ExtractTag(key[i:])
	return k.Kind <= maxKind
}

// Tag returns tag of this internal key.
func (k *ParsedInternalKey) Tag() Tag {
	return PackTag(k.Sequence, k.Kind)
}
