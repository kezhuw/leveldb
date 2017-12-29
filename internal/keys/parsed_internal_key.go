package keys

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

func (k *ParsedInternalKey) Tag() Tag {
	return PackTag(k.Sequence, k.Kind)
}
