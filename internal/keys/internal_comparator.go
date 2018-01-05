package keys

// InternalComparator is a comparator used to compare byte slices of internal keys.
type InternalComparator struct {
	UserKeyComparator UserComparator
}

// Name implements Comparator.Name.
func (cmp *InternalComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

// Compare implements Comparer.Compare and Comparator.Compare.
// It orders internal keys by ascending user key and descending sequence and
// kind. For given internal keys A with parts (Akey, Asequence, Akind) and B
// with (Bkey, Bsequence, Bkind), if A < B, we have:
//   * Akey < Bkey, otherwise Akey == Bkey and:
//   * Asequence > Bsequence, otherwise Asequence == Bsequence and:
//   * Akind > Bkind.
func (cmp *InternalComparator) Compare(a, b []byte) int {
	userKeyA := InternalKey(a).UserKey()
	userKeyB := InternalKey(b).UserKey()
	r := cmp.UserKeyComparator.Compare(userKeyA, userKeyB)
	if r == 0 {
		tagA := InternalKey(a).Tag()
		tagB := InternalKey(b).Tag()
		switch {
		case tagA > tagB:
			return -1
		case tagA < tagB:
			return 1
		}
	}
	return r
}

// AppendSuccessor implements Comparator.AppendSuccessor.
func (cmp *InternalComparator) AppendSuccessor(dst, start, limit []byte) []byte {
	ustart := InternalKey(start).UserKey()
	switch len(limit) {
	case 0:
		dst = cmp.UserKeyComparator.AppendSuccessor(dst, ustart, nil)
	default:
		ulimit := InternalKey(limit).UserKey()
		dst = cmp.UserKeyComparator.AppendSuccessor(dst, ustart, ulimit)
	}
	return append(dst, start[len(ustart):]...)
}

var _ Comparator = (*InternalComparator)(nil)
