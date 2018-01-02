package keys

type Comparator interface {
	Name() string
	Compare(a, b []byte) int
	AppendSuccessor(dst, start, limit []byte) []byte
}

type UserComparator interface {
	Comparator
	MakePrefixSuccessor(start []byte) []byte
}

type InternalComparator struct {
	UserKeyComparator UserComparator
}

func (cmp *InternalComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

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
