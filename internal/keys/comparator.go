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
