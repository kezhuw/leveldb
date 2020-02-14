package leveldb

// Reader provides an unified interface to read database and its snapshot.
type Reader interface {
	Get(key []byte, opts *ReadOptions) ([]byte, error)

	All(opts *ReadOptions) Iterator

	Find(start []byte, opts *ReadOptions) Iterator

	Range(start, limit []byte, opts *ReadOptions) Iterator

	Prefix(prefix []byte, opts *ReadOptions) Iterator

	Snapshot() *Snapshot
}
