package table

const (
	// leading 64 bits of `echo http://code.google.com/p/leveldb/ | sha1sum`
	magicNumber      = 0xdb4775248b80fb57
	blockTrailerSize = 5
)
