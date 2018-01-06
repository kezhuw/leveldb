package iterator

// Direction defines direction of iterator.
type Direction int

const (
	// Forward states that iterator is in forward iterating.
	Forward Direction = iota
	// Reverse states that iterator is in backward iterating.
	Reverse
)
