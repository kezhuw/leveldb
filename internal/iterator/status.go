package iterator

// Status defines statuses for iterator.
type Status int

const (
	// Initial is the initial status of iterator.
	Initial Status = iota
	// Invalid specifies that iterator encounters an error which can be
	// retrieved through Err().
	Invalid
	// Closed specifies that iterator has been closed.
	Closed
	// Valid specifies that iterator has entry retrieved in its Key/Value.
	Valid
)
