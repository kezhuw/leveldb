package keys

import "fmt"

// Kind expresses usage of the ambient internal key.
type Kind int

const (
	// XXX Don't change those values, together with user keys there are
	// persisted to files.

	// Delete represents deletion of this key.
	Delete = 0
	// Value represents value setting of this key.
	Value   = 1
	maxKind = Value

	// Seek is maximum(Value, Delete), which is a valid Kind and
	// serves as start point for keys with same sequence.
	//
	// See InternalComparator.Compare for ordering among internal keys.
	Seek = maxKind
)

func (k Kind) String() string {
	switch k {
	case Delete:
		return "value deletion"
	case Value:
		return "value setting"
	}
	return fmt.Sprintf("unknown kind: %d", k)
}
