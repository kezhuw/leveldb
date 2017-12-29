package keys

// Sequence is a monotonic value associated with every keys written to LevelDB.
//
// At any time, once a key is written to LevelDB, no matter whether it is a key
// deletion or value setting, it is associated with a Sequence greater than
// Sequences associated to previous written keys.
//
// For this we can conclude:
// * No keys have some Sequence.
// * For key with given Sequence, it is written before keys with bigger
//   Sequences, also it is written after keys with smaller Sequences.
type Sequence uint64

const (
	// MaxSequence is the maximum allowed value for Sequence.
	MaxSequence Sequence = (1 << 56) - 1
)

// Next returns a Sequence which is n greater than this one.
func (seq Sequence) Next(n uint64) Sequence {
	return Sequence(uint64(seq) + n)
}
