package keys

type Sequence uint64

const (
	MaxSequence Sequence = (1 << 56) - 1
)

func (seq Sequence) Add(n uint64) Sequence {
	return Sequence(uint64(seq) + n)
}
