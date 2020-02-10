package keys

import "github.com/kezhuw/leveldb/internal/endian"

const (
	// TagBytes is the number of bytes tag occupied in internal key.
	TagBytes = 8

	// kindBits is the number of bits kind occupied in tag.
	kindBits = 8
)

// Tag is a combination of Sequence and Kind. This combination has properties:
//
// * Tag with big Sequence is greater than tag with small Sequence.
// * With same Sequences, tag with Kind Seek is equal to or greater than other tags.
//
// Thus for a combination with given Sequence and Kind Seek, we can say that:
// * All keys with tag greater than this combination are written after that
//   given Sequence.
// * All keys with tag equal to or less than this combination are written before
//   or at that given Sequence.
//
// Also, due to the fact that no keys written to LevelDB have same Sequence,
// sometimes Tags are used as Sequences to determine ordering of written.
type Tag uint64

// PackTag packs sequence and kind to tag.
func PackTag(seq Sequence, kind Kind) Tag {
	return Tag((uint64(seq) << kindBits) | uint64(kind))
}

// UnpackTag unpacks tag to sequence and kind.
func UnpackTag(tag Tag) (Sequence, Kind) {
	return Sequence(tag >> kindBits), Kind(tag & 0xff)
}

// PutTag puts tag into buf. Length of buf must equal to or greater than TagBytes.
func PutTag(buf []byte, tag Tag) {
	endian.PutUint64(buf, uint64(tag))
}

// GetTag gets tag from buf. Length of buf must equal to or greater than TagBytes.
func GetTag(buf []byte) Tag {
	return Tag(endian.Uint64(buf))
}

// CombineTag combines sequence and kind into buf. Length of buf must equal to
// or greater than TagBytes.
func CombineTag(buf []byte, seq Sequence, kind Kind) {
	PutTag(buf, PackTag(seq, kind))
}

// ExtractTag extracts sequence and kind from buf. Length of buf must equal to
// or greater than TagBytes.
func ExtractTag(buf []byte) (Sequence, Kind) {
	return UnpackTag(GetTag(buf))
}
