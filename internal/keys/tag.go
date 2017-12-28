package keys

import "github.com/kezhuw/leveldb/internal/endian"

const (
	// TagBytes is the number of bytes tag occupied in internal key.
	TagBytes = 8

	// kindBits is the number of bits kind occupied in tag.
	kindBits = 8
)

// PackTag packs sequence and kind to tag.
func PackTag(seq Sequence, kind Kind) uint64 {
	return (uint64(seq) << kindBits) | uint64(kind)
}

// UnpackTag unpacks tag to sequence and kind.
func UnpackTag(tag uint64) (Sequence, Kind) {
	return Sequence(tag >> kindBits), Kind(tag & 0xff)
}

// PutTag puts tag into buf. Length of buf must equal to or greater than TagBytes.
func PutTag(buf []byte, tag uint64) {
	endian.PutUint64(buf, tag)
}

// GetTag gets tag from buf. Length of buf must equal to or greater than TagBytes.
func GetTag(buf []byte) uint64 {
	return endian.Uint64(buf)
}

// CombineTag combines sequenece and kind into buf. Length of buf must equal to
// or greater than TagBytes.
func CombineTag(buf []byte, seq Sequence, kind Kind) {
	PutTag(buf, PackTag(seq, kind))
}

// ExtractTag extracts sequence and kind from buf. Length of buf must equal to
// or greater than TagBytes.
func ExtractTag(buf []byte) (Sequence, Kind) {
	return UnpackTag(GetTag(buf))
}
