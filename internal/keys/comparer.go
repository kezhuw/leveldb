package keys

// Comparer is a interface for algorithms to compare bytes.
type Comparer interface {
	// Compare returns a value 'less than', 'equal to' or 'greater than' 0 depending
	// on whether a is 'less than', 'equal to' or 'greater than' b.
	Compare(a, b []byte) int
}

// Max returns the key which is greater than or equal to the other.
func Max(cmp Comparer, a, b []byte) []byte {
	if cmp.Compare(a, b) > 0 {
		return a
	}
	return b
}

// Min returns the key which is less than or equal to the other.
func Min(cmp Comparer, a, b []byte) []byte {
	if cmp.Compare(a, b) < 0 {
		return a
	}
	return b
}
