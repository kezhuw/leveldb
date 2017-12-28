package keys

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
