package iterator

// Next moves iterator to next if it is valid, otherwise move to its first.
func Next(it Iterator) bool {
	if it.Valid() {
		return it.Next()
	}
	return it.First()
}
