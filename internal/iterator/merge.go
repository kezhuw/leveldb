package iterator

import (
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/util"
)

type mergeIterator struct {
	cmp       keys.Comparer
	seeked    bool
	err       error
	direction Direction
	index     int // index of current iterator in iterators
	current   Iterator
	// During iteration, iterators are split to two categories: valid ones
	// and invalid ones. All iterators which reach their ends in iteration
	// order are considered invalid and store at indices from len(iterators)-1
	// to cap(iterators)-1. All valid iterators except current one point to
	// entries behind current key and hence untouched by caller.
	iterators []Iterator
}

func (m *mergeIterator) First() bool {
	i, n := 0, cap(m.iterators)
	iterators := m.iterators[:n]
	for i < n {
		if !iterators[i].First() {
			if err := iterators[i].Err(); err != nil {
				m.err = err
				m.current = nil
				return false
			}
			n--
			iterators[n], iterators[i] = iterators[i], iterators[n]
			continue
		}
		i++
	}
	m.seeked = true
	m.err = nil
	m.direction = Forward
	m.iterators = iterators[:i]
	m.current, m.index = m.findSmallest()
	return m.Valid()
}

func (m *mergeIterator) Last() bool {
	i, n := 0, cap(m.iterators)
	iterators := m.iterators[:n]
	for i < n {
		if !iterators[i].Last() {
			if err := iterators[i].Err(); err != nil {
				m.err = err
				m.current = nil
				return false
			}
			n--
			iterators[n], iterators[i] = iterators[i], iterators[n]
			continue
		}
		i++
	}
	m.seeked = true
	m.err = nil
	m.direction = Reverse
	m.iterators = iterators[:i]
	m.current, m.index = m.findLargest()
	return m.Valid()
}

func (m *mergeIterator) Seek(key []byte) bool {
	i, n := 0, cap(m.iterators)
	iterators := m.iterators[:n]
	for i < n {
		if !iterators[i].Seek(key) {
			if err := iterators[i].Err(); err != nil {
				m.err = err
				m.current = nil
				return false
			}
			n--
			iterators[n], iterators[i] = iterators[i], iterators[n]
			continue
		}
		i++
	}
	m.seeked = true
	m.err = nil
	m.direction = Forward
	m.iterators = iterators[:i]
	m.current, m.index = m.findSmallest()
	return m.Valid()
}

func (m *mergeIterator) Next() bool {
	switch {
	case !m.seeked:
		return m.First()
	case m.current == nil:
		return false
	case m.direction == Reverse:
		if err := m.forward(); err != nil {
			m.err = err
			m.current = nil
			return false
		}
	}
	if !m.current.Next() {
		if err := m.current.Err(); err != nil {
			m.err = err
			m.current = nil
			return false
		}
		last := len(m.iterators) - 1
		m.iterators[last], m.iterators[m.index] = m.iterators[m.index], m.iterators[last]
		m.iterators = m.iterators[:last]
	}
	m.current, m.index = m.findSmallest()
	return m.Valid()
}

func (m *mergeIterator) Prev() bool {
	switch {
	case !m.seeked:
		return m.Last()
	case m.current == nil:
		return false
	case m.direction == Forward:
		if err := m.reverse(); err != nil {
			m.err = err
			m.current = nil
			return false
		}
	}
	if !m.current.Prev() {
		if err := m.current.Err(); err != nil {
			m.err = err
			m.current = nil
			return false
		}
		last := len(m.iterators) - 1
		m.iterators[last], m.iterators[m.index] = m.iterators[m.index], m.iterators[last]
		m.iterators = m.iterators[:last]
	}
	m.current, m.index = m.findLargest()
	return m.Valid()
}

func (m *mergeIterator) Valid() bool {
	return m.current != nil
}

func (m *mergeIterator) Key() []byte {
	if m.Valid() {
		return m.current.Key()
	}
	return nil
}

func (m *mergeIterator) Value() []byte {
	if m.Valid() {
		return m.current.Value()
	}
	return nil
}

func (m *mergeIterator) Err() error {
	return m.err
}

func (m *mergeIterator) Close() error {
	err := m.err
	m.iterators = m.iterators[:cap(m.iterators)]
	for _, it := range m.iterators {
		err = util.FirstError(err, it.Close())
	}
	m.err = err
	m.current = nil
	m.iterators = nil
	return err
}

func (m *mergeIterator) forward() error {
	// We are in situation that:
	// * For valid iterator, its current entry must point at or before key.
	//   Its next entry, if any, must point at or after key.
	// * For invalid iterator, its first entry, if any, must point at or after key.
	i, n := 0, cap(m.iterators)
	iterators := m.iterators[:n]
	for i < n {
		it := iterators[i]
		valid := it.Valid()
		switch {
		case it == m.current:
			fallthrough
		case valid && it.Next():
			fallthrough
		case !valid && it.First():
			i++
		default:
			if err := it.Err(); err != nil {
				return err
			}
			n--
			iterators[n], iterators[i] = iterators[i], iterators[n]
			if n == m.index {
				m.index = i
			}
		}
	}
	m.direction = Forward
	m.iterators = iterators[:n]
	return nil
}

func (m *mergeIterator) reverse() error {
	// We are in situation that:
	// * For valid iterator, its current entry must point at or after key.
	//   Its previous entry, if any, must point at or before key.
	// * For invalid iterator, its last entry, if any, must point at or before key.
	i, n := 0, cap(m.iterators)
	iterators := m.iterators[:n]
	for i < n {
		it := iterators[i]
		valid := it.Valid()
		switch {
		case it == m.current:
			fallthrough
		case valid && it.Prev():
			fallthrough
		case !valid && it.Last():
			i++
		default:
			if err := it.Err(); err != nil {
				return err
			}
			n--
			iterators[n], iterators[i] = iterators[i], iterators[n]
			if n == m.index {
				m.index = i
			}
		}
	}
	m.direction = Reverse
	m.iterators = iterators[:n]
	return nil
}

func (m *mergeIterator) findSmallest() (Iterator, int) {
	n := len(m.iterators)
	if n == 0 {
		return nil, -1
	}
	index := 0
	current := m.iterators[0]
	smallest := current.Key()
	for i, it := range m.iterators[1:] {
		if m.cmp.Compare(it.Key(), smallest) < 0 {
			index = i + 1
			current = it
			smallest = it.Key()
		}
	}
	return current, index
}

func (m *mergeIterator) findLargest() (Iterator, int) {
	n := len(m.iterators)
	if n == 0 {
		return nil, -1
	}
	index := 0
	current := m.iterators[0]
	largest := current.Key()
	for i, it := range m.iterators[1:] {
		if m.cmp.Compare(it.Key(), largest) > 0 {
			index = i + 1
			current = it
			largest = it.Key()
		}
	}
	return current, index
}

// NewMergeIterator creates a iterator merging all entries from iterators.
// The resulting iterator implements semantics the exported Iterator interface
// defines. This means that if Next/Prev is the first seek method called, they
// act as First/Last respectively. If equal keys exist in different iterators,
// it is undefined and hence unstable which entry is retrieved first among
// different iterations.
func NewMergeIterator(cmp keys.Comparer, iterators ...Iterator) Iterator {
	n := len(iterators)
	iterators = append([]Iterator(nil), iterators...)
	return &mergeIterator{cmp: cmp, iterators: iterators[:n:n]}
}
