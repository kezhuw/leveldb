package compaction

import (
	"fmt"
	"sort"
)

const (
	// UnlimitedCompactionConcurrency is a cap for Registry which enforce
	// no concurrent compaction limitation.
	UnlimitedCompactionConcurrency = -1
)

// Registration represents an registered compaction.
type Registration struct {
	level          int
	target         int
	NextFileNumber uint64
}

func (r *Registration) overlap(level, target int) bool {
	return !(level > r.target || target < r.level)
}

type byLevel []*Registration

var _ sort.Interface = (byLevel)(nil)

func (s byLevel) Len() int {
	return len(s)
}

func (s byLevel) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byLevel) Less(i, j int) bool {
	return s[i].level < s[j].level
}

// Registry records all ongoing compaction registration.
type Registry struct {
	cap           int
	corrupts      int
	registrations []*Registration
}

func (registry *Registry) overlap(level, target int) bool {
	for _, r := range registry.registrations {
		if r.overlap(level, target) {
			return true
		}
	}
	return false
}

// Recap reset max concurrent compactions allowed. If cap <= 0, there will be
// no such limitation. This limitation doesn't cancel existing compactions,
// it just prevent future compaction registration to beyond this limitation.
func (registry *Registry) Recap(cap int) {
	registry.cap = cap
}

// Concurrency returns the number of concurrent ongoing compactions currently.
func (registry *Registry) Concurrency() int {
	return len(registry.registrations) - registry.corrupts
}

// NextFileNumber returns minimum NextFileNumber among all compactions
// registered or defaultValue if there is no ongoing compaction.
func (registry *Registry) NextFileNumber(defaultValue uint64) uint64 {
	if registry.Concurrency() == 0 {
		return defaultValue
	}
	var nextFileNumber uint64
	for _, r := range registry.registrations {
		if r.NextFileNumber == 0 {
			panic(fmt.Sprintf("zero next file number for compaction registration level %d, target %d", r.level, r.target))
		}
		if nextFileNumber == 0 || r.NextFileNumber < nextFileNumber {
			nextFileNumber = r.NextFileNumber
		}
	}
	return nextFileNumber
}

// Register tries to register a new compaction for given level range
// [level, target], it returns true if this compaction doesn't conflict
// with existing registrations, false otherwise.
func (registry *Registry) Register(level, target int) *Registration {
	if (registry.cap > 0 && registry.Concurrency() >= registry.cap) || registry.overlap(level, target) {
		return nil
	}
	r := &Registration{
		level:  level,
		target: target,
	}
	registry.registrations = append(registry.registrations, r)
	return r
}

// ExpandTo tries to expand target level of a existing compaction at given
// level to target. It panic if there is no such compaction.
func (registry *Registry) ExpandTo(level, target int) int {
	sort.Sort((byLevel)(registry.registrations))
	for i, n := 0, len(registry.registrations); i < n; i++ {
		r := registry.registrations[i]
		if r.level == level {
			switch j := i + 1; j {
			case n:
				r.target = target
			default:
				if maxLevel := registry.registrations[j].level - 1; target > maxLevel {
					target = maxLevel
				}
				r.target = target
			}
			return r.target
		}
	}
	panic(fmt.Sprintf("try to expand nonexistent compaction from level %d to %d", level, target))
}

// Complete removes a existing compaction registration for given level.
// It panic if there is no such compaction.
func (registry *Registry) Complete(level int) {
	for i, r := range registry.registrations {
		if r.level == level {
			last := len(registry.registrations) - 1
			registry.registrations[i], registry.registrations[last] = registry.registrations[last], registry.registrations[i]
			registry.registrations = registry.registrations[:last]
			return
		}
	}
	panic(fmt.Sprintf("try to complete nonexistent compaction for level %d", level))
}

// Corrupt marks compaction for given level as corrupted. Corrupted compaction
// doesn't contribute to concurrency, but it doest contribute to NextFileNumber.
func (registry *Registry) Corrupt(level int) {
	registry.corrupts++
}
