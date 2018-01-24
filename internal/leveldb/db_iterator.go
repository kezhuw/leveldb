package leveldb

import (
	"math/rand"
	"runtime"

	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/manifest"
)

type dbIterator struct {
	db *DB
	// Keep it away from GC, this way level files iterator seeks in
	// wouldn't got deleted due to umount from manifest.
	base     *manifest.Version
	ucmp     keys.Comparator
	sequence keys.Sequence

	err       error
	status    iterator.Status
	direction iterator.Direction
	iterator  iterator.Iterator

	// The last key we read from underlying iterator, used as
	// current key for this iterator.
	lastKey   []byte
	lastValue []byte
	parsedKey keys.ParsedInternalKey

	rnd         *rand.Rand
	sampleBytes int

	tag        [8]byte // packed sequence and keys.Seek
	keyScratch [256]byte
}

func (it *dbIterator) First() bool {
	it.err = nil
	it.direction = iterator.Forward
	if !it.iterator.First() {
		it.status = iterator.Invalid
		return false
	}
	it.status = iterator.Valid
	return it.findNextEntry(false)
}

func (it *dbIterator) Last() bool {
	it.err = nil
	it.direction = iterator.Reverse
	if !it.iterator.Last() {
		it.err = it.iterator.Err()
		it.status = iterator.Invalid
		return false
	}
	it.status = iterator.Valid
	return it.findPrevEntry()
}

func (it *dbIterator) Seek(key []byte) bool {
	it.err = nil
	it.lastKey = append(it.lastKey[:0], key...)
	it.lastKey = append(it.lastKey, it.tag[:]...)
	if !it.iterator.Seek(it.lastKey) {
		it.err = it.iterator.Err()
		it.status = iterator.Invalid
		return false
	}
	it.status = iterator.Valid
	it.direction = iterator.Forward
	return it.findNextEntry(false)
}

func (it *dbIterator) Next() bool {
	switch {
	case it.status == iterator.Initial:
		return it.First()
	case it.status != iterator.Valid:
		return false
	case it.direction == iterator.Reverse:
		// Two possible situations exist:
		//
		// * Iterator points at the last entry which has a user key less than
		//   current one.
		// * Iterator is invalid, current key has the smallest user key in iterator.
		//
		// In either case, there may be some large sequence incarnations before current
		// key, but it doesn't matter since we will ignore them all.
		//
		// So we can simply advance to the next or first entry, which has same
		// user key as current, to reverse the direction.
		if !iterator.Next(it.iterator) {
			it.err = it.iterator.Err()
			it.status = iterator.Invalid
			return false
		}
		it.direction = iterator.Forward
		fallthrough
	default:
		return it.findNextEntry(true)
	}
}

func (it *dbIterator) Prev() bool {
	switch {
	case it.status == iterator.Initial:
		return it.Last()
	case it.status != iterator.Valid:
		return false
	case it.direction == iterator.Forward:
		// Iterator is pointing at current key which has valid
		// sequence(<= it.sequence) and Kind keys.Value. There is no
		// possibility that previous valid entry has same user key as
		// current, if that, it should shadow current one.
		it.iterator.Prev()
		it.direction = iterator.Reverse
		fallthrough
	default:
		return it.findPrevEntry()
	}
}

func (it *dbIterator) Valid() bool {
	return it.status == iterator.Valid
}

func (it *dbIterator) Key() []byte {
	return it.lastKey
}

func (it *dbIterator) Value() []byte {
	switch it.direction {
	case iterator.Forward:
		return it.iterator.Value()
	default:
		return it.lastValue
	}
}

func (it *dbIterator) Err() error {
	return it.err
}

func (it *dbIterator) finalize() error {
	if it.status == iterator.Closed {
		return nil
	}
	it.status = iterator.Closed
	it.db = nil
	it.base = nil
	it.iterator = nil
	return it.Err()
}

func (it *dbIterator) Release() error {
	runtime.SetFinalizer(it, nil)
	return it.finalize()
}

func (it *dbIterator) findNextEntry(skip bool) bool {
	ikey := &it.parsedKey
	for {
		if !ikey.Parse(it.iterator.Key()) {
			it.err = errors.ErrCorruptInternalKey
			it.status = iterator.Invalid
			return false
		}
		if ikey.Sequence <= it.sequence {
			switch ikey.Kind {
			case keys.Delete:
				it.lastKey = append(it.lastKey[:0], ikey.UserKey...)
				skip = true
			case keys.Value:
				if skip && it.ucmp.Compare(ikey.UserKey, it.lastKey) <= 0 {
					break
				}
				it.lastKey = append(it.lastKey[:0], ikey.UserKey...)
				return true
			default:
				it.err = errors.ErrCorruptInternalKey
				it.status = iterator.Invalid
				return false
			}
		}
		if !it.iterator.Next() {
			it.err = it.iterator.Err()
			it.status = iterator.Invalid
			return false
		}
	}
}

func (it *dbIterator) randomSampleBytes() int {
	return it.rnd.Intn(2 * it.db.options.IterationBytesPerSampleSeek)
}

func (it *dbIterator) parseKey(ikey *keys.ParsedInternalKey) bool {
	key := it.iterator.Key()
	if !ikey.Parse(key) {
		it.err = errors.ErrCorruptInternalKey
		it.status = iterator.Invalid
		return false
	}
	it.sampleBytes -= len(key) + len(it.iterator.Value())
	for it.sampleBytes < 0 {
		it.sampleBytes += it.randomSampleBytes()
		seekOverlapFile := it.db.manifest.Version().SeekOverlap(key, nil)
		if seekOverlapFile.FileMeta != nil {
			it.db.tryCompactFile(seekOverlapFile)
		}
	}
	return true
}

func (it *dbIterator) findPrevEntry() bool {
	if !it.iterator.Valid() {
		it.err = it.iterator.Err()
		it.status = iterator.Invalid
		return false
	}
	ikey := &it.parsedKey
	skip := true
	it.status = iterator.Invalid
	for {
		if !ikey.Parse(it.iterator.Key()) {
			return false
		}
		if ikey.Sequence <= it.sequence {
			if !skip && it.ucmp.Compare(ikey.UserKey, it.lastKey) < 0 {
				return true
			}
			switch {
			case ikey.Kind == keys.Delete:
				skip = true
				it.status = iterator.Invalid
			case ikey.Kind == keys.Value:
				skip = false
				it.status = iterator.Valid
				it.lastKey = append(it.lastKey[:0], ikey.UserKey...)
				it.lastValue = append(it.lastValue[:0], it.iterator.Value()...)
			default:
				it.err = errors.ErrCorruptInternalKey
				it.status = iterator.Invalid
				return false
			}
		}
		if !it.iterator.Prev() {
			// If there is an error, current key may be
			// shadowed by previous entry.
			if err := it.iterator.Err(); err != nil {
				it.err = err
				it.status = iterator.Invalid
				return false
			}
			return skip == false
		}
	}
}

func newDBIterator(db *DB, base *manifest.Version, seq keys.Sequence, it iterator.Iterator) iterator.Iterator {
	rnd := rand.New(rand.NewSource(rand.Int63()))
	dbIt := &dbIterator{
		db:       db,
		base:     base,
		ucmp:     db.options.Comparator.UserKeyComparator,
		iterator: it,
		sequence: seq,
		rnd:      rnd,
	}
	runtime.SetFinalizer(dbIt, (*dbIterator).finalize)
	keys.CombineTag(dbIt.tag[:], seq, keys.Seek)
	dbIt.lastKey = dbIt.keyScratch[:0]
	dbIt.sampleBytes = dbIt.randomSampleBytes()
	return dbIt
}
