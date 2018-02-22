package manifest

import (
	"io"

	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/file"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/record"
)

type builder struct {
	Scratch        []byte
	Comparator     *keys.InternalComparator
	ManifestFile   file.File
	ManifestNumber uint64

	LogNumber      uint64
	NextFileNumber uint64
	LastSequence   keys.Sequence
}

func (b *builder) Build(v *Version) (int64, error) {
	var edit Edit
	r := record.NewReader(b.ManifestFile)
	buf := b.Scratch
	comparatorName := b.Comparator.UserKeyComparator.Name()
	var err error
	for {
		buf, err = r.AppendRecord(buf[:0])
		switch err {
		case nil:
		case io.EOF:
			goto done
		case record.ErrIncompleteRecord:
			offset := r.Offset()
			b.ManifestFile.Truncate(offset)
			b.ManifestFile.Seek(offset, io.SeekStart)
			goto done
		default:
			return 0, err
		}
		edit.Reset()
		if err := edit.Decode(buf); err != nil {
			return 0, err
		}
		if edit.ComparatorName != "" && edit.ComparatorName != comparatorName {
			return 0, errors.ErrComparatorMismatch
		}
		if err := v.apply(&edit); err != nil {
			return 0, err
		}
		if edit.LogNumber != 0 {
			b.LogNumber = edit.LogNumber
		}
		if edit.NextFileNumber != 0 {
			b.NextFileNumber = edit.NextFileNumber
		}
		if edit.LastSequence != 0 {
			b.LastSequence = edit.LastSequence
		}
	}
done:
	b.Scratch = buf
	v.computeCompactionScore()
	return r.Offset(), nil
}
