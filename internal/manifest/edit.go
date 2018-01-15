package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/util"
)

// Tag numbers for fields of Edit.
const (
	tagComparatorName = 1
	tagLogNumber      = 2
	tagNextFileNumber = 3
	tagLastSequence   = 4
	tagCompactPointer = 5
	tagDeletedFile    = 6
	tagNewFile        = 7
	tagPrevLogNumber  = 9
)

var (
	ErrCorruptEditTag            = errors.New("corrupt version edit: tag")
	ErrCorruptEditLogNumber      = errors.New("corrupt version edit: log number")
	ErrCorruptEditPrevLogNumber  = errors.New("corrupt version edit: prev log number")
	ErrCorruptEditNextFileNumber = errors.New("corrupt version edit: next file number")
	ErrCorruptEditLastSequence   = errors.New("corrupt version edit: last sequence")
	ErrCorruptEditCompactPointer = errors.New("corrupt version edit: compact pointer")
	ErrCorruptEditDeletedFile    = errors.New("corrupt version edit: deleted file")
	ErrCorruptEditNewFile        = errors.New("corrupt version edit: new file")
	ErrCorruptEditComparatorName = errors.New("corrupt version edit: comparator name")
)

// LevelFileMeta includes a file's meta data and its corresponding level.
type LevelFileMeta struct {
	Level int
	*FileMeta
}

type LevelFileNumber struct {
	Level  int
	Number uint64
}

type LevelCompactPointer struct {
	Level   int
	Largest keys.InternalKey
}

// Edit records a sequence changes made to a version. New version can
// be constructed from old version and an Edit which records changs made
// to that version.
type Edit struct {
	ComparatorName  string
	LogNumber       uint64
	NextFileNumber  uint64
	LastSequence    keys.Sequence
	CompactPointers []LevelCompactPointer
	AddedFiles      []LevelFileMeta
	DeletedFiles    []LevelFileNumber
	scratch         [binary.MaxVarintLen64]byte
}

func (edit *Edit) String() string {
	var s string
	if edit.ComparatorName != "" {
		s += fmt.Sprintf("ComparatorName: %q\n", edit.ComparatorName)
	}
	if edit.LogNumber != 0 {
		s += fmt.Sprintf("LogNumber: %d\n", edit.LogNumber)
	}
	if edit.NextFileNumber != 0 {
		s += fmt.Sprintf("NextFileNumber: %d\n", edit.NextFileNumber)
	}
	if edit.LastSequence != 0 {
		s += fmt.Sprintf("LastSequence: %d\n", edit.LastSequence)
	}
	for _, pointer := range edit.CompactPointers {
		s += fmt.Sprintf("CompacitionPointer at level %d: %q\n", pointer.Level, pointer.Largest)
	}
	for _, f := range edit.DeletedFiles {
		s += fmt.Sprintf("level %d: delete file %d.\n", f.Level, f.Number)
	}
	for _, f := range edit.AddedFiles {
		s += fmt.Sprintf("level %d: add file %d: size %d, smallest key: %q, largest key: %q\n", f.Level, f.Number, f.Size, f.Smallest, f.Largest)
	}
	return s
}

// Reset clears all set fields.
func (edit *Edit) Reset() {
	edit.ComparatorName = ""
	edit.LogNumber = 0
	edit.NextFileNumber = 0
	edit.LastSequence = 0
	edit.CompactPointers = edit.CompactPointers[:0]
	edit.AddedFiles = edit.AddedFiles[:0]
	edit.DeletedFiles = edit.DeletedFiles[:0]
}

// Encode appends binary encoded Edit to buf.
func (edit *Edit) Encode(buf []byte) []byte {
	if edit.ComparatorName != "" {
		buf = edit.appendUvarint(buf, tagComparatorName)
		buf = edit.appendLengthPrefixedString(buf, edit.ComparatorName)
	}
	if edit.LogNumber != 0 {
		buf = edit.appendTagAndUint64(buf, tagLogNumber, edit.LogNumber)
	}
	if edit.NextFileNumber != 0 {
		buf = edit.appendTagAndUint64(buf, tagNextFileNumber, edit.NextFileNumber)
	}
	if edit.LastSequence != 0 {
		buf = edit.appendTagAndUint64(buf, tagLastSequence, uint64(edit.LastSequence))
	}
	for _, p := range edit.CompactPointers {
		buf = edit.appendUvarint(buf, tagCompactPointer)
		buf = edit.appendUvarint(buf, uint64(p.Level))
		buf = edit.appendLengthPrefixedBytes(buf, p.Largest)
	}
	for _, file := range edit.DeletedFiles {
		buf = edit.appendUvarint(buf, tagDeletedFile)
		buf = edit.appendUvarint(buf, uint64(file.Level))
		buf = edit.appendUvarint(buf, file.Number)
	}
	for _, file := range edit.AddedFiles {
		buf = edit.appendUvarint(buf, tagNewFile)
		buf = edit.appendUvarint(buf, uint64(file.Level))
		buf = edit.appendUvarint(buf, file.Number)
		buf = edit.appendUvarint(buf, file.Size)
		buf = edit.appendLengthPrefixedBytes(buf, file.Smallest)
		buf = edit.appendLengthPrefixedBytes(buf, file.Largest)
	}
	return buf
}

// Decode decodes an Edit from buf.
func (edit *Edit) Decode(buf []byte) (err error) {
	defer util.CatchError(&err)
	var tag uint64
	var prevLogNumber uint64
	for len(buf) != 0 {
		buf = edit.decodeUint64(buf, &tag, ErrCorruptEditTag)
		switch tag {
		case tagComparatorName:
			buf = edit.decodeComparatorName(buf)
		case tagLogNumber:
			buf = edit.decodeLogNumber(buf)
		case tagNextFileNumber:
			buf = edit.decodeNextFileNumber(buf)
		case tagLastSequence:
			buf = edit.decodeLastSequence(buf)
		case tagCompactPointer:
			buf = edit.decodeCompactPointer(buf)
		case tagDeletedFile:
			buf = edit.decodeDeletedFile(buf)
		case tagNewFile:
			buf = edit.decodeAddedFile(buf)
		case tagPrevLogNumber:
			buf = edit.decodeUint64(buf, &prevLogNumber, ErrCorruptEditPrevLogNumber)
		default:
			return fmt.Errorf("unknown version edit tag: %d", tag)
		}
	}
	return nil
}

func (edit *Edit) appendUvarint(buf []byte, x uint64) []byte {
	n := binary.PutUvarint(edit.scratch[:], x)
	return append(buf, edit.scratch[:n]...)
}

func (edit *Edit) appendTagAndUint64(buf []byte, tag, x uint64) []byte {
	buf = edit.appendUvarint(buf, tag)
	return edit.appendUvarint(buf, x)
}

func (edit *Edit) appendLengthPrefixedBytes(buf []byte, b []byte) []byte {
	buf = edit.appendUvarint(buf, uint64(len(b)))
	return append(buf, b...)
}

func (edit *Edit) appendLengthPrefixedString(buf []byte, s string) []byte {
	buf = edit.appendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func (edit *Edit) decodeUint64(buf []byte, u *uint64, err error) []byte {
	x, n := binary.Uvarint(buf)
	if n <= 0 {
		panic(err)
	}
	*u = x
	return buf[n:]
}

func (edit *Edit) decodeBytes(buf []byte, ptr *[]byte, err error) []byte {
	l, n := binary.Uvarint(buf)
	if n <= 0 {
		panic(err)
	}
	buf = buf[n:]
	if l > uint64(len(buf)) {
		panic(err)
	}
	*ptr = util.DupBytes(buf[:l])
	return buf[l:]
}

func (edit *Edit) decodeLogNumber(buf []byte) []byte {
	return edit.decodeUint64(buf, &edit.LogNumber, ErrCorruptEditLogNumber)
}

func (edit *Edit) decodeNextFileNumber(buf []byte) []byte {
	return edit.decodeUint64(buf, &edit.NextFileNumber, ErrCorruptEditNextFileNumber)
}

func (edit *Edit) decodeLastSequence(buf []byte) []byte {
	return edit.decodeUint64(buf, (*uint64)(&edit.LastSequence), ErrCorruptEditLastSequence)
}

func (edit *Edit) decodeComparatorName(buf []byte) []byte {
	var b []byte
	buf = edit.decodeBytes(buf, &b, ErrCorruptEditComparatorName)
	edit.ComparatorName = string(b)
	return buf
}

func (edit *Edit) decodeCompactPointer(buf []byte) []byte {
	var level uint64
	buf = edit.decodeUint64(buf, &level, ErrCorruptEditCompactPointer)
	if level > configs.NumberLevels {
		panic(fmt.Errorf("too big level in compact pointer: %d", level))
	}
	var ikey []byte
	buf = edit.decodeBytes(buf, &ikey, ErrCorruptEditCompactPointer)
	if len(ikey) < keys.TagBytes {
		panic(ErrCorruptEditCompactPointer)
	}
	edit.CompactPointers = append(edit.CompactPointers, LevelCompactPointer{Level: int(level), Largest: keys.InternalKey(ikey)})
	return buf
}

func (edit *Edit) decodeDeletedFile(buf []byte) []byte {
	var level, fileNumber uint64
	buf = edit.decodeUint64(buf, &level, ErrCorruptEditDeletedFile)
	if level > configs.NumberLevels {
		panic(fmt.Errorf("too big level in deleted file: %d", level))
	}
	buf = edit.decodeUint64(buf, &fileNumber, ErrCorruptEditDeletedFile)
	edit.DeletedFiles = append(edit.DeletedFiles, LevelFileNumber{Level: int(level), Number: fileNumber})
	return buf
}

func (edit *Edit) decodeAddedFile(buf []byte) []byte {
	var level uint64
	buf = edit.decodeUint64(buf, &level, ErrCorruptEditNewFile)
	if level > configs.NumberLevels {
		panic(fmt.Errorf("too big level in new file: %d", level))
	}
	file := LevelFileMeta{FileMeta: &FileMeta{}}
	buf = edit.decodeUint64(buf, &file.Number, ErrCorruptEditNewFile)
	buf = edit.decodeUint64(buf, &file.Size, ErrCorruptEditNewFile)
	buf = edit.decodeBytes(buf, (*[]byte)(&file.Smallest), ErrCorruptEditNewFile)
	buf = edit.decodeBytes(buf, (*[]byte)(&file.Largest), ErrCorruptEditNewFile)
	file.Level = int(level)
	edit.AddedFiles = append(edit.AddedFiles, file)
	return buf
}
