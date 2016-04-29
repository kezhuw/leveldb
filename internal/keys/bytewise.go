package keys

import (
	"bytes"
	"fmt"
)

type bytewiseComparator struct{}

func (bytewiseComparator) Name() string {
	return "leveldb.BytewiseComparator"
}

func (bytewiseComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (bytewiseComparator) MakePrefixSuccessor(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c != 0xff {
			limit := make([]byte, i+1)
			copy(limit[:i], prefix)
			limit[i] = c + 1
			return limit
		}
	}
	return nil
}

func (bytewiseComparator) AppendSuccessor(dst, start, limit []byte) []byte {
	i := sharedPrefixLen(start, limit)
	n := len(dst)
	dst = append(dst, start...)
	if len(limit) > 0 {
		switch {
		case i == len(start): // start is a prefix of limit
			return dst
		case i == len(limit) || start[i] > limit[i]:
			panic(fmt.Sprintf("leveldb: BytewiseComparator.AppendSuccessor: limit[%v] is less than start[%v]", limit, start))
		case start[i]+1 < limit[i]: // `start[i]+1` will not overflow one byte, because start[i] is less than some value of one byte.
			n += i
			dst[n] = start[i] + 1
			return dst[:n+1]
		default:
			i++
		}
	}
	for ; i < len(start); i++ {
		if start[i] != 0xff {
			n += i
			dst[n] = start[i] + 1
			return dst[:n+1]
		}
	}
	return dst
}

func sharedPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

var BytewiseComparator UserComparator = bytewiseComparator{}
