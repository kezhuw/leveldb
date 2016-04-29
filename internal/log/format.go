package log

import "github.com/kezhuw/leveldb/internal/crc"

// Block types.
const (
	zeroBlock   = 0
	fullBlock   = 1
	firstBlock  = 2
	middleBlock = 3
	lastBlock   = 4

	numBlockTypes = 5
)

const (
	defaultBlockSize = 32 * 1024 // 32KiB
	headerSize       = 7         // bytes
)

type header [headerSize]byte

var trailer header

var typeChecksums [numBlockTypes]crc.CRC

func init() {
	var buf [1]byte
	for i := 0; i < len(typeChecksums); i++ {
		buf[0] = byte(i)
		typeChecksums[i] = crc.New(buf[:])
	}
}
