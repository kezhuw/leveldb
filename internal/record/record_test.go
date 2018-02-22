package record

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"
)

type writeCase struct {
	Name      string
	BlockSize int
	Slices    [][]byte
}

var fixedWriteCases = []writeCase{
	{Name: "fixed_test1", BlockSize: 2048, Slices: [][]byte{bytes.Repeat([]byte("abc"), 120), bytes.Repeat([]byte("cedf"), 87234)}},
	{Name: "fixed_test2", BlockSize: 0, Slices: [][]byte{[]byte("abcdef"), []byte("xy932j")}},
}

var randomWriteCases []writeCase

type writeLens struct {
	Name      string
	BlockSize int
	Lens      []int
}

var randomWriteLens = []writeLens{
	{Name: "random_test1", BlockSize: 512, Lens: []int{1000, 97270, 8000}},
	{Name: "random_test2", BlockSize: 2 * defaultBlockSize, Lens: []int{132, 485, 3293, 8888, 12390, 23453, 22222, 234}},
	{Name: "random_test3", BlockSize: 9999, Lens: []int{102400, 800, 9000}},
}

func init() {
	rand.Seed(time.Now().UnixNano())
	cases := make([]writeCase, len(randomWriteLens))
	for i, c := range randomWriteLens {
		slices := make([][]byte, len(c.Lens))
		for j, n := range c.Lens {
			slices[j] = make([]byte, n)
			rand.Read(slices[j])
		}
		cases[i].Name = c.Name
		cases[i].Slices = slices
	}
	randomWriteCases = cases
}

func testWriteCases(t *testing.T, cases []writeCase) {
	var buf bytes.Buffer
	var err error
	var record []byte
	for _, c := range cases {
		buf.Reset()

		w := newWriter(&buf, c.BlockSize, 0)
		for i, b := range c.Slices {
			err := w.Write(b)
			if err != nil {
				t.Fatalf("case[%s:%d]: block size: %d, writing error: %s", c.Name, i, c.BlockSize, err)
			}
		}

		r := newReader(&buf, c.BlockSize)
		for i := 0; true; i++ {
			record, err = r.AppendRecord(record[:0])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("case[%s:%d]: block size: %d, reading error: %s", c.Name, i, c.BlockSize, err)
			}
			if !bytes.Equal(record, c.Slices[i]) {
				t.Fatalf("case[%s:%d]: block size: %d, read bytes not equal to written", c.Name, i, c.BlockSize)
			}
		}
	}
}

func TestRandomWrite(t *testing.T) {
	testWriteCases(t, randomWriteCases)
}

func TestFixedWrite(t *testing.T) {
	testWriteCases(t, fixedWriteCases)
}
