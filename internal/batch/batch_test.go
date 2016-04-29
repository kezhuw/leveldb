package batch_test

import (
	"testing"

	"github.com/kezhuw/leveldb/internal/batch"
	"github.com/kezhuw/leveldb/internal/keys"
)

type writeCase struct {
	Kind       keys.Kind
	Key, Value string
}

type batchCase struct {
	Seq    keys.Sequence
	Writes []writeCase
}

var bunchCases = map[string]batchCase{
	"test1": {
		23293238,
		[]writeCase{
			{keys.Value, "aaaaa", "bbbbb"},
			{keys.Value, "", "xcadf"},
			{keys.Value, "", ""},
			{keys.Delete, "jlkoijl", "xjoij"},
			{keys.Value, "bnnnnnn", "\x00\x93\x32"},
			{keys.Value, "\xab\xc3", "dasfdfs"},
			{keys.Delete, "aaaaa", ""},
		},
	},
	"test2": {
		9993232,
		[]writeCase{
			{keys.Delete, "nkj89fdans", "xnonlkjojn"},
			{keys.Value, "nlkfsdjiolk", "fnsdalkjil"},
			{keys.Value, "\x93\x0a\xc3", "mbiojfasf"},
		},
	},
}

func addBatchWrites(t *testing.T, b *batch.Batch, name string, cases []writeCase) {
	k := b.Count()
	for i, c := range cases {
		switch c.Kind {
		case keys.Value:
			b.Put([]byte(c.Key), []byte(c.Value))
		case keys.Delete:
			b.Delete([]byte(c.Key))
		default:
			t.Fatalf("%s(%d): unknown keys.Kind: %s\n", name, i, c.Kind)
		}
	}
	if n := b.Count() - k; n != uint32(len(cases)) {
		t.Fatalf("%s: write %d cases, got count: %d", name, len(cases), n)
	}
}

func buildBatch(t *testing.T, name string, seq keys.Sequence, cases []writeCase) batch.Batch {
	var b batch.Batch
	addBatchWrites(t, &b, name, cases)
	b.SetSequence(seq)
	if got := b.Sequence(); got != seq {
		t.Fatalf("%s: expect sequence: %d, got: %d", seq, got)
	}
	return b
}

func TestBatchSplit(t *testing.T) {
	for name, cases := range bunchCases {
		b := buildBatch(t, name, cases.Seq, cases.Writes)
		seq, items, ok := b.Split(nil)
		if !ok {
			t.Errorf("%s: fail to split batch", name)
		}
		if seq != cases.Seq {
			t.Errorf("%s: expect sequence: %d, got: %d", name, cases.Seq, seq)
		}
		if len(cases.Writes) != len(items) {
			t.Errorf("%s: split %d cases to %d items", name, len(cases.Writes), len(items))
			continue
		}
		for i, c := range cases.Writes {
			if c.Key != string(items[i].Key) {
				t.Errorf("%s(%d): expect key: %q, got: %q", name, i, c.Key, items[i].Key)
			}
			switch c.Kind {
			case keys.Value:
				if c.Value != string(items[i].Value) {
					t.Errorf("%s(%d): key: %q, expect value: %q, got: %q", name, i, c.Key, c.Value, items[i].Value)
				}
			case keys.Delete:
				if items[i].Value != nil {
					t.Errorf("%s(%d): deleted key %q, got value: %q", name, i, c.Key, items[i].Value)
				}
			default:
				t.Errorf("%s(%d): unknown keys.Kind: %s, key: %s.\n", name, i, c.Kind, c.Key)
				continue
			}
		}

	}
}

type batchApplier struct {
	t         *testing.T
	name      string
	writes    []writeCase
	nextSeq   keys.Sequence
	nextIndex int
}

func (a *batchApplier) Add(seq keys.Sequence, kind keys.Kind, key, value []byte) {
	i, t := a.nextIndex, a.t
	if i >= len(a.writes) {
		t.Fatalf("%s: total %d writes, got next index: %d", a.name, len(a.writes), i)
	}
	if a.writes[i].Kind != kind {
		t.Errorf("%s(%d): expect kind: %s, got %s", a.name, i, a.writes[i].Kind, kind)
	}
	if a.nextSeq != seq {
		t.Errorf("%s(%d): expect sequence: %d, got %d", a.name, i, a.nextSeq, seq)
	}
	if a.writes[i].Key != string(key) {
		t.Errorf("%s(%d): expect key: %s, got: %s", a.name, i, a.writes[i].Key, key)
	}
	switch kind {
	case keys.Value:
		if a.writes[i].Value != string(value) {
			t.Errorf("%s(%d): expect value: %s, got %s", a.name, i, a.writes[i].Value, value)
		}
	case keys.Delete:
	default:
		t.Errorf("%s(%d): unknown kind %s:%d", a.name, i, kind, kind)
	}
	a.nextIndex = i + 1
	a.nextSeq = seq + 1
}

func TestBatchIterate(t *testing.T) {
	for name, cases := range bunchCases {
		b := buildBatch(t, name, cases.Seq, cases.Writes)
		applier := batchApplier{t: t, name: name, writes: cases.Writes, nextSeq: cases.Seq}
		b.Iterate(&applier)
	}
}

func TestBatchAppend(t *testing.T) {
	var a batchApplier
	var b batch.Batch
	for name, cases := range bunchCases {
		switch b.Empty() {
		case true:
			a.t = t
			a.nextSeq = cases.Seq
			b = buildBatch(t, name, cases.Seq, cases.Writes)
		default:
			b1 := buildBatch(t, name, cases.Seq, cases.Writes)
			b.Append(b1.Bytes())
		}
		a.name = name
		a.writes = append(a.writes, cases.Writes...)
		a.nextSeq = b.Sequence()
		a.nextIndex = 0
		b.Iterate(&a)
	}
}
