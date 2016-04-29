package filter

import "bytes"

type Filter interface {
	Name() string
	Append(buf *bytes.Buffer, keys [][]byte)
	Contains(data, key []byte) bool
	NewGenerator() Generator
}

type Generator interface {
	Name() string
	Reset()
	Empty() bool
	Add(key []byte)
	Append(buf *bytes.Buffer)
}

type fallbackGenerator struct {
	filter         Filter
	offsets        []int
	flattenedKeys  []byte
	referencedKeys [][]byte
}

func (g *fallbackGenerator) Name() string {
	return g.filter.Name()
}

func (g *fallbackGenerator) Add(key []byte) {
	g.offsets = append(g.offsets, len(g.flattenedKeys))
	g.flattenedKeys = append(g.flattenedKeys, key...)
}

func (g *fallbackGenerator) Reset() {
	g.offsets = g.offsets[:0]
	g.flattenedKeys = g.flattenedKeys[:0]
}

func (g *fallbackGenerator) Empty() bool {
	return len(g.offsets) == 0
}

func (g *fallbackGenerator) Append(buf *bytes.Buffer) {
	n := len(g.offsets)
	g.offsets = append(g.offsets, len(g.flattenedKeys))
	keys := g.referencedKeys[:0]
	for i, start := 0, g.offsets[0]; i < n; i++ {
		limit := g.offsets[i+1]
		keys = append(keys, g.flattenedKeys[start:limit])
		start = limit
	}
	g.filter.Append(buf, keys)
	g.referencedKeys = keys
	g.Reset()
}

func NewGenerator(filter Filter) Generator {
	g := filter.NewGenerator()
	if g != nil {
		return g
	}
	return &fallbackGenerator{filter: filter}
}
