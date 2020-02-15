package manifest

import (
	"fmt"
	"sort"

	"github.com/kezhuw/leveldb/internal/compaction"
	"github.com/kezhuw/leveldb/internal/configs"
	"github.com/kezhuw/leveldb/internal/errors"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/kezhuw/leveldb/internal/keys"
	"github.com/kezhuw/leveldb/internal/options"
	"github.com/kezhuw/leveldb/internal/table"
)

type compactionScore struct {
	level int
	score float64
}

type byTopScore []compactionScore

func (p byTopScore) Len() int {
	return len(p)
}

func (p byTopScore) Less(i, j int) bool {
	return p[i].score > p[j].score
}

func (p byTopScore) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type Version struct {
	number   uint64
	cache    *table.Cache
	options  *options.Options
	manifest *Manifest

	// Levels[0], sorted from newest to oldest;
	// Levels[n], sorted from smallest to largest.
	Levels [configs.NumberLevels]FileList

	scores []compactionScore

	CompactionPointers [configs.NumberLevels]keys.InternalKey
}

func (v *Version) String() string {
	var s string
	for level, files := range v.Levels[:] {
		if len(files) == 0 {
			s += fmt.Sprintf("level %d contains 0 files.\n", level)
			continue
		}
		s += fmt.Sprintf("level %d contains %d files:\n", level, len(files))
		for _, f := range files {
			s += fmt.Sprintf("file %d: size %d, smallest key: %q, largest key: %q\n", f.Number, f.Size, f.Smallest, f.Largest)
		}
		s += "\n"
	}
	s += fmt.Sprintf("compaction scores: %v\n", v.scores)
	for level, pointer := range v.CompactionPointers[:] {
		if len(pointer) == 0 {
			continue
		}
		s += fmt.Sprintf("level %d compaction pointer at: %q\n", level, pointer)
	}
	return s
}

func (v *Version) SortFiles() error {
	v.Levels[0].SortByNewestFileNumber()
	icmp := v.options.Comparator
	for level := 1; level < configs.NumberLevels; level++ {
		files := v.Levels[level]
		files.SortBySmallestKey(icmp)
		for i := 0; i < len(files)-1; i++ {
			if icmp.Compare(files[i].Largest, files[i+1].Smallest) >= 0 {
				return errors.ErrOverlappedTables
			}
		}
	}
	return nil
}

func (v *Version) AppendIterators(iters []iterator.Iterator, opts *options.ReadOptions) []iterator.Iterator {
	for _, f := range v.Levels[0] {
		iters = append(iters, v.cache.NewIterator(f.Number, f.Size, opts))
	}
	for level := 1; level < len(v.Levels); level++ {
		files := v.Levels[level]
		if len(files) == 0 {
			continue
		}
		iters = append(iters, newSortedFileIterator(v.options.Comparator, files, v.cache, opts))
	}
	return iters
}

func (v *Version) match(m matcher, ikey keys.InternalKey, opts *options.ReadOptions) bool {
	// If internal key a < b, two possibilities exist:
	//   * a's user key is smaller than b's user key.
	//   * a's user key is same with b, but a has a larger sequence(packed with keys.Kind).
	//
	// When ikey < file.Smallest, its smaller sequence incarnations, which we are interest in,
	// may exist in that file. But if the user key part of ikey and file.Smallest holds the less
	// than relationship, then no ikey's incarnations exist in that file. So we compare user key
	// parts of ikey and file.Smallest.
	//
	// When ikey > file.Largest, if file.Largest has same key with ikey, it must has larger sequence,
	// which we can ignore it safely. So we compare ikey and file.Largest directly.
	icmp := v.options.Comparator
	ucmp := icmp.UserKeyComparator
	ukey := ikey.UserKey()
	for _, f := range v.Levels[0] {
		if ucmp.Compare(ukey, f.Smallest.UserKey()) < 0 {
			continue
		}
		if icmp.Compare(ikey, []byte(f.Largest)) > 0 {
			continue
		}
		if m.Match(0, f, ikey, opts) {
			return true
		}
	}
	for level := 1; level < configs.NumberLevels; level++ {
		files := v.Levels[level]
		n := len(files)
		i := sort.Search(n, func(i int) bool { return icmp.Compare(ikey, files[i].Largest) <= 0 })
		if i == n || ucmp.Compare(ukey, files[i].Smallest.UserKey()) < 0 {
			continue
		}
		if m.Match(level, files[i], ikey, opts) {
			return true
		}
	}
	return false
}

func (v *Version) Get(ikey keys.InternalKey, opts *options.ReadOptions) ([]byte, LevelFileMeta, error) {
	var matcher getMatcher
	matcher.cache = v.cache
	if v.match(&matcher, ikey, opts) {
		return matcher.value, matcher.seekThrough.seekThrough(), matcher.err
	}
	return nil, matcher.seekThrough.seekThrough(), errors.ErrNotFound
}

func (v *Version) SeekOverlap(ikey keys.InternalKey, opts *options.ReadOptions) LevelFileMeta {
	var matcher seekOverlapMatcher
	if v.match(&matcher, ikey, opts) {
		return matcher.seekOverlap.seekOverlap()
	}
	return LevelFileMeta{}
}

func (v *Version) computeLevel0CompactionScore() float64 {
	numFiles := len(v.Levels[0])
	score := float64(numFiles) / float64(v.options.Level0CompactionFiles)
	switch {
	case score < 1.0:
	case numFiles >= v.options.Level0StopWriteFiles && score < 3.0:
		score = 3.0
	case numFiles >= v.options.Level0SlowdownWriteFiles && score < 2.0:
		score = 2.0
	}
	return score
}

func (v *Version) computeCompactionScore() {
	if score := v.computeLevel0CompactionScore(); score > 1.0 {
		v.scores = append(v.scores, compactionScore{level: 0, score: score})
	}
	maxBytes := 10 * 1024 * 1024
	for level := 1; level < len(v.Levels)-1; level++ {
		if score := float64(v.Levels[level].TotalFileSize()) / float64(maxBytes); score > 1.0 {
			v.scores = append(v.scores, compactionScore{level: level, score: score})
		}
	}
	sort.Sort(byTopScore(v.scores))
}

type overlayer interface {
	Start()
	Done()
	Overlap(f *FileMeta)
}

type panicBoolOverlayer struct {
	overlapped bool
}

func (o *panicBoolOverlayer) Start() {
}

func (o *panicBoolOverlayer) Overlap(f *FileMeta) {
	o.overlapped = true
	panic(*o)
}

func (o *panicBoolOverlayer) Done() {
}

type sizeOverlayer struct {
	start int64
	total int64
}

func (o *sizeOverlayer) Start() {
	o.total = o.start
}

func (o *sizeOverlayer) Overlap(f *FileMeta) {
	o.total += int64(f.Size)
}

func (o *sizeOverlayer) Done() {
	o.start = o.total
}

type fileOverlayer struct {
	start int
	files FileList
}

func (o *fileOverlayer) Start() {
	o.files = o.files[:o.start]
}

func (o *fileOverlayer) Overlap(f *FileMeta) {
	o.files = append(o.files, f)
}

func (o *fileOverlayer) Done() {
	o.start = len(o.files)
}

func (v *Version) overlapLevel0(o overlayer, smallest, largest keys.InternalKey) {
	ucmp := v.options.Comparator.UserKeyComparator
	files := v.Levels[0]
	defer o.Done()
restart:
	o.Start()
	for _, f := range files {
		switch {
		case smallest != nil && ucmp.Compare(smallest.UserKey(), f.Largest.UserKey()) > 0:
			continue
		case largest != nil && ucmp.Compare(largest.UserKey(), f.Smallest.UserKey()) < 0:
			continue
		}
		o.Overlap(f)
		lowerBoundExtended := smallest != nil && ucmp.Compare(f.Smallest.UserKey(), smallest.UserKey()) < 0
		upperBoundExtended := largest != nil && ucmp.Compare(f.Largest.UserKey(), largest.UserKey()) > 0
		if lowerBoundExtended || upperBoundExtended {
			if lowerBoundExtended {
				smallest = f.Smallest
			}
			if upperBoundExtended {
				largest = f.Largest
			}
			goto restart
		}
	}
}

func (v *Version) overlapLeveln(o overlayer, level int, smallest, largest keys.InternalKey) {
	if level == 0 {
		v.overlapLevel0(o, smallest, largest)
		return
	}
	ucmp := v.options.Comparator.UserKeyComparator
	files := v.Levels[level]
	o.Start()
	defer o.Done()
	n := len(files)
	i := 0
	if smallest != nil {
		i = sort.Search(n, func(i int) bool { return ucmp.Compare(smallest.UserKey(), files[i].Largest.UserKey()) <= 0 })
	}
	for ; i < n; i++ {
		if largest != nil && ucmp.Compare(largest.UserKey(), files[i].Smallest.UserKey()) < 0 {
			break
		}
		o.Overlap(files[i])
	}
}

func toInternalKeyRange(start, limit []byte) (startInternalKey, limitInternalKey keys.InternalKey) {
	if start != nil {
		startInternalKey = keys.NewInternalKey(start, keys.MaxSequence, keys.Seek)
	}
	if limit != nil {
		limitInternalKey = keys.NewInternalKey(limit, 0, 0)
	}
	return startInternalKey, limitInternalKey
}

func (v *Version) OverlapLevel(level int, start, limit []byte) bool {
	startInternalKey, limitInternalKey := toInternalKeyRange(start, limit)
	return v.isOverlappingWithLevel(level, startInternalKey, limitInternalKey)
}

// NextOverlappingLevel returns first overlapping level in range [level, maxLevel] with start and limit.
func (v *Version) NextOverlapingLevel(level int, start, limit []byte) int {
	for ; level < configs.NumberLevels; level++ {
		if v.OverlapLevel(level, start, limit) {
			return level
		}
	}
	return -1
}

func (v *Version) isOverlappingWithLevel(level int, smallest, largest keys.InternalKey) (overlapped bool) {
	if smallest == nil && largest == nil {
		return len(v.Levels[level]) != 0
	}
	var b panicBoolOverlayer
	defer func() {
		if r := recover(); r != nil {
			overlapped = r.(panicBoolOverlayer).overlapped
		}
	}()
	v.overlapLeveln(&b, level, smallest, largest)
	return b.overlapped
}

func (v *Version) appendOverlappingFiles(files FileList, level int, smallest, largest keys.InternalKey) FileList {
	var collector fileOverlayer
	collector.start = len(files)
	collector.files = files
	v.overlapLeveln(&collector, level, smallest, largest)
	return collector.files
}

func (v *Version) rangeOf(files FileList) (smallest, largest keys.InternalKey) {
	icmp := v.options.Comparator
	smallest, largest = files[0].Smallest, files[0].Largest
	for _, f := range files[1:] {
		if icmp.Compare(f.Smallest, smallest) < 0 {
			smallest = f.Smallest
		}
		if icmp.Compare(f.Largest, largest) > 0 {
			largest = f.Largest
		}
	}
	return smallest, largest
}

func (v *Version) unionOf(smallest0, largest0, smallest1, largest1 keys.InternalKey) (smallest, largest keys.InternalKey) {
	icmp := v.options.Comparator
	return keys.Min(icmp, smallest0, smallest1), keys.Max(icmp, largest0, largest1)
}

func (v *Version) pickLevelInputs(c *Compaction, inputs FileList) (smallest, largest keys.InternalKey) {
	level := c.Level
	files := v.Levels[level]
	icmp := v.options.Comparator
	switch {
	case len(inputs) != 0 && level == 0:
		inputs.SortByNewestFileNumber()
	case len(inputs) != 0:
		inputs.SortBySmallestKey(icmp)
	case len(v.CompactionPointers[level]) == 0:
		inputs = append(inputs, files[0])
	case level == 0:
		for _, f := range files {
			if icmp.Compare(f.Largest, v.CompactionPointers[level]) > 0 {
				inputs = append(inputs, f)
				goto find_overlapping
			}
		}
		inputs = append(inputs, files[0])
	default:
		n := len(files)
		i := sort.Search(n, func(i int) bool { return icmp.Compare(files[i].Largest, v.CompactionPointers[level]) > 0 })
		switch i {
		case n:
			inputs = append(inputs, files[0])
		default:
			inputs = append(inputs, files[i])
		}
	}
find_overlapping:
	smallest, largest = v.rangeOf(inputs)
	if level == 0 {
		inputs = v.appendOverlappingFiles(inputs[:0], 0, smallest, largest)
		smallest, largest = v.rangeOf(inputs)
	}
	c.Inputs[0] = inputs
	return smallest, largest
}

func (v *Version) expandLevelInputs(c *Compaction, smallest, largest *keys.InternalKey) (allSmallest, allLargest keys.InternalKey) {
	smallest0, largest0 := *smallest, *largest
	c.Inputs[1] = v.appendOverlappingFiles(c.Inputs[1][:0], c.Level+1, smallest0, largest0)
	if len(c.Inputs[1]) == 0 {
		return smallest0, largest0
	}
	// Try to expand the number of files in inputs[0], without changing the number
	// of files in inputs[1].
	smallest1, largest1 := v.rangeOf(c.Inputs[1])
	allSmallest, allLargest = v.unionOf(smallest0, largest0, smallest1, largest1)
	expandeds0 := v.appendOverlappingFiles(nil, c.Level, allSmallest, allLargest)
	if len(expandeds0) <= len(c.Inputs[0]) {
		return
	}
	inputs1Size := c.Inputs[1].TotalFileSize()
	expandeds0Size := expandeds0.TotalFileSize()
	if expandeds0Size+inputs1Size >= uint64(v.options.MaxExpandedCompactionBytes) {
		return
	}
	smallest0, largest0 = v.rangeOf(expandeds0)
	expandeds1 := v.appendOverlappingFiles(nil, c.Level+1, smallest0, largest0)
	if len(expandeds1) != len(c.Inputs[1]) {
		return
	}
	*smallest = smallest0
	*largest = largest0
	c.Inputs[0] = expandeds0
	return v.unionOf(smallest0, largest0, smallest1, largest1)
}

func (v *Version) newLevelCompaction(registration *compaction.Registration, level int, inputs FileList) *Compaction {
	c := &Compaction{Level: level, Base: v, Registration: registration}

	smallest, largest := v.pickLevelInputs(c, inputs)
	allSmallest, allLargest := v.expandLevelInputs(c, &smallest, &largest)

	if grandparentsLevel := c.Level + 2; grandparentsLevel < configs.NumberLevels {
		c.Grandparents = v.appendOverlappingFiles(c.Grandparents[:0], grandparentsLevel, allSmallest, allLargest)
	}
	c.MaxOutputFileSize = v.options.MaxFileSize
	c.NextCompactPointer = largest

	return c
}

func (v *Version) appendScoreCompactions(compactions []*Compaction, registry *compaction.Registry, nextFileNumber uint64) []*Compaction {
	for _, score := range v.scores {
		level := score.level
		registration := registry.Register(level, level+1)
		if registration == nil {
			continue
		}
		registration.NextFileNumber = nextFileNumber
		compactions = append(compactions, v.newLevelCompaction(registration, level, nil))
	}
	return compactions
}

func (v *Version) appendFileCompactions(compactions []*Compaction, registry *compaction.Registry, levels []FileList, nextFileNumber uint64) []*Compaction {
	for level, files := range levels {
		n := len(files)
		if n == 0 {
			continue
		}
		i := 0
		for i < n {
			f := files[i]
			if v.Levels[level].IndexFile(f.Number) == -1 {
				n--
				files[i], files[n] = files[n], nil
				continue
			}
			i++
		}
		files = files[:i]
		levels[level] = files
		if i != 0 {
			registration := registry.Register(level, level+1)
			if registration == nil {
				continue
			}
			registration.NextFileNumber = nextFileNumber
			compactions = append(compactions, v.newLevelCompaction(registration, level, files.Dup()))
		}
	}
	return compactions
}

func (v *Version) pickCompactions(registry *compaction.Registry, pendingFiles []FileList, nextFileNumber uint64) []*Compaction {
	var compactions []*Compaction
	compactions = v.appendScoreCompactions(compactions, registry, nextFileNumber)
	compactions = v.appendFileCompactions(compactions, registry, pendingFiles, nextFileNumber)
	return compactions
}

func (v *Version) PickRangeCompaction(registry *compaction.Registry, level int, start, limit []byte) *Compaction {
	startInternalKey, limitInternalKey := toInternalKeyRange(start, limit)
	overlappingFiles := v.appendOverlappingFiles(nil, level, startInternalKey, limitInternalKey)
	if len(overlappingFiles) == 0 {
		return nil
	}
	registration := registry.Register(level, level+1)
	if registration == nil {
		return nil
	}
	return v.newLevelCompaction(registration, level, overlappingFiles)
}

func (v *Version) PickLevelForMemTableOutput(smallest, largest []byte) int {
	maxLevel := configs.MaxMemTableCompactLevel
	if maxLevel <= 0 || configs.NumberLevels <= 1 {
		return 0
	}
	if v.isOverlappingWithLevel(0, smallest, largest) || v.isOverlappingWithLevel(1, smallest, largest) {
		return 0
	}
	var size sizeOverlayer
	for level := 1; level <= maxLevel; level++ {
		if level+1 >= configs.NumberLevels {
			return level
		}
		size.start = 0
		size.total = 0
		v.overlapLeveln(&size, level+1, smallest, largest)
		switch {
		case size.total > v.options.MaxGrandparentOverlapBytes:
			return level - 1
		case size.total != 0:
			return level
		}
	}
	return maxLevel
}

func (v *Version) edit(edit *Edit) (*Version, error) {
	v1 := v.clone()
	if err := v1.apply(edit); err != nil {
		return nil, err
	}
	v1.computeCompactionScore()
	return v1, nil
}

func (v *Version) snapshot(edit *Edit) {
	for level := 0; level < configs.NumberLevels; level++ {
		if len(v.CompactionPointers[level]) != 0 {
			edit.CompactPointers = append(edit.CompactPointers, LevelCompactPointer{Level: level, Largest: v.CompactionPointers[level]})
		}
		for _, f := range v.Levels[level] {
			edit.AddedFiles = append(edit.AddedFiles, LevelFileMeta{Level: level, FileMeta: f})
		}
	}
}

func (v *Version) clone() *Version {
	copy := &Version{number: v.number + 1, options: v.options, cache: v.cache, manifest: v.manifest}
	for level := 0; level < configs.NumberLevels; level++ {
		copy.Levels[level] = v.Levels[level].Dup()
	}
	copy.CompactionPointers = v.CompactionPointers
	return copy
}

func (v *Version) apply(edit *Edit) error {
	for _, deleted := range edit.DeletedFiles {
		ok := v.Levels[deleted.Level].DeleteFile(deleted.Number)
		if !ok {
			return fmt.Errorf("no file numbered %d in level %d", deleted.Number, deleted.Level)
		}
	}
	for _, added := range edit.AddedFiles {
		f := added.FileMeta
		f.resetAllowedSeeks(v.options.CompactionBytesPerSeek, v.options.MinimalAllowedOverlapSeeks)
		v.Levels[added.Level] = append(v.Levels[added.Level], f)
	}
	for _, pointer := range edit.CompactPointers {
		v.CompactionPointers[pointer.Level] = pointer.Largest
	}
	return v.SortFiles()
}

func (v *Version) refFiles(files map[uint64]int) {
	for level := 0; level < configs.NumberLevels; level++ {
		for _, f := range v.Levels[level] {
			files[f.Number]++
		}
	}
}

func (v *Version) unrefFiles(files map[uint64]int) {
	for level := 0; level < configs.NumberLevels; level++ {
		for _, f := range v.Levels[level] {
			refs := files[f.Number] - 1
			if refs <= 0 {
				delete(files, f.Number)
				continue
			}
			files[f.Number] = refs
		}
	}
}

func (v *Version) finalize() {
	v.manifest.unmountVersion(v)
}
