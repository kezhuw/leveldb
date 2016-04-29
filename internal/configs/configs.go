package configs

const (
	NumberLevels = 7
)

const (
	L0CompactionFiles = 4
	L0SlowdownFiles   = 8
	L0StopWritesFiles = 12
)

const MaxMemTableCompactLevel = 2

const (
	TargetFileSize                 = 2 * 1024 * 1024
	ExpandedCompactionLimitBytes   = 25 * TargetFileSize
	MaxGrandparentOverlappingBytes = 10 * TargetFileSize
)
