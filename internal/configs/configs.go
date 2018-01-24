package configs

const (
	NumberLevels = 7
)

const MaxMemTableCompactLevel = 2

const (
	TargetFileSize                 = 2 * 1024 * 1024
	ExpandedCompactionLimitBytes   = 25 * TargetFileSize
	MaxGrandparentOverlappingBytes = 10 * TargetFileSize
)
