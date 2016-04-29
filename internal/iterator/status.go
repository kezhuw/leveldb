package iterator

type Status int

const (
	Initial Status = iota
	Invalid
	Closed
	Valid
)
