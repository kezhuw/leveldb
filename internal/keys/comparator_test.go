package keys_test

type comparatorCompareTest struct {
	a []byte
	b []byte
	r int
}

type comparatorSuccessorTest struct {
	start     []byte
	limit     []byte
	successor []byte
}

type comparatorPrefixSuccessorTest struct {
	prefix    []byte
	successor []byte
}
