package leveldb_test

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/kezhuw/leveldb"
	"github.com/stretchr/testify/suite"
)

var creatingOptions = &leveldb.Options{CreateIfMissing: true, ErrorIfExists: false}

type entry interface {
	Key() []byte
	Value() []byte
}

type stringEntry struct {
	key   string
	value string
}

func (s *stringEntry) Key() []byte {
	return []byte(s.key)
}

func (s *stringEntry) Value() []byte {
	return []byte(s.value)
}

type DBTestSuite struct {
	suite.Suite

	setupPerTest bool

	dir      string
	db       *leveldb.DB
	snapshot *leveldb.Snapshot
}

func (suite *DBTestSuite) createDB() {
	dir, err := ioutil.TempDir("", "leveldb_test_")
	suite.Require().NoError(err, "fail to create tmp directory")

	defer func() {
		if err != nil {
			os.RemoveAll(dir)
		}
	}()

	db, err := leveldb.Open(dir, creatingOptions)
	suite.Require().NoError(err, "fail to create db %s", dir)

	suite.dir = dir
	suite.db = db
}

func (suite *DBTestSuite) removeDB() {
	defer os.RemoveAll(suite.dir)
	defer suite.db.Close()
}

func (suite *DBTestSuite) SetupSuite() {
	if !suite.setupPerTest {
		suite.createDB()
	}
}

func (suite *DBTestSuite) TearDownSuite() {
	if !suite.setupPerTest {
		suite.removeDB()
	}
}

func (suite *DBTestSuite) SetupTest() {
	if suite.setupPerTest {
		suite.createDB()
	}
}

func (suite *DBTestSuite) TearDownTest() {
	if suite.setupPerTest {
		suite.removeDB()
	}
}

func (suite *DBTestSuite) BeforeTest(suiteName, testName string) {
	if strings.Contains(suiteName, "Snapshot") {
		suite.snapshot = suite.db.Snapshot()
	}
}

func (suite *DBTestSuite) AfterTest(suiteName, testName string) {
	if snapshot := suite.snapshot; snapshot != nil {
		suite.snapshot = nil
		snapshot.Close()
	}
}

func (suite *DBTestSuite) getReader() leveldb.Reader {
	if suite.snapshot != nil {
		return suite.snapshot
	}
	return suite.db
}

func assertIterator(suite *suite.Suite, it leveldb.Iterator, entries []entry) {
	require := suite.Require()
	for _, entry := range entries {
		require.True(it.Next(), "expect entry: %s ==> %v", entry.Key(), entry.Value())
		require.Equal(entry.Key(), it.Key(), "value: %s, err: %s", it.Value(), it.Err())
		require.Equal(entry.Value(), it.Value(), "key: %s, err: %s", it.Key(), it.Err())
	}
	if it.Next() {
		require.Fail("unexpected next entry", "%s ==> %v", it.Key(), it.Value())
	}
}

func (suite *DBTestSuite) assertIterator(it leveldb.Iterator, entries []entry) {
	assertIterator(&suite.Suite, it, entries)
}

type EmptyDBTestSuite struct {
	DBTestSuite
}

func (suite *EmptyDBTestSuite) TestGetG() {
	r := suite.getReader()
	_, err := r.Get([]byte("g"), nil)
	suite.Require().Equal(leveldb.ErrNotFound, err)
}

func (suite *EmptyDBTestSuite) TestGetNotFound() {
	r := suite.getReader()
	_, err := r.Get([]byte("not-found"), nil)
	suite.Require().Equal(leveldb.ErrNotFound, err)
}

func (suite *EmptyDBTestSuite) TestAll() {
	r := suite.getReader()
	it := r.All(nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestFind() {
	r := suite.getReader()
	it := r.Find([]byte("g"), nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestFindAll() {
	r := suite.getReader()
	it := r.Find(nil, nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestRange() {
	r := suite.getReader()
	it := r.Range([]byte("g"), []byte("o"), nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestRangeAll() {
	r := suite.getReader()
	it := r.Range(nil, nil, nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestRangeEmpty() {
	r := suite.getReader()
	it := r.Range([]byte("o"), []byte("g"), nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestRangeStartAll() {
	r := suite.getReader()
	it := r.Range(nil, []byte("g"), nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestRangeLimitAll() {
	r := suite.getReader()
	it := r.Range([]byte("a"), nil, nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestPrefixG() {
	r := suite.getReader()
	it := r.Prefix([]byte("g"), nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *EmptyDBTestSuite) TestPrefixAll() {
	r := suite.getReader()
	it := r.Prefix(nil, nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func TestEmptyDB(t *testing.T) {
	suite.Run(t, new(EmptyDBTestSuite))
}

type EmptyDBSnapshotTestSuite struct {
	EmptyDBTestSuite
}

func TestEmptyDBSnapshot(t *testing.T) {
	suite.Run(t, new(EmptyDBSnapshotTestSuite))
}

type PredefinedData1DBTestSuite struct {
	DBTestSuite
	allEntries []entry
}

func (suite *PredefinedData1DBTestSuite) SetupTest() {
	var err error
	var batch leveldb.Batch

	batch.Put([]byte("a"), []byte("v/a"))
	batch.Put([]byte("g"), []byte("v/g"))
	batch.Put([]byte("g0"), []byte("v/g0"))
	batch.Put([]byte("g1"), []byte("v/g1"))
	batch.Put([]byte("g01"), []byte("v/g01"))
	batch.Put([]byte("k"), []byte("v/k"))
	batch.Put([]byte("k5"), []byte("v/k5/d"))
	batch.Put([]byte("k5"), []byte("v/k5"))
	batch.Put([]byte("k05"), []byte("v/k05"))
	batch.Put([]byte("o"), []byte("v/o"))
	batch.Put([]byte("z"), []byte("v/z"))

	err = suite.db.Write(batch, nil)
	suite.Require().NoError(err)

	suite.allEntries = []entry{
		&stringEntry{"a", "v/a"},
		&stringEntry{"g", "v/g"},
		&stringEntry{"g0", "v/g0"},
		&stringEntry{"g01", "v/g01"},
		&stringEntry{"g1", "v/g1"},
		&stringEntry{"k", "v/k"},
		&stringEntry{"k05", "v/k05"},
		&stringEntry{"k5", "v/k5"},
		&stringEntry{"o", "v/o"},
		&stringEntry{"z", "v/z"},
	}
}

func (suite *PredefinedData1DBTestSuite) TestGetG() {
	r := suite.getReader()
	v, err := r.Get([]byte("g"), nil)
	suite.Require().NoError(err)
	suite.Require().Equal([]byte("v/g"), v)
}

func (suite *PredefinedData1DBTestSuite) TestGetNotFound() {
	r := suite.getReader()
	_, err := r.Get([]byte("not-found"), nil)
	suite.Require().Equal(leveldb.ErrNotFound, err)
}

func (suite *PredefinedData1DBTestSuite) TestAll() {
	r := suite.getReader()
	it := r.All(nil)
	defer it.Close()
	suite.assertIterator(it, suite.allEntries)
}

func (suite *PredefinedData1DBTestSuite) TestFind() {
	r := suite.getReader()
	it := r.Find([]byte("g"), nil)
	defer it.Close()
	suite.assertIterator(it, []entry{
		&stringEntry{"g", "v/g"},
		&stringEntry{"g0", "v/g0"},
		&stringEntry{"g01", "v/g01"},
		&stringEntry{"g1", "v/g1"},
		&stringEntry{"k", "v/k"},
		&stringEntry{"k05", "v/k05"},
		&stringEntry{"k5", "v/k5"},
		&stringEntry{"o", "v/o"},
		&stringEntry{"z", "v/z"},
	})
}

func (suite *PredefinedData1DBTestSuite) TestFindAll() {
	r := suite.getReader()
	it := r.Find(nil, nil)
	defer it.Close()
	suite.assertIterator(it, suite.allEntries)
}

func (suite *PredefinedData1DBTestSuite) TestRange() {
	r := suite.getReader()
	it := r.Range([]byte("g"), []byte("o"), nil)
	defer it.Close()
	suite.assertIterator(it, []entry{
		&stringEntry{"g", "v/g"},
		&stringEntry{"g0", "v/g0"},
		&stringEntry{"g01", "v/g01"},
		&stringEntry{"g1", "v/g1"},
		&stringEntry{"k", "v/k"},
		&stringEntry{"k05", "v/k05"},
		&stringEntry{"k5", "v/k5"},
	})
}

func (suite *PredefinedData1DBTestSuite) TestRangeAll() {
	r := suite.getReader()
	it := r.Range(nil, nil, nil)
	defer it.Close()
	suite.assertIterator(it, suite.allEntries)
}

func (suite *PredefinedData1DBTestSuite) TestRangeEmpty() {
	r := suite.getReader()
	it := r.Range([]byte("o"), []byte("g"), nil)
	defer it.Close()
	suite.assertIterator(it, nil)
}

func (suite *PredefinedData1DBTestSuite) TestRangeStartAll() {
	r := suite.getReader()
	it := r.Range(nil, []byte("o"), nil)
	defer it.Close()
	suite.assertIterator(it, []entry{
		&stringEntry{"a", "v/a"},
		&stringEntry{"g", "v/g"},
		&stringEntry{"g0", "v/g0"},
		&stringEntry{"g01", "v/g01"},
		&stringEntry{"g1", "v/g1"},
		&stringEntry{"k", "v/k"},
		&stringEntry{"k05", "v/k05"},
		&stringEntry{"k5", "v/k5"},
	})
}

func (suite *PredefinedData1DBTestSuite) TestRangeLimitAll() {
	r := suite.getReader()
	it := r.Range([]byte("g"), nil, nil)
	defer it.Close()
	suite.assertIterator(it, []entry{
		&stringEntry{"g", "v/g"},
		&stringEntry{"g0", "v/g0"},
		&stringEntry{"g01", "v/g01"},
		&stringEntry{"g1", "v/g1"},
		&stringEntry{"k", "v/k"},
		&stringEntry{"k05", "v/k05"},
		&stringEntry{"k5", "v/k5"},
		&stringEntry{"o", "v/o"},
		&stringEntry{"z", "v/z"},
	})
}

func (suite *PredefinedData1DBTestSuite) TestPrefixG() {
	r := suite.getReader()
	it := r.Prefix([]byte("g"), nil)
	defer it.Close()
	suite.assertIterator(it, []entry{
		&stringEntry{"g", "v/g"},
		&stringEntry{"g0", "v/g0"},
		&stringEntry{"g01", "v/g01"},
		&stringEntry{"g1", "v/g1"},
	})
}

func (suite *PredefinedData1DBTestSuite) TestPrefixG0() {
	r := suite.getReader()
	it := r.Prefix([]byte("g0"), nil)
	defer it.Close()
	suite.assertIterator(it, []entry{
		&stringEntry{"g0", "v/g0"},
		&stringEntry{"g01", "v/g01"},
	})
}

func (suite *PredefinedData1DBTestSuite) TestPrefixAll() {
	r := suite.getReader()
	it := r.Prefix(nil, nil)
	defer it.Close()
	suite.assertIterator(it, suite.allEntries)
}

func TestPredefinedData1DB(t *testing.T) {
	suite.Run(t, new(PredefinedData1DBTestSuite))
}

type PredefinedData1DBSnapshotTestSuite struct {
	PredefinedData1DBTestSuite
}

func TestPredefinedData1DBSnapshot(t *testing.T) {
	suite.Run(t, new(PredefinedData1DBSnapshotTestSuite))
}

type CompactRangeDBTestSuite struct {
	DBTestSuite
}

func (suite *CompactRangeDBTestSuite) TestRange() {
	var err error
	db := suite.db
	require := suite.Require()

	err = db.Put([]byte("a"), []byte("v/a0"), nil)
	require.NoError(err)

	err = db.Put([]byte("g"), []byte("v/g0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), []byte("p"))
	require.NoError(err)

	err = db.Put([]byte("f"), []byte("v/f0"), nil)
	require.NoError(err)

	err = db.Put([]byte("k"), []byte("v/k0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), []byte("p"))
	require.NoError(err)

	err = db.Put([]byte("o"), []byte("v/o0"), nil)
	require.NoError(err)

	err = db.Put([]byte("q"), []byte("v/q0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), []byte("p"))
	require.NoError(err)

	err = db.Put([]byte("r"), []byte("v/r0"), nil)
	require.NoError(err)

	err = db.Put([]byte("s"), []byte("v/s0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), []byte("p"))
	require.NoError(err)
}

func (suite *CompactRangeDBTestSuite) TestRangeAll() {
	var err error
	db := suite.db
	require := suite.Require()

	err = db.Put([]byte("a"), []byte("v/a0"), nil)
	require.NoError(err)

	err = db.Put([]byte("g"), []byte("v/g0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, nil)
	require.NoError(err)

	err = db.Put([]byte("f"), []byte("v/f0"), nil)
	require.NoError(err)

	err = db.Put([]byte("k"), []byte("v/k0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, nil)
	require.NoError(err)

	err = db.Put([]byte("o"), []byte("v/o0"), nil)
	require.NoError(err)

	err = db.Put([]byte("q"), []byte("v/q0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, nil)
	require.NoError(err)

	err = db.Put([]byte("r"), []byte("v/r0"), nil)
	require.NoError(err)

	err = db.Put([]byte("s"), []byte("v/s0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, nil)
	require.NoError(err)
}

func (suite *CompactRangeDBTestSuite) TestRangeEmpty() {
	var err error
	db := suite.db
	require := suite.Require()

	err = db.Put([]byte("a"), []byte("v/a0"), nil)
	require.NoError(err)

	err = db.Put([]byte("g"), []byte("v/g0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("p"), []byte("c"))
	require.NoError(err)

	err = db.Put([]byte("f"), []byte("v/f0"), nil)
	require.NoError(err)

	err = db.Put([]byte("k"), []byte("v/k0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("p"), []byte("c"))
	require.NoError(err)

	err = db.Put([]byte("o"), []byte("v/o0"), nil)
	require.NoError(err)

	err = db.Put([]byte("q"), []byte("v/q0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), []byte("p"))
	require.NoError(err)

	err = db.Put([]byte("r"), []byte("v/r0"), nil)
	require.NoError(err)

	err = db.Put([]byte("s"), []byte("v/s0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), []byte("p"))
	require.NoError(err)
}

func (suite *CompactRangeDBTestSuite) TestRangeStartAll() {
	var err error
	db := suite.db
	require := suite.Require()

	err = db.Put([]byte("a"), []byte("v/a0"), nil)
	require.NoError(err)

	err = db.Put([]byte("g"), []byte("v/g0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, []byte("p"))
	require.NoError(err)

	err = db.Put([]byte("f"), []byte("v/f0"), nil)
	require.NoError(err)

	err = db.Put([]byte("k"), []byte("v/k0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, []byte("p"))
	require.NoError(err)

	err = db.Put([]byte("o"), []byte("v/o0"), nil)
	require.NoError(err)

	err = db.Put([]byte("q"), []byte("v/q0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, []byte("p"))
	require.NoError(err)

	err = db.Put([]byte("r"), []byte("v/r0"), nil)
	require.NoError(err)

	err = db.Put([]byte("s"), []byte("v/s0"), nil)
	require.NoError(err)

	err = db.CompactRange(nil, []byte("p"))
	require.NoError(err)
}

func (suite *CompactRangeDBTestSuite) TestRangeLimitAll() {
	var err error
	db := suite.db
	require := suite.Require()

	err = db.Put([]byte("a"), []byte("v/a0"), nil)
	require.NoError(err)

	err = db.Put([]byte("g"), []byte("v/g0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), nil)
	require.NoError(err)

	err = db.Put([]byte("f"), []byte("v/f0"), nil)
	require.NoError(err)

	err = db.Put([]byte("k"), []byte("v/k0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), nil)
	require.NoError(err)

	err = db.Put([]byte("o"), []byte("v/o0"), nil)
	require.NoError(err)

	err = db.Put([]byte("q"), []byte("v/q0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), nil)
	require.NoError(err)

	err = db.Put([]byte("r"), []byte("v/r0"), nil)
	require.NoError(err)

	err = db.Put([]byte("s"), []byte("v/s0"), nil)
	require.NoError(err)

	err = db.CompactRange([]byte("c"), nil)
	require.NoError(err)
}

func TestDBCompactRange(t *testing.T) {
	suite.Run(t, &CompactRangeDBTestSuite{DBTestSuite{setupPerTest: true}})
}
