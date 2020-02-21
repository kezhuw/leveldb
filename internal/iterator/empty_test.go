package iterator_test

import (
	"testing"

	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/stretchr/testify/suite"
)

type EmptyIteratorTestSuite struct {
	perTest bool

	it         iterator.Iterator
	panicValue string

	suite.Suite
}

var _ suite.SetupAllSuite = (*EmptyIteratorTestSuite)(nil)
var _ suite.SetupTestSuite = (*EmptyIteratorTestSuite)(nil)

func (suite *EmptyIteratorTestSuite) SetupSuite() {
	if !suite.perTest {
		suite.it = iterator.Empty()
	}
	suite.panicValue = "leveldb: empty iterator"
}

func (suite *EmptyIteratorTestSuite) SetupTest() {
	if suite.perTest {
		suite.it = iterator.Empty()
	}
}

func (suite *EmptyIteratorTestSuite) TestFirst() {
	require := suite.Require()
	it := suite.it
	require.False(it.First())
}

func (suite *EmptyIteratorTestSuite) TestLast() {
	require := suite.Require()
	it := suite.it
	require.False(it.Last())
}

func (suite *EmptyIteratorTestSuite) TestNext() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Next()
	})
}

func (suite *EmptyIteratorTestSuite) TestPrev() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Prev()
	})
}

func (suite *EmptyIteratorTestSuite) TestSeek() {
	require := suite.Require()
	it := suite.it
	require.False(it.Seek(nil))
	require.False(it.Seek([]byte("")))
	require.False(it.Seek([]byte("a")))
}

func (suite *EmptyIteratorTestSuite) TestValid() {
	require := suite.Require()
	it := suite.it
	require.False(it.Valid())
}

func (suite *EmptyIteratorTestSuite) TestKey() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Key()
	})
}

func (suite *EmptyIteratorTestSuite) TestValue() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Value()
	})
}

func (suite *EmptyIteratorTestSuite) TestErr() {
	require := suite.Require()
	it := suite.it
	require.NoError(it.Err())
}

func (suite *EmptyIteratorTestSuite) TestClose() {
	require := suite.Require()
	it := suite.it
	require.NoError(it.Close())
}

func TestEmptyIterator(t *testing.T) {
	suite.Run(t, &EmptyIteratorTestSuite{})
}

func TestEmptyIteratorPerMethod(t *testing.T) {
	suite.Run(t, &EmptyIteratorTestSuite{perTest: true})
}
