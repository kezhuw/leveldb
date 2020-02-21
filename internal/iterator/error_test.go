package iterator_test

import (
	"errors"
	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ErrorIteratorTestSuite struct {
	err error
	perTest bool

	it iterator.Iterator
	panicValue string

	suite.Suite
}

var _ suite.SetupAllSuite = (*ErrorIteratorTestSuite)(nil)
var _ suite.SetupTestSuite = (*ErrorIteratorTestSuite)(nil)

func (suite *ErrorIteratorTestSuite) SetupSuite() {
	if !suite.perTest {
		suite.it = iterator.Error(suite.err)
	}
	suite.panicValue = "leveldb: error iterator: " + suite.err.Error()
}

func (suite *ErrorIteratorTestSuite) SetupTest() {
	if suite.perTest {
		suite.it = iterator.Error(suite.err)
	}
}

func (suite *ErrorIteratorTestSuite) TestFirst() {
	require := suite.Require()
	it := suite.it
	require.False(it.First())
}

func (suite *ErrorIteratorTestSuite) TestLast() {
	require := suite.Require()
	it := suite.it
	require.False(it.Last())
}

func (suite *ErrorIteratorTestSuite) TestNext() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Next()
	})
}

func (suite *ErrorIteratorTestSuite) TestPrev() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Prev()
	})
}

func (suite *ErrorIteratorTestSuite) TestSeek() {
	require := suite.Require()
	it := suite.it
	require.False(it.Seek(nil))
	require.False(it.Seek([]byte("")))
	require.False(it.Seek([]byte("a")))
}

func (suite *ErrorIteratorTestSuite) TestValid() {
	require := suite.Require()
	it := suite.it
	require.False(it.Valid())
}

func (suite *ErrorIteratorTestSuite) TestKey() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Key()
	})
}

func (suite *ErrorIteratorTestSuite) TestValue() {
	require := suite.Require()
	it := suite.it
	require.PanicsWithValue(suite.panicValue, func() {
		it.Value()
	})
}

func (suite *ErrorIteratorTestSuite) TestErr() {
	require := suite.Require()
	it := suite.it
	require.Same(suite.err, it.Err())
}

func (suite *ErrorIteratorTestSuite) TestClose() {
	require := suite.Require()
	it := suite.it
	require.Same(suite.err, it.Close())
}

func TestErrorIterator(t *testing.T) {
	suite.Run(t, &ErrorIteratorTestSuite{err: errors.New("an error iterator")})
}

func TestErrorIteratorPerMethod(t *testing.T) {
	suite.Run(t, &ErrorIteratorTestSuite{err: errors.New("yet another error iterator"), perTest: true})
}
