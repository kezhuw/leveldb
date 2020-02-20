package iterator_test

import (
	"errors"
	"testing"

	"github.com/kezhuw/leveldb/internal/iterator"
	"github.com/stretchr/testify/suite"
)

func okCleanup() error {
	return nil
}

func newErrorCleanup(err error) func() error {
	return func() error {
		return err
	}
}

type WrappedOkIteratorTestSuite struct {
	suite.Suite
	wrappedIterator iterator.Iterator
}

var _ suite.SetupTestSuite = (*WrappedOkIteratorTestSuite)(nil)

func (suite *WrappedOkIteratorTestSuite) SetupTest() {
	suite.wrappedIterator = iterator.Empty()
}

func (suite *WrappedOkIteratorTestSuite) TestNilCleanup() {
	require := suite.Require()
	it := iterator.WithCleanup(suite.wrappedIterator, nil)
	require.Same(suite.wrappedIterator, it)
}

func (suite *WrappedOkIteratorTestSuite) TestOkCleanup() {
	require := suite.Require()
	it := iterator.WithCleanup(suite.wrappedIterator, okCleanup)
	require.NoError(it.Close())
}

func (suite *WrappedOkIteratorTestSuite) TestErrorCleanup() {
	require := suite.Require()
	err := errors.New("cleanup error")
	it := iterator.WithCleanup(suite.wrappedIterator, newErrorCleanup(err))
	require.Same(err, it.Close())
}

func TestCleanupIteratorWrappedOkIterator(t *testing.T) {
	suite.Run(t, new(WrappedOkIteratorTestSuite))
}

type WrappedErrorIteratorTestSuite struct {
	suite.Suite
	err             error
	wrappedIterator iterator.Iterator
}

var _ suite.SetupTestSuite = (*WrappedErrorIteratorTestSuite)(nil)

func (suite *WrappedErrorIteratorTestSuite) SetupTest() {
	suite.err = errors.New("wrapped error")
	suite.wrappedIterator = iterator.Error(suite.err)
}

func (suite *WrappedErrorIteratorTestSuite) TestNilCleanup() {
	require := suite.Require()
	it := iterator.WithCleanup(suite.wrappedIterator, nil)
	require.Same(suite.wrappedIterator, it)
}

func (suite *WrappedErrorIteratorTestSuite) TestOkCleanup() {
	require := suite.Require()
	it := iterator.WithCleanup(suite.wrappedIterator, okCleanup)
	require.Same(suite.err, it.Close())
}

func (suite *WrappedErrorIteratorTestSuite) TestErrorCleanup() {
	require := suite.Require()
	err := errors.New("cleanup error")
	it := iterator.WithCleanup(suite.wrappedIterator, newErrorCleanup(err))
	require.Same(suite.err, it.Close())
}

func TestCleanupIteratorWrappedErrorIterator(t *testing.T) {
	suite.Run(t, new(WrappedErrorIteratorTestSuite))
}
