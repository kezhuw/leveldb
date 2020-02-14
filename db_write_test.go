package leveldb_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/kezhuw/leveldb"
	"github.com/stretchr/testify/suite"
)

type DBWriteTestSuite struct {
	DBTestSuite
}

func (suite *DBWriteTestSuite) TestGetPutDelete() {
	var err error
	var value []byte

	db := suite.db
	require := suite.Require()

	value, err = db.Get([]byte("k"), nil)
	require.Equal(leveldb.ErrNotFound, err)
	require.Nil(value)

	err = db.Put([]byte("k"), []byte("v/k"), nil)
	require.NoError(err)

	value, err = db.Get([]byte("k"), nil)
	require.NoError(err)
	require.Equal([]byte("v/k"), value)

	err = db.Delete([]byte("k"), nil)
	require.NoError(err)

	value, err = db.Get([]byte("k"), nil)
	require.Equal(leveldb.ErrNotFound, err)
	require.Nil(value)

	err = db.Delete([]byte("k"), nil)
	require.NoError(err)
}

func (suite *DBWriteTestSuite) TestWriteBatch() {
	var err error
	var value []byte
	var batch leveldb.Batch

	db := suite.db
	require := suite.Require()

	// Put single key.
	batch.Clear()
	batch.Put([]byte("k"), []byte("v/k0"))
	err = db.Write(batch, nil)
	require.NoError(err)
	value, err = db.Get([]byte("k"), nil)
	require.NoError(err)
	require.Equal([]byte("v/k0"), value)

	// Delete existing key and Put new key.
	batch.Clear()
	batch.Delete([]byte("k"))
	batch.Put([]byte("p"), []byte("v/p0"))
	err = db.Write(batch, nil)
	require.NoError(err)
	value, err = db.Get([]byte("p"), nil)
	require.NoError(err)
	require.Equal([]byte("v/p0"), value)

	// Delete existing key.
	batch.Clear()
	batch.Delete([]byte("p"))
	err = db.Write(batch, nil)
	require.NoError(err)
	_, err = db.Get([]byte("p"), nil)
	require.Equal(leveldb.ErrNotFound, err)

	// Put/Delete in sequence for same key
	batch.Clear()
	batch.Put([]byte("k"), []byte("v/k0"))
	batch.Delete([]byte("k"))
	err = db.Write(batch, nil)
	require.NoError(err)
	_, err = db.Get([]byte("k"), nil)
	require.Equal(leveldb.ErrNotFound, err)

	// Delete non exist key.
	batch.Clear()
	batch.Delete([]byte("k"))
	err = db.Write(batch, nil)
	require.NoError(err)
	_, err = db.Get([]byte("k"), nil)
	require.Equal(leveldb.ErrNotFound, err)

	// Delete/Put in sequence for same key.
	batch.Clear()
	batch.Delete([]byte("p"))
	batch.Put([]byte("p"), []byte("v/p1"))
	err = db.Write(batch, nil)
	require.NoError(err)
	value, err = db.Get([]byte("p"), nil)
	require.NoError(err)
	require.Equal([]byte("v/p1"), value)

	// Overwrite existing key.
	batch.Clear()
	batch.Put([]byte("p"), []byte("v/p2"))
	err = db.Write(batch, nil)
	require.NoError(err)
	value, err = db.Get([]byte("p"), nil)
	require.NoError(err)
	require.Equal([]byte("v/p2"), value)

	// Put twice for same key.
	batch.Clear()
	batch.Put([]byte("p"), []byte("v/p3"))
	batch.Put([]byte("p"), []byte("v/p4"))
	err = db.Write(batch, nil)
	require.NoError(err)
	value, err = db.Get([]byte("p"), nil)
	require.NoError(err)
	require.Equal([]byte("v/p4"), value)
}

type Operation interface {
	Operation()
}

type ModifyOperation interface {
	Operation
	ModifyOperation()
}

type VerifyOperation interface {
	Operation
	VerifyOperation()
}

type Operator struct{}

func (op Operator) Operation() {}

type WriteOperator struct {
	Operator
}

func (WriteOperator) ModifyOperation() {}

type VerifyOperator struct {
	Operator
}

func (VerifyOperator) VerifyOperation() {}

type PutOperation interface {
	ModifyOperation

	Put()

	Key() []byte
	Value() []byte
	Options() *leveldb.WriteOptions
}

type putOperator struct {
	WriteOperator
	key     []byte
	value   []byte
	options *leveldb.WriteOptions
}

var _ PutOperation = (*putOperator)(nil)

func (pt *putOperator) Put() {}

func (pt *putOperator) Key() []byte {
	return pt.key
}

func (pt *putOperator) Value() []byte {
	return pt.value
}

func (pt *putOperator) Options() *leveldb.WriteOptions {
	return pt.options
}

func putString(key, value string, options *leveldb.WriteOptions) PutOperation {
	return &putOperator{key: []byte(key), value: []byte(value), options: options}
}

type DeleteOperation interface {
	ModifyOperation

	Delete()

	Key() []byte
	Options() *leveldb.WriteOptions
}

type deleteOperator struct {
	WriteOperator
	key     []byte
	options *leveldb.WriteOptions
}

var _ DeleteOperation = (*deleteOperator)(nil)

func (dt *deleteOperator) Delete() {}

func (dt *deleteOperator) Key() []byte {
	return dt.key
}

func (dt *deleteOperator) Options() *leveldb.WriteOptions {
	return dt.options
}

func deleteString(key string, options *leveldb.WriteOptions) DeleteOperation {
	return &deleteOperator{key: []byte(key), options: options}
}

type WriteOperation interface {
	ModifyOperation
	Batch() leveldb.Batch
	Options() *leveldb.WriteOptions
}

type writeOperator struct {
	WriteOperator
	batch   leveldb.Batch
	options *leveldb.WriteOptions
}

var _ WriteOperation = (*writeOperator)(nil)

func (wt *writeOperator) Batch() leveldb.Batch {
	return wt.batch
}

func (wt *writeOperator) Options() *leveldb.WriteOptions {
	return wt.options
}

func writeOperations(operations []ModifyOperation, options *leveldb.WriteOptions) WriteOperation {
	var batch leveldb.Batch
	for _, v := range operations {
		switch op := v.(type) {
		case PutOperation:
			batch.Put(op.Key(), op.Value())
		case DeleteOperation:
			batch.Delete(op.Key())
		default:
			s := fmt.Sprintf("unexpected write op of type %s", reflect.TypeOf(v))
			panic(s)
		}
	}
	return &writeOperator{batch: batch, options: options}
}

type Verifier interface {
	Verify(suite *DBTestSuite, r leveldb.Reader)
}

type GetOperation interface {
	VerifyOperation

	Key() []byte
	Options() *leveldb.ReadOptions
	Value() []byte
}

type getOperator struct {
	VerifyOperator

	key     []byte
	value   []byte
	options *leveldb.ReadOptions
}

func (gt *getOperator) Key() []byte {
	return gt.key
}

func (gt *getOperator) Options() *leveldb.ReadOptions {
	return gt.options
}

func (gt *getOperator) Value() []byte {
	return gt.value
}

func getString(key, value string) GetOperation {
	return &getOperator{key: []byte(key), value: []byte(value)}
}

func notFoundString(key string) GetOperation {
	return &getOperator{key: []byte(key)}
}

type IterateOperation interface {
	VerifyOperation

	Iterate(r leveldb.Reader) leveldb.Iterator
	Entries() []entry
}

type allOperator struct {
	VerifyOperator
	options *leveldb.ReadOptions
	entries []entry
}

var _ Verifier = (*allOperator)(nil)
var _ IterateOperation = (*allOperator)(nil)

func (a *allOperator) Iterate(r leveldb.Reader) leveldb.Iterator {
	return r.All(a.options)
}

func (a *allOperator) Entries() []entry {
	return a.entries
}

func (a *allOperator) verifyIterator(suite *DBTestSuite, it leveldb.Iterator) {
	defer it.Close()
	suite.assertIterator(it, a.Entries())
}

func (a *allOperator) Verify(suite *DBTestSuite, r leveldb.Reader) {
	a.verifyIterator(suite, r.All(a.options))
	a.verifyIterator(suite, r.Find(nil, a.options))
	a.verifyIterator(suite, r.Prefix(nil, a.options))
	a.verifyIterator(suite, r.Range(nil, nil, a.options))
}

type findOperator struct {
	VerifyOperator
	start   []byte
	options *leveldb.ReadOptions
	entries []entry
}

var _ IterateOperation = (*findOperator)(nil)

func (f *findOperator) Iterate(r leveldb.Reader) leveldb.Iterator {
	return r.Find(f.start, f.options)
}

func (f *findOperator) Entries() []entry {
	return f.entries
}

type rangeOperator struct {
	VerifyOperator
	start   []byte
	limit   []byte
	options *leveldb.ReadOptions
	entries []entry
}

var _ IterateOperation = (*rangeOperator)(nil)

func (rg *rangeOperator) Iterate(r leveldb.Reader) leveldb.Iterator {
	return r.Range(rg.start, rg.limit, rg.options)
}

func (rg *rangeOperator) Entries() []entry {
	return rg.entries
}

type prefixOperator struct {
	VerifyOperator
	prefix  []byte
	options *leveldb.ReadOptions
	entries []entry
}

var _ IterateOperation = (*prefixOperator)(nil)

func (px *prefixOperator) Iterate(r leveldb.Reader) leveldb.Iterator {
	return r.Prefix(px.prefix, px.options)
}

func (px *prefixOperator) Entries() []entry {
	return px.entries
}

type getVerifier struct {
	GetOperation
}

var _ Verifier = getVerifier{}

func (verifier getVerifier) Verify(suite *DBTestSuite, r leveldb.Reader) {
	require := suite.Require()
	value, err := r.Get(verifier.Key(), verifier.Options())
	switch expectedValue := verifier.Value(); expectedValue {
	case nil:
		require.Equal(leveldb.ErrNotFound, err)
		require.Nil(value)
	default:
		require.NoError(err)
		require.Equal(expectedValue, value)
	}
}

type iterateVerifier struct {
	IterateOperation
}

var _ Verifier = iterateVerifier{}

func (verifier iterateVerifier) Verify(suite *DBTestSuite, r leveldb.Reader) {
	it := verifier.Iterate(r)
	defer it.Close()
	suite.assertIterator(it, verifier.Entries())
}

func (suite *DBWriteTestSuite) verify(verifier Verifier) {
	verifier.Verify(&suite.DBTestSuite, suite.db)
	snapshot := suite.db.Snapshot()
	defer snapshot.Close()
	verifier.Verify(&suite.DBTestSuite, snapshot)
}

func (suite *DBWriteTestSuite) TestWriteOperation() {
	operations := []Operation{
		putString("c", "v/c/0", nil),
		putString("g", "v/g/0", nil),
		putString("k0", "v/k0/0", nil),
		putString("o0", "v/o0/0", nil),
		putString("u0", "v/u0/0", nil),
		putString("x", "v/x/0", nil),
		putString("z", "v/z/0", nil),
		getString("c", "v/c/0"),
		getString("g", "v/g/0"),
		getString("x", "v/x/0"),
		notFoundString("k"),
		notFoundString("o"),
		putString("o", "v/o/0", nil),
		deleteString("x", nil),
		getString("o", "v/o/0"),
		writeOperations([]ModifyOperation{
			deleteString("k", nil),
			putString("o0", "v/o0/1", nil),
			putString("o1", "v/o1/0", nil),
			putString("o01", "v/o01/0", nil),
		}, nil),
		&allOperator{
			entries: []entry{
				&stringEntry{key: "c", value: "v/c/0"},
				&stringEntry{key: "g", value: "v/g/0"},
				&stringEntry{key: "k0", value: "v/k0/0"},
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
				&stringEntry{key: "o1", value: "v/o1/0"},
				&stringEntry{key: "u0", value: "v/u0/0"},
				&stringEntry{key: "z", value: "v/z/0"},
			},
		},
		&findOperator{
			start: []byte("h"),
			entries: []entry{
				&stringEntry{key: "k0", value: "v/k0/0"},
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
				&stringEntry{key: "o1", value: "v/o1/0"},
				&stringEntry{key: "u0", value: "v/u0/0"},
				&stringEntry{key: "z", value: "v/z/0"},
			},
		},
		&findOperator{
			start: []byte("k"),
			entries: []entry{
				&stringEntry{key: "k0", value: "v/k0/0"},
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
				&stringEntry{key: "o1", value: "v/o1/0"},
				&stringEntry{key: "u0", value: "v/u0/0"},
				&stringEntry{key: "z", value: "v/z/0"},
			},
		},
		&findOperator{
			start: []byte("k0"),
			entries: []entry{
				&stringEntry{key: "k0", value: "v/k0/0"},
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
				&stringEntry{key: "o1", value: "v/o1/0"},
				&stringEntry{key: "u0", value: "v/u0/0"},
				&stringEntry{key: "z", value: "v/z/0"},
			},
		},
		&findOperator{
			start: []byte("k1"),
			entries: []entry{
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
				&stringEntry{key: "o1", value: "v/o1/0"},
				&stringEntry{key: "u0", value: "v/u0/0"},
				&stringEntry{key: "z", value: "v/z/0"},
			},
		},
		&rangeOperator{
			start: []byte("o"),
			entries: []entry{
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
				&stringEntry{key: "o1", value: "v/o1/0"},
				&stringEntry{key: "u0", value: "v/u0/0"},
				&stringEntry{key: "z", value: "v/z/0"},
			},
		},
		&rangeOperator{
			limit: []byte("o"),
			entries: []entry{
				&stringEntry{key: "c", value: "v/c/0"},
				&stringEntry{key: "g", value: "v/g/0"},
				&stringEntry{key: "k0", value: "v/k0/0"},
			},
		},
		&rangeOperator{
			start: []byte("o"),
			limit: []byte("o0"),
			entries: []entry{
				&stringEntry{key: "o", value: "v/o/0"},
			},
		},
		&rangeOperator{
			start: []byte("o"),
			limit: []byte("o1"),
			entries: []entry{
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
			},
		},
		&rangeOperator{
			start: []byte("o"),
			limit: []byte("o01"),
			entries: []entry{
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
			},
		},
		&prefixOperator{
			prefix: []byte("o"),
			entries: []entry{
				&stringEntry{key: "o", value: "v/o/0"},
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
				&stringEntry{key: "o1", value: "v/o1/0"},
			},
		},
		&prefixOperator{
			prefix: []byte("o0"),
			entries: []entry{
				&stringEntry{key: "o0", value: "v/o0/1"},
				&stringEntry{key: "o01", value: "v/o01/0"},
			},
		},
		&prefixOperator{
			prefix:  []byte("o2"),
			entries: []entry{},
		},
	}
	db := suite.db
	require := suite.Require()
	for _, operation := range operations {
		switch op := operation.(type) {
		case PutOperation:
			err := db.Put(op.Key(), op.Value(), op.Options())
			require.NoError(err)
		case DeleteOperation:
			err := db.Delete(op.Key(), op.Options())
			require.NoError(err)
		case WriteOperation:
			err := db.Write(op.Batch(), op.Options())
			require.NoError(err)
		case Verifier:
			suite.verify(op)
		case GetOperation:
			suite.verify(getVerifier{op})
		case IterateOperation:
			suite.verify(iterateVerifier{op})
		default:
			err := fmt.Errorf("unexpected operation type: %s", reflect.TypeOf(operation))
			panic(err)
		}
	}
}

func TestDBWrite(t *testing.T) {
	suite.Run(t, &DBWriteTestSuite{DBTestSuite{setupPerTest: true}})
}
