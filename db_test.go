package leveldb_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/kezhuw/leveldb"
	"github.com/stretchr/testify/require"
)

var creatingOptions = &leveldb.Options{CreateIfMissing: true, ErrorIfExists: false}

func TestDbCompaction(t *testing.T) {
	dir, err := ioutil.TempDir("", "leveldb_test_")
	require.NotEmpty(t, dir, err)
	defer os.RemoveAll(dir)

	db, err := leveldb.Open(dir, creatingOptions)
	require.NotNilf(t, db, "fail to create db %s: %s", dir, err)
	defer db.Close()

	err = db.Put([]byte("a"), []byte("a0"), nil)
	require.Nil(t, err)

	err = db.Put([]byte("g"), []byte("g0"), nil)
	require.Nil(t, err)

	err = db.CompactRange(nil, nil)
	require.Nil(t, err)

	err = db.Put([]byte("f"), []byte("f0"), nil)
	require.Nil(t, err)

	err = db.Put([]byte("k"), []byte("k0"), nil)
	require.Nil(t, err)

	err = db.CompactRange(nil, nil)
	require.Nil(t, err)
}
