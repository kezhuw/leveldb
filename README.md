# LevelDB implementation in pure Go
This is a LevelDB implementation, written in pure Go.

[![Build Status](https://travis-ci.org/kezhuw/leveldb.svg?branch=master)](https://travis-ci.org/kezhuw/leveldb)
[![Go Report Card](https://goreportcard.com/badge/github.com/kezhuw/leveldb)](https://goreportcard.com/report/github.com/kezhuw/leveldb)
[![codecov](https://codecov.io/gh/kezhuw/leveldb/branch/master/graph/badge.svg)](https://codecov.io/gh/kezhuw/leveldb)
[![GoDoc](https://godoc.org/github.com/kezhuw/leveldb?status.svg)](http://godoc.org/github.com/kezhuw/leveldb)

**This project is in early stage of development, and should not be considered as stable. Don't use it in production!**


## Goals
This project aims to write an pure Go implementation for LevelDB. It conforms to LevelDB storage format, but not
implementation details of official. This means that there may be differences in implementation with official.


## Features

- **Clean interface**  
The only exporting package is the top level package. All other packages are internal, and should not used by clients.

- **Concurrent compactions**  
Client can control concurrency of compactions through CompactionConcurrency option.


## Usage

### Create or open a database
```go
options := &leveldb.Options{
	CreateIfMissing: true,
	// ErrorIfExists:   true,
	// Filter: leveldb.NewBloomFilter(16),
}
db, err := leveldb.Open("dbname", options)
if err != nil {
	// Handing failure
}
defer db.Close()
```

### Reads and writes
```go
var db *leveldb.DB
var err error
var key, value []byte

// Put key value pair to db
err = db.Put(key, value, nil)

// Read key's value from db
value, err = db.Get(key, nil)
if err == leveldb.ErrNotFound {
	// Key not found
}

// Delete key from db
err = db.Delete(key, nil)
```

### Batch writes
```go
var db *leveldb.DB
var err error
var key, value []byte

var batch leveldb.Batch
batch.Put(key, value)
batch.Delete(key)

err = db.Write(batch, nil)
```


### Iteration
```go
var db *leveldb.DB
var it leveldb.Iterator
var err error
var start, limit, prefix []byte

// All keys from db
it = db.All(nil)
defer it.Close()
for it.Next() {
	fmt.Printf("key: %x, value: %x\n", it.Key(), it.Value())
}
err = it.Err()


// All keys greater than or equal to given key from db
it = db.Find(start, nil)
defer it.Close()
for it.Next() {
}
err = it.Err()


// All keys in range [start, limit) from db
it = db.Range(start, limit, nil)
defer it.Close()
for it.Next() {
}
err = it.Err()


// All keys starts with prefix from db
it = db.Prefix(prefix, nil)
defer it.Close()
for it.Next() {
}
err = it.Err()
```

### Snapshots
```go
var db *leveldb.DB
var key, start, limit, prefix []byte

snapshot := db.GetSnapshot()
defer snapshot.Close()

// Dup an snapshot for usage in another goroutine
go func(ss *leveldb.Snapshot) {
	defer ss.Close()
	it := ss.Prefix(prefix, nil)
	defer it.Close()
	for it.Next() {
	}
}(snapshot.Dup())


// Use snapshot in this goroutine
value, err := snapshot.Get(key, nil)

it := snapshot.Range(start, limit, nil)
defer it.Close()
for it.Next() {
}
```

## Benchmarks
See [kezhuw/go-leveldb-benchmarks][go-leveldb-benchmarks].

## TODO
- [ ] Source code documentation [WIP]
- [ ] Tests [WIP]
- [ ] Logging
- [ ] Abstract cache interface, so we can share cache among multiple LevelDB instances
- [ ] Reference counting openning file collection, don't rely on GC
- [ ] Statistics
- [x] Benchmarks, See [kezhuw/go-leveldb-benchmarks][go-leveldb-benchmarks].
- [x] Concurrent level compaction
- [ ] Replace hardcoded constants with configurable options
- [ ] Automatic adjustment of volatile options


## License
The MIT License (MIT). See [LICENSE](LICENSE) for the full license text.


## Contribution
Before going on, make sure you agree on [The MIT License (MIT).](LICENSE).

This project is far from complete, lots of things are missing. If you find any bugs or complement any parts of missing,
throw me a pull request.


## Acknowledgments
* [google/leveldb](https://github.com/google/leveldb) Official implementation written in in C++.  
  My knownledge of LevelDB storage format and implementation details mainly comes from this implementation.

* [syndtr/goleveldb](https://github.com/syndtr/goleveldb) Complete and stable Go implementation.  
  The `DB.Range`, `DB.Prefix`, and `filter.Generator` interface origin from this implementation.

* [golang/leveldb](https://github.com/golang/leveldb) Official but incomplete Go implementation.  
  The `Batch` and `Comparator.AppendSuccessor` origins from this implementation.

[go-leveldb-benchmarks]: https://github.com/kezhuw/go-leveldb-benchmarks

