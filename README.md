# LevelDB implementation in pure Go
This is a LevelDB implementation, written in pure Go.

[![GoDoc](https://godoc.org/github.com/kezhuw/leveldb?status.svg)](http://godoc.org/github.com/kezhuw/leveldb)
[![Build Status](https://travis-ci.org/kezhuw/leveldb.svg?branch=master)](https://travis-ci.org/kezhuw/leveldb)

**This project is in early stage of development, and should not be considered as stable. Don't use it in production!**


## Goals
This project aims to write an pure Go implementation for LevelDB. It conforms to LevelDB storage format, but not
implementation details of official. This means that there may be differences in implementation with official.


## Features

- **Clean interface**  
The only exporting package is the top level package. All other packages are internal, and should not used by clients.

- **Concurrent compactions**  
Currently, one level compaction can coexist with memtable compaction. It is possible to do concurrent level compactions
in the future.


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
- [ ] Concurrent level compaction
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

