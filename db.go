package boltdb

import (
	"sync"

	"github.com/dgraph-io/badger"
)

// Options is another name of badger.Options
// Options {Timeout: 0, ReadOnly: false}
type Options badger.Options

// Open ...
// <opt> alias of bolt.Options
// db, err := badger.Open("/tmp/demo.db", nil)
// bucket, err := db.Bucket("0")
//
// hash, err := bucket.Hash("user:100422")
// val, err := hash.Get("name")
// list, err := bucket.List("userlist")
// list.RPush("a", "b", "c")
// item, err := list.LPop()
func Open(dbpath string, opt *Options) (*DB, error) {
	opts := badger.DefaultOptions
	opts.Dir = dbpath
	opts.ValueDir = dbpath

	if opt ==nil {
		opts.ReadOnly = false
	} else {
		//opts = (*badger.Options)(opt)
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:      db,
		buckets: map[string]*Bucket{},
	}, nil
}

type DB struct {
	db      *badger.DB
	mu      sync.Mutex
	buckets map[string]*Bucket
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) Bucket(name []byte) (*Bucket, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	bucket, exists := d.buckets[string(name)]
	if !exists {

		bucket = &Bucket{db: d.db, bucketName: name}
		d.buckets[string(name)] = bucket
	}

	return bucket, nil
}
