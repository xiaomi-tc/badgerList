package boltdb

import (
	"bytes"
	"errors"
	"github.com/dgraph-io/badger"
)

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

type Bucket struct {
	db         *badger.DB
	bucketName []byte
}

//func (b *Bucket) Hash(key []byte) (*Hash, error) {
//	if err := b.ensureType(key, HASH); err != nil {
//		return nil, err
//	}
//	return &Hash{bucket: b, key: key}, nil
//}

func (b *Bucket) List(key []byte) (*List, error) {
	if err := b.ensureType(key, LIST); err != nil {
		return nil, err
	}
	return &List{bucket: b, key: key}, nil
}

//func (b *Bucket) SortedSet(key []byte) (*SortedSet, error) {
//	if err := b.ensureType(key, SORTEDSET); err != nil {
//		return nil, err
//	}
//	return &SortedSet{bucket: b, key: key}, nil
//}

func (b *Bucket) ensureType(key []byte, elemType ElemType) error {
	if t, err := b.TypeOf(key); err != nil {
		return err
	} else if t != NONE && t != elemType {
		return ErrWrongType
	}
	return nil
}

func (b *Bucket) TypeOf(key []byte) (ElemType, error) {
	elemType := NONE
	err := b.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := bytes.Join([][]byte{KEY, key, SEP}, nil)
		if it.Seek(prefix); it.ValidForPrefix(prefix){
			item := it.Item()
			k := item.Key()
			t := bytes.TrimPrefix(k, prefix)
			elemType = ElemType(t[0])
		}
		return nil
	})
	return elemType, err
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	var val []byte
	err := b.View(func(txn *badger.Txn) error {
		//item, err := txn.Get(rawKey(key, STRING))
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)

		return err
		//val = bucket.Get(rawKey(key, STRING))
		//return nil
	})
	return val, err
}

func (b *Bucket) Set(key, value []byte) error {
	if err := b.ensureType(key, STRING); err != nil {
		return err
	}
	return b.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
		//return txn.Set(rawKey(key, STRING), value)
	})
}

// View make bolt.DB.View(Tx){tx.Bucket(...)} to View(bolt.Bucket)
func (b *Bucket) View(fn func(*badger.Txn) error) error {
	return b.db.View(func(txn *badger.Txn) error {
		return fn(txn)
	})
}

// Update make db.View easy to use
func (b *Bucket) Update(fn func(*badger.Txn) error) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return fn(txn)
	})
}
