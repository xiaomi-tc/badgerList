package boltdb

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"io/ioutil"
	"strconv"

	. "github.com/smartystreets/goconvey/convey"
	"path"
	"testing"
)

func TestString(t *testing.T) {
	db := newBoltDB(t)
	defer db.Close()

	Convey("DB Testing",t, func(){
		bucket, err := db.Bucket([]byte("0"))
		So(err,ShouldBeNil)

		key := rawKey([]byte("version"),STRING)
		err = bucket.Set(key, []byte("1.0.0"))
		//err = bucket.Set([]byte("version"), []byte("1.0.0"))
		So(err,ShouldBeNil)

		//val, err := bucket.Get([]byte("version"))
		val, err := bucket.Get(key)
		So(err,ShouldBeNil)
		So(string(val),ShouldEqual,string("1.0.0"))

		scan(db.db,[]byte("0"))
		elemType, err := bucket.TypeOf([]byte("version"))
		So(err,ShouldBeNil)
		So(elemType,ShouldEqual,STRING)

		prekey := "no-"
		preval := "val-"
		for i:=0; i< 10; i++ {
			key := prekey + strconv.Itoa(i)
			val := preval + strconv.Itoa(i)
			err = bucket.Set([]byte(key), []byte(val))
		}

		scan(db.db,[]byte("0"))
	})

}

func newBoltDB(t *testing.T) *DB {
	var db *DB

	dir, err := ioutil.TempDir("", "badger")
	Convey("newBadgerDB()",t, func() {
		So(err,ShouldBeNil)
		//ensure.Nil(t, err)

		dbpath := path.Join(dir, "badger.db")
		// log.Println("dbpath:", dbpath)
		opt := &Options{
			Dir:dbpath,
			ValueDir:dbpath,
			ReadOnly: false,
			ValueLogFileSize: 1024*1024*8,
		}
		db,err = Open(dbpath,opt)
		So(err,ShouldBeNil)
		//ensure.Nil(t, err)
	})

	return db
}

func scan(db *badger.DB, bucket []byte) {
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key:\"%v\" <--> value:\"%s\"\n", k, v)
				fmt.Printf("key:\"%s\" <--> value:\"%s\"\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {

	}
}
