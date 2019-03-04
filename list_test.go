package boltdb

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestList(t *testing.T) {
	db := newBoltDB(t)
	defer db.Close()

	Convey("TestList",t, func(){
		//var val []byte
		key := []byte("letter")
		bucket, _ := db.Bucket([]byte("1"))
		list, err := bucket.List(key)
		So(err,ShouldBeNil)


		// insert a, b, c, d
		err = list.RPush([]byte("d"),[]byte("c"),[]byte("b"), []byte("a"))
		//err = list.RPush([]byte("c"), []byte("d"))
		So(err,ShouldBeNil)
		size, err := list.Len()
		So(err,ShouldBeNil)
		So(size, ShouldEqual, int64(4))

		err = list.LPush([]byte("D"), []byte("C"))
		err = list.LPush([]byte("B"), []byte("A"))
		So(err,ShouldBeNil)


		size, err = list.Len()
		So(err,ShouldBeNil)
		So(size, ShouldEqual, int64(8))

		prefixScan(list.bucket,list.keyPrefix())

		err = list.Range(0, 3, func(i int64, value []byte, quit *bool) {
			// log.Println(i, string(value))
			fmt.Printf("Range() index=%v v=%v\n",i,string(value))
		})
		So(err,ShouldBeNil)

		var val []byte
		val, err = list.Index(0)
		So(err,ShouldBeNil)
		So(string(val),ShouldEqual,string("A"))

		val, err = list.LPop()
		So(err,ShouldBeNil)
		So(string(val),ShouldEqual,string("A"))

		size, err = list.Len()
		So(err,ShouldBeNil)
		So(size, ShouldEqual, int64(7))

		val, err = list.RPop()
		So(err,ShouldBeNil)
		So(string(val),ShouldEqual,string("a"))

		size, err = list.Len()
		So(err,ShouldBeNil)
		So(size, ShouldEqual, int64(6))

		prefixScan(list.bucket,list.keyPrefix())

		//scan(db.db, []byte("1"))

		list.RPop()
		//list.RPop()
		//list.LPop()
		//list.LPop()
		//list.LPop()
		//size, err = list.Len()
		//So(err,ShouldBeNil)
		//So(size, ShouldEqual, int64(1))
		list.LPop()

		size, err = list.Len()
		So(err,ShouldBeNil)
		So(size, ShouldEqual, int64(4))

		prefixScan(list.bucket,list.keyPrefix())

		// test new LBatchDelete() and RBatchDelete()
		list.LBatchDelete(2)
		size, err = list.Len()
		So(err,ShouldBeNil)
		So(size, ShouldEqual, int64(2))

		val, err = list.Index(0)
		So(err,ShouldBeNil)
		So(string(val),ShouldEqual,string("d"))

		val, err = list.Index(1)
		So(err,ShouldBeNil)
		So(string(val),ShouldEqual,string("c"))

		list.RBatchDelete(2)
		size, err = list.Len()
		So(err,ShouldBeNil)
		So(size,ShouldEqual,int64(0))
	})
	//scan(db.db, []byte("1"))
}


func prefixScan(bucket *Bucket,prefix []byte) {
	err :=bucket.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		//prefix := []byte("1234")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%v, value=%s\n", k, v)
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
