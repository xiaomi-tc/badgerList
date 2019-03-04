package boltdb

import (
	"bytes"
	"errors"
	"github.com/dgraph-io/badger"

	"fmt"
)

// List ...
// +key,l = ""
// l[key]0 = "a"
// l[key]1 = "b"
// l[key]2 = "c"
type List struct {
	bucket *Bucket
	key    []byte
}

func (l *List) Index(i int64) ([]byte, error) {
	x, err := l.leftIndex()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Index(%v): left=%v\n",i,x)
	var val []byte
	err = l.bucket.View(func(txn *badger.Txn) error {
		val,err = l.bucket.Get(l.indexKey(x + i))
		//fmt.Printf("\nIndex() get k=%v  v=%v\n",l.indexKey(x + i),val)
		return err
	})
	return val, err
}

// Range enumerate value by index
// <start> must >= 0
// <stop> should equal to -1 or lager than <start>
func (l *List) Range(start, stop int64, fn func(i int64, value []byte, quit *bool)) error {
	if start < 0 || (stop != -1 && start > stop) {
		return errors.New("bad start/stop index")
	}

	fmt.Printf("Range(): start=%v    stop=%v\n",start,stop)

	x, y, err := l.rangeIndex()
	if err != nil {
		return err
	}
	if stop == -1 {
		stop = (y - x + 1) - 1 // (size) - 1
	}
	min := l.indexKey(x + int64(start))
	max := l.indexKey(x + int64(stop))

	return l.bucket.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		i := int64(0) // 0
		for it.Seek(min);it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			if k == nil || bytes.Compare(k, max) > 0 {
				break
			}
			quit := false
			item.Value(func(v []byte) error {
				fn(start+i, v, &quit)
				return nil
			})
			if quit {
				break
			}
			i++
		}
		return nil
	})
}

// RPush ...
func (l *List) RPush(vals ...[]byte) error {
	_, y, err := l.rangeIndex()
	isErr := false
	if err != nil {
		isErr = true
	}
	err = l.bucket.Update(func(txn *badger.Txn) error {
		if isErr {
			//fmt.Println("RPush: rawKey=" + string(l.rawKey()))
			l.bucket.Set(l.rawKey(), nil)
			y = ZEROPOINT -1  // start from 0
		}
		for i, val := range vals {
			//fmt.Printf("\nRPush: inputkey:=%v\n",y+int64(i)+1)
			l.bucket.Set(l.indexKey(y+int64(i)+1), val)
		}
		return nil
	})
	return err
}

// LPush ...
func (l *List) LPush(vals ...[]byte) error {
	x, _, err := l.rangeIndex()
	isErr := false
	if err != nil {
		isErr = true
	}
	err = l.bucket.Update(func(txn *badger.Txn) error {
		if isErr {
			//fmt.Println("LPush: rawKey=" + string(l.rawKey()))
			l.bucket.Set(l.rawKey(), nil)
			x = ZEROPOINT + 1 // start from 1
		}
		for i, val := range vals {
			//fmt.Printf("\nLPush: inputkey:=%v\n",x-int64(i)-1)
			l.bucket.Set(l.indexKey(x-int64(i)-1), val)
		}
		return nil
	})
	return err
}

// RPop ...
func (l *List) RPop() ([]byte, error) {
	return l.pop(false)
}

// LPop ...
func (l *List) LPop() ([]byte, error) {
	return l.pop(true)
}

func (l *List) pop(left bool) ([]byte, error) {
	x, y, err := l.rangeIndex()
	if err != nil {
		return nil, err
	}

	size := y - x + 1
	if size == 0 {
		return nil, nil
	} else if size < 0 { // double check
		return nil, errors.New("bad list struct")
	}

	var idxkey []byte
	if left {
		idxkey = l.indexKey(x)
	} else {
		idxkey = l.indexKey(y)
	}

	var val []byte
	err = l.bucket.Update(func(txn *badger.Txn) error {
		if val,err = l.bucket.Get(idxkey);err !=nil {
			return nil
		}
		if err := txn.Delete(idxkey); err != nil {
			return err
		}
		if size == 1 { // clean up
			return txn.Delete(l.rawKey())
		}
		return nil
	})

	return val, nil
}

// LBactchDelete ...
func (l *List) LBatchDelete(dCount int64) error {
	return l.batchDelete(dCount,true)
}

func (l *List) RBatchDelete(dCount int64) error {
	return l.batchDelete(dCount,false)
}

func (l *List) batchDelete(dCount int64,left bool) error {
	if dCount < 1  {
		return errors.New("bad delete count")
	}
	x, y, err := l.rangeIndex()

	if err != nil {
		return err
	}

	size := y - x + 1
	if size == 0 {
		return nil
	} else if size < 0 { // double check
		return errors.New("bad list struct")
	}

	count := dCount
	isClean := false

	if size <= dCount {
		count = size
		isClean = true
	}

	return l.bucket.Update(func(txn *badger.Txn) error {
		for i := int64(0); i<count; i++ {
			if left {
				if err := txn.Delete(l.indexKey(x + int64(i))); err != nil {
					return err
				}
			} else {
				if err := txn.Delete(l.indexKey(y - int64(i))); err != nil {
					return err
				}
			}
		}
		if isClean { // clean up
			return txn.Delete(l.rawKey())
		}
		return nil
	})
}


// Len ...
func (l *List) Len() (int64, error) {
	x, y, err := l.rangeIndex()
	if err != nil {
		return 0, nil
	}
	return y - x + 1, err
}

func (l *List) rangeIndex() (int64, int64, error) {
	left, err := l.leftIndex()
	if err != nil {
		return 0, -1, err
	}
	right, err := l.rightIndex()
	if err != nil {
		return 0, -1, err
	}
	//fmt.Printf("rangeIndex() left=%v, right=%v\n",left,right)
	return left, right, nil
}

func (l *List) leftIndex() (int64, error) {
	idx := int64(0) // default 0
	err := l.bucket.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		it := txn.NewIterator(opt)
		defer it.Close()

		prefix := l.keyPrefix()
		it.Seek(prefix)
		//fmt.Printf("\nleftIndex(): prefix=%v  it.Valid()=%v\n",prefix,it.Valid())

		if it.ValidForPrefix(prefix){
			item := it.Item()
			k := item.Key()
			idx = l.indexInKey(k)
			//fmt.Printf("left=%v ",idx)
			return nil
		}

		return errors.New("No find left Index")
	})
	//fmt.Printf("return-left=%v\n\n",idx)
	return idx, err
}

func (l *List) rightIndex() (int64, error) {
	idx := int64(0) // default 0
	err := l.bucket.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Reverse = true
		it := txn.NewIterator(opt)
		defer it.Close()
		prefix := l.keyPrefix()
		pre := append(l.keyPrefix(), 0xff)
		it.Seek(pre)
		//fmt.Printf("\nrightIndex(): prefix=%v  it.Valid()=%v\n",pre,it.Valid())

		if it.ValidForPrefix(prefix){
		//if it.Seek(prefix); it.ValidForPrefix(prefix){
			item := it.Item()
			k := item.Key()
			idx = l.indexInKey(k)
			//fmt.Printf("right=%v  ",idx)
			return nil
		}
		return errors.New("No find right Index")
	})
	//fmt.Printf("return-right=%v\n",idx)
	return idx, err
}

// +key,l = ""
func (l *List) rawKey() []byte {
	return rawKey(l.key, ElemType(LIST))
}

// l[key]0
func (l *List) keyPrefix() []byte {
	sign := []byte{0} // 干扰隔离字段
	return bytes.Join([][]byte{[]byte{byte(LIST)}, SOK, l.key, EOK, sign}, nil)
}

// l[key]00 = "a"
func (l *List) indexKey(i int64) []byte {
	return bytes.Join([][]byte{l.keyPrefix(), itob(i)}, nil)
}

// split l[key]index into index
func (l *List) indexInKey(key []byte) int64 {
	idxbuf := bytes.TrimPrefix(key, l.keyPrefix())
	return btoi(idxbuf)
}
