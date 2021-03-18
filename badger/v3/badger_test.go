package badger

import (
	"os"
	"testing"

	"github.com/luisjakon/bucketeer/database"
	"github.com/stretchr/testify/assert"
)

var path = "./tmp"

func TestMain(m *testing.M) {
	// setup
	os.MkdirAll(path, 0755)

	// run
	ret := m.Run()

	// teardown
	os.RemoveAll(path)
	os.Exit(ret)
}

func TestBadger(t *testing.T) {

	db, err := New("./tmp/badgerdb")
	assert.NoError(t, err)
	defer db.Close()

	testBucket(t, db)
	testTxn(t, db)
	testBatch(t, db)
}

func testBucket(t *testing.T, db DB) {

	bkt := []byte("mybucket")
	key0, val0 := []byte("mykey0"), []byte("myval0")
	key1, val1 := []byte("mykey1"), []byte("myval1")
	key2, val2 := []byte("mykey2"), []byte("myval2")
	key3, val3 := []byte("mykey3"), []byte("myval3")

	err := db.Update(func(tx Txn) error {

		bucket := tx.Bucket(bkt)

		////////////////////////////
		// Put
		////////////////////////////
		err := bucket.Put(key0, val0)
		if !assert.NoError(t, err) {
			return err
		}
		err = bucket.Put(key1, val1)
		if !assert.NoError(t, err) {
			return err
		}
		err = bucket.Put(key2, val2)
		if !assert.NoError(t, err) {
			return err
		}
		err = bucket.Put(key3, val3)
		if !assert.NoError(t, err) {
			return err
		}

		////////////////////////////
		// Get
		////////////////////////////
		ret, err := bucket.Get(key0)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, val0, ret)

		////////////////////////////
		// List
		////////////////////////////
		list, err := bucket.List(nil, 10, false)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, 4, len(list))
		assert.Equal(t, &Entry{bkt, key0, val0}, list[0])
		assert.Equal(t, &Entry{bkt, key1, val1}, list[1])
		assert.Equal(t, &Entry{bkt, key2, val2}, list[2])
		assert.Equal(t, &Entry{bkt, key3, val3}, list[3])

		////////////////////////////
		// Delete
		////////////////////////////
		err = bucket.Delete(key0)
		if !assert.NoError(t, err) {
			return err
		}
		list, err = bucket.List(nil, 10, false)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, 3, len(list))
		assert.Equal(t, &Entry{bkt, key1, val1}, list[0])
		assert.Equal(t, &Entry{bkt, key2, val2}, list[1])
		assert.Equal(t, &Entry{bkt, key3, val3}, list[2])

		////////////////////////////
		// Iterate
		////////////////////////////
		prefix := key1[:2]
		it := bucket.Iterator(WithPrefix(prefix))

		for key := it.FirstKey(); key != nil; key = it.NextKey() {
			assert.Equal(t, key1, key)

			err := bucket.Delete(key)
			if !assert.NoError(t, err) {
				return err
			}

			break
		}
		it.Close()

		list, err = bucket.List(nil, 10, false)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, 2, len(list))
		assert.Equal(t, &Entry{bkt, key2, val2}, list[0])
		assert.Equal(t, &Entry{bkt, key3, val3}, list[1])

		////////////////////////////
		// CmpAndSwap
		////////////////////////////
		swapped, err := bucket.CmpAndSwap(key2, val1, val3)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, false, swapped)

		ret, err = bucket.Get(key2)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, val2, ret)

		swapped, err = bucket.CmpAndSwap(key2, val2, val3)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, true, swapped)

		ret, err = bucket.Get(key2)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, val3, ret)

		////////////////////////////
		// ClearBucket
		////////////////////////////
		err = tx.ClearBucket(bkt)
		if !assert.NoError(t, err) {
			return err
		}

		list, err = bucket.List(nil, 10, false)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, 0, len(list))

		return err
	})
	if err != nil {
		t.Fatal(err.Error())
	}
}

func testTxn(t *testing.T, db DB) {
	err := db.Update(func(tx Txn) error {

		key0, val0 := []byte("mykey0"), []byte("myval0")
		key1, val1 := []byte("mykey1"), []byte("myval1")

		////////////////////////////
		// Put
		////////////////////////////
		err := tx.Put(key0, val0)
		if !assert.NoError(t, err) {
			return err
		}

		////////////////////////////
		// Get
		////////////////////////////
		ret, err := tx.Get(key0)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, val0, ret)

		err = tx.Put(key1, val1)
		if !assert.NoError(t, err) {
			return err
		}

		////////////////////////////
		// List
		////////////////////////////
		list, err := tx.List(nil, 10, false)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, 2, len(list))
		assert.Equal(t, &Entry{[]byte{byte(0xaa)}, key0, val0}, list[0])
		assert.Equal(t, &Entry{[]byte{byte(0xaa)}, key1, val1}, list[1])

		////////////////////////////
		// Delete
		////////////////////////////
		err = tx.Delete(key0)
		if !assert.NoError(t, err) {
			return err
		}
		list, err = tx.List(nil, 10, false)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, 1, len(list))
		assert.Equal(t, &Entry{[]byte{byte(0xaa)}, key1, val1}, list[0])

		////////////////////////////
		// Iterate
		////////////////////////////
		prefix := key1[:2]
		it := tx.Iterator(WithPrefix(prefix))
		defer it.Close()

		for key := it.FirstKey(); key != nil; key = it.NextKey() {
			assert.Equal(t, key1, key)

			err := tx.Delete(key)
			if !assert.NoError(t, err) {
				return err
			}
		}

		list, err = tx.List(nil, 10, false)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, 0, len(list))

		return err
	})
	if err != nil {
		t.Fatal(err.Error())
	}
}

func testBatch(t *testing.T, db DB) {

	bkt := []byte("mybucket")
	key0, val0 := []byte("mykey0"), []byte("myval0")
	key1, val1 := []byte("mykey1"), []byte("myval1")

	batch := database.NewBatch()

	batch.Set(bkt, key0, val0)
	batch.Set(bkt, key1, val1)

	batch.Get(bkt, key0)
	batch.Del(bkt, key0)
	batch.Get(bkt, key0)

	err := db.Batch(batch)
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, val0, batch.GetOperations()[2].Value)
	assert.Equal(t, []byte(nil), batch.GetOperations()[4].Value)

	err = db.View(func(tx Txn) error {
		bucket := tx.Bucket(bkt)

		_, err := bucket.Get(key0)
		assert.Equal(t, ErrNotFound, err)

		val, err := bucket.Get(key1)
		if !assert.NoError(t, err) {
			return err
		}
		assert.Equal(t, val1, val)

		return nil
	})
	assert.NoError(t, err)

}

// func Test_example(t *testing.T) {
// 	type args struct {
// 		val []byte
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 		want []byte
// 		err  error
// 	}{
// 		{"fail/input-too-long", args{make([]byte, 65536)}, nil, errors.New("length of input cannot be greater than 65535")},
// 		{"fail/input-empty", args{nil}, nil, errors.New("input cannot be empty")},
// 		{"ok", args{[]byte("hello")}, []byte{5, 0, 104, 101, 108, 108, 111}, nil},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			var somefn = func([]byte) (interface{}, error) { return nil, nil }
// 			got, err := somefn(tt.args.val)
// 			if err != nil {
// 				if assert.NotNil(t, tt.err) {
// 				}
// 			} else {
// 				if assert.Nil(t, tt.err) && assert.NotNil(t, got) && assert.NotNil(t, tt.want) {
// 					assert.Equal(t, got, tt.want)
// 				}
// 			}
// 		})
// 	}
// }
