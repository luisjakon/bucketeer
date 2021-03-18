package badger

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/luisjakon/bucketeer/database"
	"github.com/pkg/errors"
)

const (
	DB_ROOT = "./"
	DB_DIR  = "data"
)

var (
	ErrNotImplemented error = fmt.Errorf("Not implemented")
	ErrNotFound       error = fmt.Errorf("Not found")
	ErrInvalidFormat  error = fmt.Errorf("Invalid format")
	ErrInvalidOption  error = fmt.Errorf("Invalid option")
)

var defaultBucket = []byte{0xAA} // default bucket

type DB = database.DB
type Txn = database.Txn
type Batch = database.Batch
type Bucket = database.Bucket
type Iterator = database.Iterator
type TxnEntry = database.TxnEntry
type TxCmd = database.TxCmd
type Entry = database.Entry
type Option = database.Option
type IteratorOption = database.IteratorOption

// Iterator options
type (
	prefix   []byte
	keysonly struct{}
)

type db struct {
	db   *badger.DB
	opts badger.Options
}

type tx struct {
	txn *badger.Txn
}

type bucket struct {
	tx   *tx
	name []byte
}

type iterator struct {
	it     *badger.Iterator
	prefix []byte
}

func WithValueDir(dir string) Option {
	return func(d DB) error {
		if l, ok := d.(*db); ok {
			l.opts.ValueDir = dir
			return nil
		}
		return ErrInvalidOption
	}
}

func WithInMemoryDB() Option {
	return func(d DB) error {
		if l, ok := d.(*db); ok {
			l.opts.InMemory = true
			return nil
		}
		return ErrInvalidOption
	}
}

func WithBadgerOptions(options badger.Options) Option {
	return func(d DB) error {
		if l, ok := d.(*db); ok {
			l.opts = options
			return nil
		}
		return ErrInvalidOption
	}
}

func WithBadgerIteratorOptions(options badger.IteratorOptions) IteratorOption {
	return &options
}

func WithPrefix(p []byte) IteratorOption {
	return prefix(p)
}

func WithKeysOnly() IteratorOption {
	return keysonly(struct{}{})
}

func New(path string, options ...Option) (DB, error) {
	dbase := &db{}
	return dbase, dbase.Open(path, options...)
}

func (db *db) Open(path string, options ...Option) error {

	dir, err := ensurePath(path)
	if err != nil {
		return err
	}
	db.opts = badger.DefaultOptions(dir)

	// apply options
	for _, apply := range options {
		apply(db)
	}

	bdb, err := badger.Open(db.opts)
	if err != nil {
		return err
	}

	for {
		if bdb.RunValueLogGC(0.5) != nil {
			break
		}
	}

	db.db = bdb
	return nil
}

func (db *db) Close() error {
	return db.db.Close()
}

func autoDiscard(txn *badger.Txn, duration time.Duration) {
	go func() {
		ctx, cancel := context.WithTimeout(context.TODO(), duration)
		defer cancel()
		select {
		case <-ctx.Done():
			txn.Discard()
		}
	}()
}

func txn(db *db) *tx {
	return &tx{db.db.NewTransaction(false)}
}

func mtxn(db *db) *tx {
	return &tx{db.db.NewTransaction(true)}
}

func (db *db) Bucket(name []byte) Bucket {
	txn := mtxn(db)
	autoDiscard(txn.txn, 100*time.Millisecond) // discard required by badger
	return txn.Bucket(name)
}

func (db *db) ClearBucket(name []byte) error {
	txn := txn(db)
	defer txn.discard()
	return txn.ClearBucket(name)
}

func (db *db) List(cursor []byte, limit int, inReverse bool) (res []*Entry, err error) {
	txn := txn(db)
	defer txn.discard()
	return txn.List(cursor, limit, inReverse)
}

func (db *db) Get(key []byte) ([]byte, error) {
	txn := txn(db)
	defer txn.discard()
	return txn.Get(key)
}

func (db *db) Put(key, value []byte) error {
	txn := txn(db)
	defer txn.discard()
	return txn.Put(key, value)
}

func (db *db) Delete(key []byte) error {
	txn := txn(db)
	defer txn.discard()
	return txn.Delete(key)
}

func (db *db) CmpAndSwap(key []byte, oldValue, newValue []byte) (bool, error) {
	txn := txn(db)
	defer txn.discard()
	return txn.CmpAndSwap(key, oldValue, newValue)
}

func (db *db) Iterator(opts ...IteratorOption) Iterator {
	txn := txn(db)
	defer txn.discard()
	return txn.Iterator(opts...)
}

func (db *db) View(fn func(tx Txn) error) error {
	return db.db.View(
		func(txn *badger.Txn) error {
			return fn(&tx{txn})
		})
}

func (db *db) Update(fn func(tx Txn) error) error {
	return db.db.Update(
		func(txn *badger.Txn) error {
			return fn(&tx{txn})
		})
}

func (db *db) Batch(b Batch) error {
	database.UpdateBatchTimestamp(b)
	return db.Update(func(tx Txn) error {
		ops := b.GetOperations()
		if len(ops) == 0 {
			return nil
		}
		for _, op := range ops {
			bucket := tx.Bucket(op.Bucket)
			switch op.Cmd {
			case database.Get:
				v, err := bucket.Get(op.Key)
				if err != nil {
					op.Result = err
					continue
				}
				op.Value = v
				op.Timestamp = time.Now().Unix()
			case database.Set:
				err := bucket.Put(op.Key, op.Value)
				if err != nil {
					op.Result = err
					continue
				}
				op.Timestamp = time.Now().Unix()
			case database.Delete:
				err := bucket.Delete(op.Key)
				if err != nil {
					op.Result = err
					continue
				}
				op.Timestamp = time.Now().Unix()
			case database.CmpAndSwap:
				swapped, err := bucket.CmpAndSwap(op.Key, op.CmpValue, op.Value)
				if err != nil {
					op.Result = err
					continue
				}
				op.Swapped = swapped
				op.Timestamp = time.Now().Unix()
			}

		}
		return nil
	})
}

func (tx *tx) Bucket(name []byte) Bucket {
	bkt := name
	if len(name) == 0 {
		bkt = defaultBucket
	}
	return &bucket{
		name: bkt,
		tx:   tx,
	}
}

func (tx *tx) ClearBucket(name []byte) error {
	b := tx.Bucket(name)
	c := b.Iterator()
	defer c.Close()
	for key := c.FirstKey(); key != nil; key = c.NextKey() {
		err := b.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tx *tx) List(cursor []byte, limit int, inReverse bool) (res []*Entry, err error) {
	return tx.Bucket(defaultBucket).List(cursor, limit, inReverse)
}

func (tx *tx) Get(key []byte) ([]byte, error) {
	return tx.Bucket(defaultBucket).Get(key)
}

func (tx *tx) Put(key, value []byte) error {
	return tx.Bucket(defaultBucket).Put(key, value)
}

func (tx *tx) Delete(key []byte) error {
	return tx.Bucket(defaultBucket).Delete(key)
}

func (tx *tx) CmpAndSwap(key []byte, oldValue, newValue []byte) (bool, error) {
	return tx.Bucket(defaultBucket).CmpAndSwap(key, oldValue, newValue)
}

func (tx *tx) Iterator(opts ...IteratorOption) Iterator {
	return tx.Bucket(defaultBucket).Iterator(opts...)
}

func (tx *tx) discard() {
	tx.txn.Discard()
}

func (b *bucket) List(cursor []byte, limit int, reverse bool) (res []*Entry, err error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Reverse = reverse
	opts.PrefetchSize = limit
	opts.Prefix = cursor

	c := b.Iterator(&opts)
	defer c.Close()

	for key := c.FirstKey(); key != nil; key = c.NextKey() {
		k, v := c.Current()
		entry := &Entry{
			Bucket: b.name,
			Key:    k,
			Value:  v,
		}
		res = append(res, entry)
		if len(res) >= limit {
			break
		}
	}

	return res, nil
}

func (b *bucket) Get(key []byte) ([]byte, error) {
	keyWithPrefix := append(b.name, key...)
	item, err := b.tx.txn.Get(keyWithPrefix)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		return nil, Error(err, "bucket: %s, key: %s - %s", string(b.name), key, err.Error())
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, Error(err, "bucket: %s, key: %s - %s", string(b.name), key, err.Error())
	}
	return value, nil
}

func (b *bucket) Put(key, value []byte) error {
	keyWithPrefix := append(b.name, key...)
	err := b.tx.txn.Set(keyWithPrefix, value)
	if err != nil {
		return err
	}
	return nil
}

func (b *bucket) Delete(key []byte) error {
	keyWithPrefix := append(b.name, key...)
	err := b.tx.txn.Delete(keyWithPrefix)
	if err != nil {
		return err
	}
	return nil
}

func (b *bucket) CmpAndSwap(key, oldValue, newValue []byte) (bool, error) {
	dbValue, err := b.Get(key)
	if err != nil && !IsErrNotFound(err) {
		return false, err
	}

	if !bytes.Equal(dbValue, oldValue) {
		return false, nil
	}

	if err := b.Put(key, newValue); err != nil {
		return false, Error(err, "failed to set %s", key)
	}

	return true, nil
}

func (b *bucket) Iterator(options ...IteratorOption) Iterator {
	var opts *badger.IteratorOptions
	if opts == nil {
		o := badger.DefaultIteratorOptions
		opts = &o
		opts.Prefix = b.name
		opts.PrefetchValues = false
	}

	for _, o := range options {
		switch opt := o.(type) {
		case keysonly:
			opts.PrefetchValues = false
		case prefix:
			// Ensure original prefix is maintained on sub-prefixed iterator
			// NOTE: this is an additive operation, use with care
			opts.Prefix = append(opts.Prefix, opt...)
		case *badger.IteratorOptions:
			// Ensure bucket name is prefixed before replacing
			opt.Prefix = append(b.name, opt.Prefix...)
			opts = opt
		}
	}

	it := b.tx.txn.NewIterator(*opts)
	return &iterator{it: it, prefix: b.name}
}

func (c *iterator) Valid() bool {
	return c.it.ValidForPrefix(c.prefix)
}

func (c *iterator) FirstKey() []byte {
	c.it.Seek(c.prefix)
	return c.CurrentKey()
}

func (c *iterator) CurrentKey() []byte {
	if !c.it.ValidForPrefix(c.prefix) {
		return nil
	}
	item := c.it.Item()
	return item.Key()[len(c.prefix):]
}

func (c *iterator) NextKey() []byte {
	c.it.Next()
	return c.CurrentKey()
}

func (c *iterator) First() ([]byte, []byte) {
	c.it.Seek(c.prefix)
	return c.Current()
}

func (c *iterator) Current() ([]byte, []byte) {
	if !c.it.ValidForPrefix(c.prefix) {
		return nil, nil
	}
	item := c.it.Item()
	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, nil
	}
	return item.Key()[len(c.prefix):], value
}

func (c *iterator) Next() ([]byte, []byte) {
	c.it.Next()
	return c.Current()
}

func (c *iterator) Close() {
	c.it.Close()
}

func Error(cause error, msg string, args ...interface{}) error {
	if len(args) == 0 {
		return errors.Wrap(cause, msg)
	}
	return errors.Wrapf(cause, msg, args...)
}

func IsErrNotFound(err error) bool {
	return err == ErrNotFound || cause(err) == ErrNotFound
}

func cause(err error) error {
	type causer interface {
		Cause() error
	}

	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return err
}

func ensurePath(path string) (dir string, err error) {
	if path == "" {
		if dir, err = os.Getwd(); err != nil {
			return
		}
		dir = filepath.Join(dir, DB_DIR)
	} else {
		dir = path
	}
	// Ensure directory exits
	err = os.MkdirAll(dir, 0700)
	return
}
