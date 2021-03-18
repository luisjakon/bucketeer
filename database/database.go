package database

import (
	"fmt"
	"time"
)

type Option func(DB) error
type IteratorOption interface{}

type DB interface {
	Txn
	View(fn func(tx Txn) error) error
	Update(fn func(tx Txn) error) error
	Batch(b Batch) error

	Close() error
}

type Txn interface {
	Bucket
	Bucket(name []byte) Bucket
	ClearBucket(name []byte) error
}

type Bucket interface {
	Iterator(opts ...IteratorOption) Iterator

	List(cursor []byte, limit int, reverse bool) ([]*Entry, error)
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error

	CmpAndSwap(key, oldValue, newValue []byte) (bool, error)
}

type Iterator interface {
	Valid() bool

	FirstKey() []byte
	CurrentKey() []byte
	NextKey() []byte

	First() ([]byte, []byte)
	Current() ([]byte, []byte)
	Next() ([]byte, []byte)

	Close()
}

type Batch interface {
	GetOperations() []*TxnEntry
}

type Entry struct {
	Bucket []byte
	Key    []byte
	Value  []byte
}

type TxnEntry struct {
	Cmd      TxCmd
	BatchId  []byte
	Bucket   []byte
	Key      []byte
	Value    []byte
	CmpValue []byte
	// Where the result of Get or CmpAndSwap txns is stored.
	// Contains either error or bool values as result
	Result interface{}
	// If ts is same as batch time it means operation failed
	Timestamp int64
	Swapped   bool
}

func NewBatch() *batch {
	return &batch{Operations: []*TxnEntry{}}
}

type TxCmd int

const (
	Get TxCmd = iota
	Set
	Delete
	CmpAndSwap
)

func (o TxCmd) String() string {
	switch o {
	case Get:
		return "Get"
	case Set:
		return "Set"
	case Delete:
		return "Delete"
	case CmpAndSwap:
		return "CmpAndSwap"
	default:
		return fmt.Sprintf("unknown(%d)", o)
	}
}

type batch struct {
	Id         []byte
	Timestamp  int64
	Operations []*TxnEntry

	// internal for changing the timestamp of each operation
	lastCommitOrRollbackIndex int
}

func (tx *batch) GetOperations() []*TxnEntry {
	return tx.Operations
}

func (tx *batch) Get(bucket, key []byte) {
	tx.Operations = append(tx.Operations, &TxnEntry{
		Timestamp: tx.Timestamp,
		BatchId:   tx.Id,
		Bucket:    bucket,
		Key:       key,
		Cmd:       Get,
	})
}

func (tx *batch) Set(bucket, key, value []byte) {
	tx.Operations = append(tx.Operations, &TxnEntry{
		Timestamp: tx.Timestamp,
		BatchId:   tx.Id,
		Bucket:    bucket,
		Key:       key,
		Value:     value,
		Cmd:       Set,
	})
}

func (tx *batch) Del(bucket, key []byte) {
	tx.Operations = append(tx.Operations, &TxnEntry{
		Timestamp: tx.Timestamp,
		BatchId:   tx.Id,
		Bucket:    bucket,
		Key:       key,
		Cmd:       Delete,
	})
}

// Cas adds a new compare-and-swap query to the transaction.
func (tx *batch) CmpAndSwap(bucket, key, value []byte) {
	tx.Operations = append(tx.Operations, &TxnEntry{
		Timestamp: tx.Timestamp,
		BatchId:   tx.Id,
		Bucket:    bucket,
		Key:       key,
		Value:     value,
		Cmd:       CmpAndSwap,
	})
}

func UpdateBatchTimestamp(b Batch) int64 {
	now := time.Now().Unix()
	if b, ok := b.(*batch); ok {
		b.Timestamp = now
	}
	return now
}

func updateTimestamps(ops []*TxnEntry) int64 {
	// update all tx entries to the same timestamp as the commit
	now := time.Now().Unix()
	for _, op := range ops {
		op.Timestamp = now
	}
	return now
}
