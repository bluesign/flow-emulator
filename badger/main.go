package badger

import (
	"github.com/pkg/errors"
)

func Entries(prefix []byte) []*Item {
	items := make([]*Item, 0)
	return items
}

type DB struct {
}

type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

type Options struct {
	Logger          Logger
	Truncate        bool
	BypassLockGuard bool
	Dir             string
}

func (o Options) WithValueLogFileSize(_ int64) Options {
	return o
}

func (o Options) WithTableLoadingMode(_ any) Options {
	return o
}

func (o Options) WithValueLogLoadingMode(_ any) Options {
	return o
}

func (o Options) WithNumMemtables(_ any) Options {
	return o
}

func (o Options) WithKeepL0InMemory(_ any) Options {
	return o
}

func (o Options) WithMaxTableSize(_ any) Options {
	return o
}

func (o Options) WithCompactL0OnClose(_ any) Options {
	return o
}

func (o Options) WithNumLevelZeroTables(_ any) Options {
	return o
}

func (o Options) WithNumLevelZeroTablesStall(_ any) Options {
	return o
}

func (o Options) WithLoadBloomsOnOpen(_ any) Options {
	return o
}

func (o Options) WithIndexCacheSize(_ any) Options {
	return o
}

func (o Options) WithBlockCacheSize(_ any) Options {
	return o
}

func (o Options) WithLogger(_ any) Options {
	return o
}

type Iterator struct {
	prefix []byte
	index  int
	length int
	items  []*Item
}
type IteratorOptions struct {
	Prefix []byte
}

var DefaultIteratorOptions = IteratorOptions{}

type Txn struct {
}

func (txn *Txn) Set(key, val []byte) error {
	return nil
}
func (db *DB) NewTransaction(update bool) *Txn {
	return &Txn{}
}

func DefaultOptions(path string) Options {
	return Options{}
}

func Open(opt Options) (db *DB, err error) {
	return &DB{}, nil
}

func (txn *Txn) Discard() {
}
func (txn *Txn) NewIterator(opt IteratorOptions) *Iterator {
	return &Iterator{prefix: opt.Prefix, index: 0, length: 0}
}
func (txn *Txn) Commit() error {
	return nil
}
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	return &Item{key: []byte{}, value: []byte{}}, nil
}

type WriteBatch struct {
}

func (wb *WriteBatch) Flush() error {
	return nil
}

func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

func (db *DB) View(fn func(txn *Txn) error) error {
	return fn(&Txn{})
}

func (db *DB) Update(fn func(txn *Txn) error) error {
	return fn(&Txn{})
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Sync() error {
	return nil
}

func (db *DB) RLock() {
}
func (db *DB) RUnlock() {
}

type Item struct {
	key   []byte
	value []byte
}

func (it *Iterator) Item() *Item {
	return it.items[it.index]
}
func (it *Iterator) Valid() bool {
	return it.index < it.length
}
func (it *Iterator) Next() {
	it.index++
}
func (it *Iterator) Seek(key []byte) {
	it.index = 0
}

func (it *Iterator) Close() {
}
func (it *Iterator) Rewind() {
	it.index = 0
	it.items = Entries(it.prefix)
	it.length = len(it.items)
}

var (
	ErrKeyNotFound = errors.New("Key not found")
	ErrNoRewrite   = errors.New("Value log GC attempt didn't result in any cleanup")
)

func (item *Item) Key() []byte {
	return item.key
}

func (item *Item) Value(fn func(val []byte) error) error {
	return fn(item.value)
}

func (item *Item) ValueCopy(dst []byte) ([]byte, error) {
	return item.value, nil
}

func (item *Item) ValueSize() int64 {
	return int64(len(item.value))
}

func (db *DB) RunValueLogGC(discardRatio float64) error {
	return nil
}
