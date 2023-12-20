package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

//1. 使用badger.Txn来实现Reader()函数，即badger提供的事务支持。
//2. badger不支持Column Family，engine_util提供了一系列函数，利用前缀来实现Column Family，使用它们来实现Write()函数。
//3. 使用Connor1996/badger而不是dgraph-io/badger。
//4. 使用Discard()关闭badger.Txn，这之前需要关闭所有迭代器。

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	StorageEngine *engine_util.Engines
	conf          *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	StandKvDB := engine_util.CreateDB(conf.DBPath, false) // use false to declare a KV engine
	StandKvEngine := engine_util.NewEngines(StandKvDB, nil, conf.DBPath, "")

	return &StandAloneStorage{
		StorageEngine: StandKvEngine,
		conf:          conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	if s.StorageEngine == nil || s.conf == nil {
		return errors.New("StorageEngine or conf is nil")
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	// 这是stop，所以只需要关闭 DB 即可，不是destroy
	return s.StorageEngine.Close()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 1. 根据 Modify 的类型(Put/Delete)调用 PutCF/DeleteCF 可以向 DB 中 modify 一个条目
	// 这里的 batch 并不是一次写入所有的请求，而是对上层的请求做缓存合并成一个 batch ，减少从上层到磁盘的请求 IO 路径次数

	var err error
	for _, entry := range batch {
		key, value, cf := entry.Key(), entry.Value(), entry.Cf()
		if _, ok := entry.Data.(storage.Put); ok {
			err = engine_util.PutCF(s.StorageEngine.Kv, cf, key, value)
		} else {
			err = engine_util.DeleteCF(s.StorageEngine.Kv, cf, key)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// 1. 调用 DB 类中的 NewTransaction 方法，为 Kv DB创建一个 txn
// 2. 返回一个使用Storage interface创建的类的实例对象
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	var (
		sreader *StandAloneStorageReader
		err     error
	)

	txn := s.StorageEngine.Kv.NewTransaction(false) // set false for only-read txn
	if txn == nil {
		err = errors.New("NewTransaction failed")
	} else {
		err = nil
	}

	sreader = NewStandAloneStorageReader(txn)
	return sreader, err
}

// type StorageReader interface 的实现
// interface 可以理解成c++里面的template，对于这个interface需要定义一个具体的类(struct)
// 然后为这个类实现 interface 里面定义的各种接口函数
type StandAloneStorageReader struct {
	ReadTxn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		ReadTxn: txn,
	}
}

// 调用 kv/util/engine_util/util.go:GetCFFromTxn ,其本质就是调用的 badger 中的txn.Get
func (sreader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var (
		val []byte
		err error
	)
	val, err = engine_util.GetCFFromTxn(sreader.ReadTxn, cf, key)
	return val, err
}

// 调用 kv/util/engine_util/cf_iterator.go:
// func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator
func (sreader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sreader.ReadTxn)
}

// 在 close Reader时需要 Discard 所有迭代器
func (sreader *StandAloneStorageReader) Close() {
	sreader.ReadTxn.Discard()
}
