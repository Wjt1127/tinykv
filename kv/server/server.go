package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
// 获取单个 key 的 Value。
// 1. 获取 Lock，如果 Lock 的 startTs 小于当前的 startTs，说明存在之前存在尚未 commit 的请求，中断操作，返回 LockInfo；
// 2. 否则直接获取 Value，如果 Value 不存在，则设置 NotFound = true；
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}

	// 获取 reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return resp, err
	}

	// 创建 txn，获取 Lock
	txn := mvcc.NewMvccTxn(reader, req.Version) // 创建读事务txn，其中的 TS 使用 req.Version
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return resp, err
	}

	// Percolator 为了保证快照隔离的时候总是能读到已经 commit 的数据
	// 当发现准备读取的数据被锁定的时候，会等待解锁
	if lock != nil && req.Version >= lock.Ts {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
	}

	// 获取 <key, startTS> 对应的 value
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value

	return resp, nil
}

// Percolator 第一阶段 preWrite
// 1. 通过 MostRecentWrite 检查所有 key 的最新 Write，如果存在，且其 commitTs 大于当前事务的 startTs，说明存在 write conflict，终止操作；
// 2. 通过 GetLock() 检查所有 key 是否有 Lock，如果存在 Lock，说明当前 key 被其他事务使用中，终止操作；
// 3. 到这一步说明可以正常执行 Prewrite 操作了，写入 Default 数据和 Lock；
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}

	// 获取 reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return resp, err
	}
	defer reader.Close()
	// 创建 txn
	txn := mvcc.NewMvccTxn(reader, req.StartVersion) // 创建读事务txn，其中的 TS 使用 req.StartVersion

	// 1. 检查在 startTS 到现在这段时间内，key 中最新的 Write 是否有 commitTS 大于当前事务的 startTS
	//    利用 4A 中的 MostRecentWrite 获取 key 的最新 Write
	var keyErrors []*kvrpcpb.KeyError
	for _, operation := range req.Mutations {
		write, commitTS, err := txn.MostRecentWrite(operation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return resp, err
		}
		// 检测在 StartTS 之后是否有已经提交的 Write，如果有的话说明写冲突，需要 abort 当前的事务
		if write != nil && commitTS >= req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTS,
					Key:        operation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		// 检测 Key 是否有 Lock 锁住，如果有的话则说明别的事务可能正在修改并且未提交
		lock, err := txn.GetLock(operation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return resp, err
		}
		if lock != nil {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.PrimaryLock,
					LockVersion: lock.Ts,
					Key:         operation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}

		// 暂存修改到 txn 中，然后对需要修改的 Key 进行加锁
		var kind mvcc.WriteKind
		switch operation.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(operation.Key, operation.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
			txn.DeleteValue(operation.Key)
		case kvrpcpb.Op_Rollback: // 直接返回了
			return nil, nil
		}

		txn.PutLock(operation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}

	// 说明有冲突
	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
		return resp, nil
	}

	// 写入事务中暂存的修改到 storage 中
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

// 1. 通过 Latches 上锁对应的 key；
// 2. 尝试获取每一个 key 的 Lock，并检查 Lock.StartTs 和当前事务的 startTs 是否一致，不一致直接取消。因为存在这种情况，客户端 Prewrite 阶段耗时过长，Lock 的 TTL 已经超时，被其他事务回滚，所以当客户端要 commit 的时候，需要先检查一遍 Lock；
// 3. 如果成功则写入 Write 并移除 Lock；
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}

	// 获取 reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return resp, err
	}
	defer reader.Close()
	// 创建 txn
	txn := mvcc.NewMvccTxn(reader, req.StartVersion) // 创建读事务txn，其中的 TS 使用 req.StartVersion

	// 1. 通过 latch 对对应的 key 上锁, 为什么需要额外上 latch 呢，每次操作检查 lock 不就行了？
	// （去掉 latch 都能过 4B 和 4C，后面写一个这个冲突的测试，提交一个 pr）
	//    因为在 batchRollBack 中是对每一个 key 进行删除 lock，删除这一版本的defualt，
	//    然后写 rollbackKind 的 write，因为这是一组操作，所以需要原子性的完成。
	//    不能在删完一个 lock 后，其他同一批的 lock 还没解除的时候，就让其他事务来上锁开始prewrite，
	//    因为如果 batchRollBack 失败了，那么其中某个 lock 甚至被其他的事务上锁了，那么 rollback 会出错。
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		// 2. 检测是否已经通过 checkStatus 完成了 roll back ，是否是重复提交
		write, _, err := txn.CurrentWrite(key) // 获取最新的 write
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// Rollback 类型的 Write 表示是已经正确提交的，这个时候按照重复提交处理
		if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == req.StartVersion {
			return resp, nil
		}
		// 3. 检查每个 Key 的 Lock 是否还存在
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// 如果 Lock 不存在，有两种情况：
		// 第一种 nil 是事务已经正确提交了，这次是一个重复提交；
		// 第二种 lock.Ts != req.StartVersion 是这个事务前面的 prewrite 操作因为过于缓慢，超时，导致你的 lock 被其他事务 rollback 了
		if lock == nil || lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return resp, nil
		}
		// 3. 第一次提交事务，正常处理
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// 返回 key 的最新版本
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer reader.Close()
	defer scanner.Close()
	var pairs []*kvrpcpb.KvPair // 存储扫描到的 {key, value}
	// 查找 key
	for i := 0; i < int(req.Limit); i++ {
		key, value, err := scanner.Next()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// key 为空表示没有数据了
		if key == nil {
			break
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// 如果上锁了，说明有事务正在更新
		if lock != nil && req.Version >= lock.Ts {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					},
				},
				Key: key,
			})
			continue
		}
		if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		}
	}
	resp.Pairs = pairs
	return resp, nil
}

// Percolator 采取 lazy Gc 的方式，让其他事务来删除异常事务的锁
// KvCheckTxnStatus 检查超时时间，删除过期的锁并返回锁的状态
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_NoAction}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// 1. 检查 PrimaryKey 是否存在 Write，Write 的开始时间戳与 req.LockTs 相等
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	// 如果 primaryKey 已经完成了 write(即完成了 commit point 的同步)
	if write != nil {
		// WriteKind 如果不是 WriteKindRollback 则说明已经被 commit
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = ts
		}
		return resp, nil
	}

	// 如果 primaryKey 不在，说明没有完成 commit，需要通过 Lock 的 ttl 来判断, 此时分为三种情况：
	//   lock 不存在：说明已经完成了 rollback 但是没有加上 rollbackKind Write
	//   没有超过 ttl ： 暂且认为这个事务没有出现异常，仍然在执行
	//	 超过了 ttl ： 认为这个事务出现异常了，可能是这个事务做 prewrite 的时候做了一部分宕机了，然后现在又恢复起来了
	// 2. 检查 lock 是否存在
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	// 2.1 primaryKey 的 write 不在 lock 也不在，说明已经完成了 rollback 操作了，
	//     但是没有 rollBackKind 的write记录，需要添加一个
	if lock == nil {
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}

	// 2.2 primaryKey 的 write 不在 lock 还在，根据 lock 的ttl判断
	// 检查 lock 是否超时，如果超时则移除 Lock 和 Value，创建一个 WriteKindRollback
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, nil
	}
	// 否则认为其他事务正在提交
	return resp, nil
}

// KvBatchRollback 批量回滚 key
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// 因为可能 commit 和 rollback 出现冲突
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		// 1. 获取 key 的 Write，如果已经是 WriteKindRollback 则跳过这个 key
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
		}
		// 获取 Lock，如果 Lock 被清除或者 Lock 不是当前事务的 Lock，则中止操作
		// 这个时候说明 key 被其他事务占用
		// 否则的话移除 Lock、删除 Value，写入 WriteKindRollback 的 Write
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	iter := reader.IterCF(engine_util.CfLock)
	defer reader.Close()
	defer iter.Close()

	// 1. 通过 iter 获取到含有 Lock 的所有 key；
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, nil
		}
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}

	// 根据 request.CommitVersion 提交或者 Rollback
	if req.CommitVersion == 0 {
		resp1, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error, resp.RegionError = resp1.Error, resp1.RegionError
		return resp, err
	} else {
		resp1, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error, resp.RegionError = resp1.Error, resp1.RegionError
		return resp, err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
