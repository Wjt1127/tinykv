package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState // 用于存储当前 Raft hard state 和 Last Log Index。
	// current apply state of the peer
	applyState *rspb.RaftApplyState // 用于存储 Raft applied 的 Last Log Index 和一些 truncated Log 信息

	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// generate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 || (len(oldEndKey) == 0 && len(newEndKey) != 0) {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
// 负责将 Ready 中的 entries 持久化到 raftDB 中去，然后更新 RaftLoaclState 的状态。同时，如果底层存储有冲突条目，则将其删除。
// 将给定的条目附加到 raft 日志并更新 ps.raftState，同时删除永远不会提交的日志条目
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	// Your Code Here (2B).
	if len(entries) == 0 {
		return nil
	}
	// 将所有的 Entry 都添加到 WriteBatch 中
	for _, ent := range entries {
		if err := raftWB.SetMeta(meta.RaftLogKey(ps.region.Id, ent.Index), &ent); err != nil {
			log.Panic(err)
		}
	}

	// 由于已经持久化的日志可能会因为冲突而被 leader 覆盖掉，对于这部分数据也需要在存储引擎中删除
	// ......stabled -> ......truncated......currLastIndex......stabled
	// 对于 [truncated, stabled] 中的日志本应该全都删除，但是 [truncated, currLastIndex] 中的数据只需要修改就可以了
	currLastTerm, currLastIndex := entries[len(entries)-1].Term, entries[len(entries)-1].Index
	prevLastIndex, _ := ps.LastIndex() // prevLastIndex 对应 RaftLog 中的 stabled
	for index := currLastIndex + 1; index <= prevLastIndex; index++ {
		raftWB.DeleteMeta(meta.RaftLogKey(ps.region.Id, index))
	}

	ps.raftState.LastTerm, ps.raftState.LastIndex = currLastTerm, currLastIndex // 更新 RaftLocalState
	return nil
}

// Apply the peer with given snapshot
// 应用一个快照之后 RaftLog 里面只包含了快照中的日志，并且快照中的数据都是已经被应用了的
// 接收节点在 HandleRaftReady()→SaveReadyState()→ApplySnapshot() 中
// 根据 pendingSnapshot 的 Metadata 更新自己的 RaftTruncatedState 和 RaftLocalState，
// 然后发送一个 RegionTaskApply 请求到 region_task.go 中。
// 此时它会异步的把刚刚收到保存的 Snapshot 应用到 kvDB 中。
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// 提示：这里需要做的事情包括：更新 peer 的存储状态，如raftState和applyState等，
	// 并通过 ps.regionSched 将 RegionTaskApply 任务发送给 region worker，还请记住调用ps.clearMeta
	// 和ps.clearExtraData删除过时数据
	// Your Code Here (2C).

	/* ApplySnapshot 描述：
	 *	1. 负责应用 Snapshot，先通过 ps.clearMeta 和 ps.clearExtraData() 清除原来的数据，
	 *     因为新的 snapshot 会包含新的 meta 信息，需要先清除老的。
	 * 	2. 根据 Snapshot 的 Metadata 信息更新当前的 raftState 和 applyState。并保存信息到
	 *     WriteBatch 中，可以等到 SaveReadyState() 方法结束时统一写入 DB。
	 *	3. 发送 RegionTaskApply 到 regionSched 安装 snapshot，因为 Snapshot 很大，
	 *     所以这里通过异步的方式安装。这里的实现是等待 Snapshot 安装完成后才继续执行，相当于没异步，以防出错。
	 */

	// 1. 删除过时数据：清空install snapshot之前的数据
	if ps.isInitialized() {
		ps.clearMeta(kvWB, raftWB)
		ps.clearExtraData(snapData.Region)
	}

	// 2. 更新 peer_storage 的内存状态，包括：
	// (1). RaftLocalState: 已经「持久化」到 DB 的最后一条日志设置为快照的最后一条日志
	// (2). RaftApplyState: 「applied」和「truncated」日志设置为快照的最后一条日志
	ps.raftState.LastIndex, ps.raftState.LastTerm = snapshot.Metadata.Index, snapshot.Metadata.Term
	ps.applyState.AppliedIndex = snapshot.Metadata.Index
	ps.applyState.TruncatedState.Index, ps.applyState.TruncatedState.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	ps.snapState.StateType = snap.SnapState_Applying

	// 将 applyState(状态机元数据信息)的修改加入 kvWB 中
	if err := kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState); err != nil {
		log.Panic(err)
	}

	// 3. 发送 runner.RegionTaskApply 任务给 region worker，并等待处理完毕
	ch := make(chan bool, 1)
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: ps.region.Id,
		Notifier: ch,
		SnapMeta: snapshot.Metadata,
		StartKey: snapData.Region.GetStartKey(),
		EndKey:   snapData.Region.GetEndKey(),
	}

	// 阻塞等待 region worker 完成 snapshot 的 apply
	if res := <-ch; res {
		log.Infof("%v end to apply snapshot, metaDataIndex %v, truncatedStateIndex %v", ps.Tag, snapshot.Metadata.Index, ps.applyState.TruncatedState.Index)
		result := &ApplySnapResult{PrevRegion: ps.region, Region: snapData.Region}

		// snapshot 处理完了：修改 regionState 元数据（startkey、endkey之类的信息会修改）
		meta.WriteRegionState(kvWB, snapData.Region, rspb.PeerState_Normal)

		return result, nil
	}

	return nil, nil
}

/*
 *	Save memory states to disk.
 *
 * 将内存状态保存到磁盘。持久化
 * 将 raft.Ready 中的数据保存到 badger 中，包括追加日志和保存 Raft 硬状态。
 *
 *	Do not modify ready in this function, this is a requirement to advance the ready object properly later.
 *
 * 不要在此函数中修改ready，这是稍后正确推进ready对象的要求。
 * 处理 Ready 中的 Entries 和 HardState 数据
 *
 * raftdb 存储 raft 日志和 RaftLocalState。
 * kvdb 在不同的列族中存储键值数据，RegionLocalState 和 RaftApplyState。你可以把 kvdb 看作是Raft论文中提到的状态机。
 */
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).
	raftWB := &engine_util.WriteBatch{} // 往磁盘的写操作都用 WriteBatch ，总共分为两个：raftWb 和 KvWb
	var result *ApplySnapResult
	var err error

	// 1. 通过 raft.isEmptySnap() 方法判断是否存在 Snapshot，如果有，则调用ApplySnapshot() 方法应用；
	if !raft.IsEmptySnap(&ready.Snapshot) {
		kvWB := &engine_util.WriteBatch{}
		// ApplySnapshot : Apply the peer with given snapshot
		result, err = ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			log.Panic(err)
		}
		kvWB.MustWriteToDB(ps.Engines.Kv)
	}

	// 2. 调用 Append() 将需要持久化的 entries 保存到 raftDB；
	if err = ps.Append(ready.Entries, raftWB); err != nil {
		log.Panic(err)
	}

	// 3. 保存 ready 中的 HardState 到 ps.raftState.HardState，注意先使用raft.isEmptyHardState() 进行判空；
	if !raft.IsEmptyHardState(ready.HardState) {
		*ps.raftState.HardState = ready.HardState
	}

	// 4. 持久化 RaftLocalState 到 raftDB；
	if err = raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState); err != nil {
		log.Panic(err)
	}

	// 5. 通过 raftWB.WriteToDB 和 kvWB.WriteToDB 进行原子的写入到存储引擎
	raftWB.MustWriteToDB(ps.Engines.Raft)
	return result, nil
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
