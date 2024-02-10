package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"

	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

/*
 * raftstore 从 Raft 模块获得并处理 ready，包括发送 raft 消息、持久化状态、将提交的日志项应用到状态机。
 * 一旦应用，通过回调函数将响应返回给客户。
 */

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

/* HandleRaftReady 处理 rawNode 传递来的 Ready
 * HandleRaftReady 对这些 entries 进行 apply（即执行底层读写命令）
 * 每执行完一次 apply，都需要对 proposals 中的相应 Index 的 proposal 进行 callback 回应（调用 cb.Done()）
 * 然后从中删除这个 proposal。
 */
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	// 1. 没有 Ready 提交给上层应用，什么都不处理，直接返回
	if !d.RaftGroup.HasReady() {
		return
	}

	ready := d.RaftGroup.Ready()
	// 2. 调用 SaveReadyState 将 Ready 中需要持久化的内容保存到 badger。
	// 如果 Ready 中存在 snapshot，则应用这个 snapshot；
	// 保存 unstable entries, hard state, snapshot
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Panic(err)
	}
	// 如果有 snapshot 存在，应用它
	if applySnapResult != nil {
		if !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
			d.peerStorage.SetRegion(applySnapResult.Region)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
			storeMeta.regionRanges.Delete(&regionItem{applySnapResult.PrevRegion})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{applySnapResult.Region})
			storeMeta.Unlock()
		}
	}

	// 3. Ready中还可能包含的是 Msg，需要发送给同 Region 中的其他 peer
	d.Send(d.ctx.trans, ready.Messages)

	// 4. apply Ready 中的 commitEntries
	if len(ready.CommittedEntries) > 0 {
		// 需要使用 kvWB 去应用这些 kv 键值对的读/写操作
		kvWB := &engine_util.WriteBatch{}
		for _, ent := range ready.CommittedEntries {
			// 对需要 apply 的 Entry 进行处理，并在完成后调用 callback 函数通知 client
			kvWB = d.processCommittedEntry(&ent, kvWB) // 将需要处理的 Kv write缓存在一个 writeBatch 结构体中
			// 节点有可能在 processCommittedEntry 返回之后就销毁了
			// 如果销毁了需要直接返回，保证对这个节点而言不会再 DB 中写入数据
			if d.stopped {
				return
			}
		}
		// 更新 RaftApplyState
		lastEntry := ready.CommittedEntries[len(ready.CommittedEntries)-1]
		d.peerStorage.applyState.AppliedIndex = lastEntry.Index
		if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Panic(err)
		}
		// 在这里一次性执行所有的 Command 操作和 ApplyState 更新操作，将写操作批处理
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	}

	//5. 调用 d.RaftGroup.Advance() 推进 RawNode,更新 raft 状态
	d.RaftGroup.Advance(ready)
}

// 在 raftStore 层处理由 RawNode 层提交上来的 Ready 中的committedEntry，在应用中 apply 这些 Entry 的请求
func (d *peerMsgHandler) processCommittedEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	// 首先检查 EntryType 是 EntryConfChange 还是 EntryNormal
	if entry.EntryType == pb.EntryType_EntryConfChange {
		confChange := &pb.ConfChange{}
		// unmarshal 就是 server-stub 中的反序列化函数
		if err := confChange.Unmarshal(entry.Data); err != nil {
			log.Panic(err)
		}
		log.Infof("EntryType_EntryConfChange")
		return d.processConfChange(entry, confChange, kvWB)
	}

	// 下面处理的就是需要 apply 的日志条目，这些条目需要转化为request然后在应用层执行
	requests := &raft_cmdpb.RaftCmdRequest{}
	if err := requests.Unmarshal(entry.Data); err != nil { // 解析 entry.Data 中的数据
		log.Panic(err)
	}

	// 判断是普通 request 还是 AdminRequest
	if requests.AdminRequest != nil {
		return d.processAdminRequest(entry, requests, kvWB)
	} else {
		return d.processNormalRequest(entry, requests, kvWB)
	}
}

// HandleRaftReady 中处理 Ready 中需要 Apply 的 committedEntry 中的 Normal request（invalid/get/put/delete/snap操作）
func (d *peerMsgHandler) processNormalRequest(entry *pb.Entry, requests *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	// NormalRequestResponse 只需要对这两个变量赋值
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: make([]*raft_cmdpb.Response, 0),
	}

	// 处理一次请求中包含的所有操作，对于 Get/Put/Delete 操作首先检查 Key 是否在 Region 中
	for _, req := range requests.Requests {
		switch req.CmdType {
		// Get 操作
		case raft_cmdpb.CmdType_Get:
			key := req.Get.Key
			// 检查这个 request 对应的 key 是不是在本 peer 所属的 region 中
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				BindRespError(resp, err)
			} else { // key 在本 region 中
				// Get 和 Snap 请求需要先将之前的结果写到 DB
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv) // 因为 requests 中可能有读请求在写请求后
				kvWB = &engine_util.WriteBatch{}             // kvWB 是缓存 writeBtach 的结构体
				value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				// 返回给 client 的请求 response
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: value},
				})
			}
		case raft_cmdpb.CmdType_Put:
			key := req.Put.Key
			// 同样需要判断是否在本 region 中
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				BindRespError(resp, err)
			} else {
				kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
			}
		case raft_cmdpb.CmdType_Delete:
			key := req.Delete.Key
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				BindRespError(resp, err)
			} else {
				kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			}
		case raft_cmdpb.CmdType_Snap:
			if requests.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				BindRespError(resp, &util.ErrEpochNotMatch{})
			} else {
				// Get 和 Snap 请求需要先将结果写到 DB，否则的话如果有多个 entry 同时被 apply，客户端无法及时看到写入的结果
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
				kvWB = &engine_util.WriteBatch{}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
				})
			}
		}
	}

	// 处理完 RawNode 给上层的 Ready 中需要 apply 的 NormalRequest 后，需要调用 proposal 中对应索引的 callback 函数通知client 这个请求被处理了
	// 这个 callback 对应的索引就是 term 和 index，上层应用对 raft-server 进行 proposal 的时候，如果在 leader 节点收到，会分配对应的 term 和index的
	d.callbackAfterProcessCmds(entry, resp)
	return kvWB
}

// callback函数记录在 peer 结构体的 proposals[] 中，使用 term + Index 作为唯一标识判断是否是这个 entry 对应的 callback
func (d *peerMsgHandler) callbackAfterProcessCmds(entry *pb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	// 1. 找到 entry 对应的回调函数（proposal），存入操作的执行结果（resp）
	// 2. 有可能会找到过期的回调（term 比较小或者 index 比较小），此时应该使用 Stable Command 响应并从回调数组中删除 proposal
	// 3. 其他情况：正确匹配的 proposal（处理完毕之后应该立即结束），further proposal（直接返回）

	for len(d.proposals) > 0 {
		// 可能是由于领导者变更，导致某些日志未提交并被新领导者的日志覆盖。
		// 但客户并不知道这一点，仍在等待回复。因此，应该返回 ErrStaleCommand 信息以让客户端知道并再次重试该命令。
		proposal := d.proposals[0]
		if proposal.term < entry.Term || proposal.index < entry.Index { // stalecommand
			// 日志冲突，然后冲突后的entry被截断的情况
			NotifyStaleReq(proposal.term, proposal.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		// 正确匹配到对应的proposal
		if proposal.term == entry.Term && proposal.index == entry.Index {
			if proposal.cb != nil {
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false) // snap resp should set txn explicitly
			}
			proposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
		// further proposal（即当前的 entry 并没有 proposal 在等待，或许是因为现在是 follower 在处理 committed entry）
		return
	}
}

// HandleRaftReady 中处理 Ready 中需要 Apply 的 committedEntry 中的 Admin request（compactLog/Region Split/操作）
func (d *peerMsgHandler) processAdminRequest(entry *pb.Entry, requests *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	adminReq := requests.AdminRequest
	switch adminReq.CmdType {
	// CompactLogRequest 是修改元数据，即更新RaftApplyState 中的 RaftTruncatedState。
	// 之后，通过ScheduleCompactLog 给 raftlog-gc worker 安排一个任务。
	// Raftlog-gc worker 将以异步方式进行实际的日志删除工作。
	case raft_cmdpb.AdminCmdType_CompactLog:
		// CompactLog 类型请求不需要将执行结果存储到 proposal 回调,
		// 因为这个 CompactLog 相当于是raft 模块内部到达阈值被动触发的，而不是client提出的
		if adminReq.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
			// 记录最后一条被截断的日志（快照中的最后一条日志）的索引和任期
			truncatedState := d.peerStorage.applyState.TruncatedState
			truncatedState.Index, truncatedState.Term = adminReq.CompactLog.CompactIndex, adminReq.CompactLog.CompactTerm
			// 调度日志截断任务到 raftlog-gc worker
			d.ScheduleCompactLog(adminReq.CompactLog.CompactIndex)
			log.Infof("%d apply commit, entry %v, type %s, truncatedIndex %v", d.peer.PeerId(), entry.Index, adminReq.CmdType, adminReq.CompactLog.CompactIndex)
		}
	}
	return kvWB
}

// HandleRaftReady 中处理 Ready 中需要持久化的 Entry 条目，这个 Entry 条目是 confChangeType 类型的
func (d *peerMsgHandler) processConfChange(entry *pb.Entry, confChange *pb.ConfChange, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {

	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage) // 类型断言：将msg.Data的底层值以*rspb.RaftMessage的形势取出
		// 在 d.onRaftMsg 中处理了这个来自同 raftGroup 中其他节点 peer 的 Msg（也许是heartBeat、RequestVote、AppendEntry消息等）
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd: // 上层应用的 kv 指令：可能是NormalCmd（put、get、snapshot、delete）或 AdminCmd
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	// 检查这个 peer 是不是 Leader，只有 Leader 才处理 client 的请求
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// 将 client 的请求(Requests是一个数组，可能是一组操作)包装成 entry 传递给 raft 层
// 所以一个 entry 可能是一批操作形成的日志
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// 检查各种信息查看是否 term 过时了，或者发错节点了，或者发送的这个peer不是leader
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeNormalRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {

}

// 将普通 request 封装成 entry 传递给 RawNode 进行处理
// 回调函数的term 和 index 是和该 leader 节点中对应的 logEntry 中一样
func (d *peerMsgHandler) proposeNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	//1. 封装回调，等待log被apply的时候调用
	//后续相应的 entry 执行完毕后，响应该 proposal，即 callback.Done( )；
	d.proposals = append(d.proposals, &proposal{
		index: d.RaftGroup.Raft.RaftLog.LastIndex() + 1,
		term:  d.RaftGroup.Raft.Term,
		cb:    cb,
	})
	//2. 序列化RaftCmdRequest
	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}
	//3. 将该字节流包装成 entry 传递给下层raft MessageType_MsgPropose
	err = d.RaftGroup.Propose(data)
	if err != nil {
		log.Panic(err)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

// 实际上是从上一次的 LastCompactedIndex 到本次的 truncatedIndex 之间的 log 做compact
func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

// 向 Leader 发出 logCompact（属于AdminRequest） 请求，用 proposeRaftCommand 提出
// logCompact 和 do_snapshot 是两个东西，logCompact 是 Leader 执行无用 Log 的删除操作，并修改相关元数据信息，同时将操作propose到其他节点
// 而 do_snapshot 是上层应用去生成的，raft 模块并不能生成快照，因为 raft 模块不知道数据的结构和格式，需要由上层应用生成
func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	// 获取firstIndex 和 appliedIndex
	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
