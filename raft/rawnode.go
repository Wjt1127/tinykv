// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType // 0:follower  1: candidate  2:leader
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
// Ready 结构体用于保存已经处于 ready 状态的日志和消息
// 这些都是准备保存到持久化存储、提交或者发送给其他节点的
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 现在的 leader 和自身是什么身份，是易变的，所以是软状态
	// 不需要持久化
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 要 persist 的节点状态。
	// 如果没有更新，则HardState将等于空状态。
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// 需要在发送消息给 application 之前 persist 的日志
	// 待持久化的日志条目
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// 快照, 需要persist
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// 需要被输入到状态机中的日志，这些日志之前已经被保存到 Storage 中了
	// 待 apply
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

/*
由于领导的更迭必然会导致任期的更迭，可以认为在稳定的集群中，只有hardstate变化时
才会导致softstate变化，故针对hardstate的变化更能反应RawNode的更新需要不需要被上层状态机获取。
*/

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft // raft.RaftLog.HardState 是当前的 hardstate
	// Your Data Here (2A).
	preSoftState *SoftState   // 上一阶段的 softState
	preHardState pb.HardState // 上一阶段的 hardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	rawNode := &RawNode{
		Raft:         raft,
		preSoftState: &SoftState{Lead: raft.Lead, RaftState: raft.State},
		preHardState: pb.HardState{Term: raft.Term, Vote: raft.Vote, Commit: raft.RaftLog.committed},
	}

	return rawNode, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
// 竞选：让该节点转成 candidate
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
// 上层的propose：发起 proposeMsg
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
// 根据 proposeConfigChangeMsg 修改集群节点配置，根据 ConfChangeType 是增加或减少节点，然后修改 ConfState 信息
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
// RawNode层的 Step 函数处理网络请求，对于来自网络的 LocalMsgtype 应该忽略
// 内部调用 raft 层的 Step 函数处理相应的消息请求
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
// Ready entry 存的其实就是当前 RawNode 的一些信息状态
// 看了测试之后的理解：应该是需要先通过 HasReady 去判断是否有需要上层处理的（持久化、apply、处理消息等）
// 然后调用 Ready 向上层传递当前的 Ready 状态
// 然后再调用 Advance 通知 RawNode 上次的这个 Ready 已经处理完了，RawNode更新相应状态信息
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	readyEntry := Ready{
		Messages:         rn.Raft.msgs,
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
	}

	if rn.isSoftStateUpdate() {
		// 虽然 SoftState 不需要持久化，但是测试用例里面用到了
		readyEntry.SoftState = &SoftState{Lead: rn.Raft.Lead, RaftState: rn.Raft.State}
	}

	if rn.isHardStateUpdate() {
		readyEntry.HardState = pb.HardState{Term: rn.Raft.Term, Vote: rn.Raft.Vote, Commit: rn.Raft.RaftLog.committed}
	}

	// 有 snapshot 需要 apply
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		readyEntry.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}

	return readyEntry
}

// HasReady called when RawNode user need to check if any Ready pending.
// 判断 raft 模块是否可以生成 Ready Entry，让上层处理信息
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	// 1. 是否有需要持久化的硬状态（HardState被修改的Msg处理）
	// 2. 是否有需要持久化的日志（AppendEntry带来的新Entries）
	// 3. 是否有需要apply的日志（AppendEntry后Leader计算了commitIndex,commit了但是没apply的日志）
	// 4. 是否有需要发送的Msg
	// 5. 是否有需要 apply 的 snapshot

	return len(rn.Raft.msgs) > 0 || // 是否有需要处理的Msg
		rn.isHardStateUpdate() || // 是否有硬状态的变化
		len(rn.Raft.RaftLog.unstableEntries()) > 0 || // 是否有需要持久化的日志
		len(rn.Raft.RaftLog.nextEnts()) > 0 || // 是否有commited 但是没 apply 的日志
		!IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) //是否有需要 apply 的 snapshot
}

func (rn *RawNode) isSoftStateUpdate() bool {
	return rn.preSoftState.Lead != rn.Raft.Lead || rn.preSoftState.RaftState != rn.Raft.State
}

func (rn *RawNode) isHardStateUpdate() bool {
	return rn.preHardState.Term != rn.Raft.Term || // 任期改变
		rn.preHardState.Commit != rn.Raft.RaftLog.committed || // commitIndex改变
		rn.preHardState.Vote != rn.Raft.Vote // 投票变化

}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// 上层应用(状态机)调用 Advance 通知 RawNode 应用程序已 apply 并保存了上次 Ready 结果中的进度信息
// 而Advance则是由上层告诉RawNode，这一Ready状态已被同步，并将其写入RawNode的软、硬状态中，
// 使之与其含有的Raft节点的软、硬状态同步。
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).

	// 如果 rd 的softState不为 nil 说明上次的 Ready 更改了softState
	if rd.SoftState != nil {
		rn.preSoftState = rd.SoftState
	}

	if !IsEmptyHardState(rd.HardState) {
		rn.preHardState = rd.HardState
	}

	// 说明已经持久化了这些日志
	if len(rd.Entries) > 0 {
		rn.Raft.RaftLog.stabled += uint64(len(rd.Entries))
	}

	// 说明已经提交了这些日志
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.RaftLog.applied += uint64(len(rd.CommittedEntries))
	}
	rn.Raft.RaftLog.maybeCompact()        // 丢弃被压缩的暂存日志；
	rn.Raft.RaftLog.pendingSnapshot = nil // 清空 pendingSnapshot, 表示已经将这个 snapshot apply 了
	rn.Raft.msgs = nil                    // 清空被处理过的条目
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
