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
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower  StateType = iota // 0
	StateCandidate                  // 1
	StateLeader                     // 2
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// 节点 ID
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// 一个 raft 集群内的所有节点 ID
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// nextIndex 对于每个节点，待发送到该节点的下一个日志条目的索引，初值为领导人最后的日志条目索引 + 1
	// matchIndex 对于每个节点，已知的已经同步到该节点的最高日志条目的索引，初值为0，表示没有
	Match, Next uint64
	// 通常 matchIndex + 1 = nextIndex，并且可以通过 Leader 节点上记录的matchIndex 算出可以 apply 的Index范围
}

type Raft struct {
	// 节点 ID
	id uint64
	// 节点当前 Term
	Term uint64
	// 当前任期内，票投给了谁
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// 日志复制需要记录 follower 的进度，及记录每个 follower 的日志同步到哪个index了
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	// 如果本节点参与竞选，存放哪些节点投票给了本节点
	votes map[uint64]bool

	// msgs need to send
	// 先将需要发送的msgs存放在里面
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64 // 记录配置文件修改的entry index

	// 增加一个随机 electionTimeOut
	randomElectionTimeOutTick int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, _ := c.Storage.InitialState()
	// peers should only be set when starting a new raft cluster
	if c.peers == nil {
		// confState 存了所有的节点ID
		c.peers = confState.Nodes
	}

	rf := &Raft{
		id:                        c.ID,
		Term:                      hardState.Term,
		Vote:                      hardState.Vote,
		RaftLog:                   newLog(c.Storage),
		Prs:                       map[uint64]*Progress{},
		State:                     StateFollower,     // 初始化的时候全部设置为 follower
		votes:                     map[uint64]bool{}, // 初始化无人开始投票
		msgs:                      []pb.Message{},    // 初始化时没有消息需要发送
		Lead:                      0,                 // 初始化时 Leader ID 设置为 0
		heartbeatTimeout:          c.HeartbeatTick,
		electionTimeout:           c.ElectionTick,
		heartbeatElapsed:          0,
		electionElapsed:           0,
		leadTransferee:            0,
		PendingConfIndex:          0,
		randomElectionTimeOutTick: 0,
	}

	// 为了避免多个candidate循环的陷入选举竞争状态，对每个选举者的选举超时时间做随机处理，基于electionTimeout
	rf.setRandomElectionTime()

	// 更新集群配置，后续不会清空
	rf.Prs = make(map[uint64]*Progress)
	for _, id := range c.peers {
		rf.Prs[id] = &Progress{}
	}

	// 后续为了选举投票会被清空后处理
	rf.votes = make(map[uint64]bool)

	return rf
}

// 为了避免多个candidate循环的陷入选举竞争状态，对每个选举者的选举超时时间做随机处理 : [electionTime, 2 * electionTime]
func (r *Raft) setRandomElectionTime() {
	randsource := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.randomElectionTimeOutTick = r.electionTimeout + randsource.Intn(r.electionTimeout)
}

/* ************************************* Tick Management *************************** */

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.leaderTick()
	case StateCandidate:
		r.candiateTick()
	case StateFollower:
		r.followerTick()
	}
}

func (r *Raft) leaderTick() {
	r.heartbeatElapsed++ // 对于 Leader 是 heartBeart 计数，到时间了发心跳包
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0

		// 我觉得也可以 append 到r.msg中
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, To: r.id, Term: r.Term}) // 提醒leader去发心跳包
	}
}

func (r *Raft) candiateTick() {
	r.electionElapsed++ // 成为 candidate 一样要计数，判断是否超时
	if r.electionElapsed >= r.randomElectionTimeOutTick {
		r.electionElapsed = 0

		// 我觉得也可以 append 到r.msg中
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id, Term: r.Term}) // 提醒该节点准备开启选举
	}
}

func (r *Raft) followerTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeOutTick {
		r.electionElapsed = 0

		// 我觉得也可以 append 到r.msg中
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id, Term: r.Term}) // 提醒该节点准备开启选举
	}
}

/* *************************** State Transfer ***************************** */

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		// 只有任期超过该节点任期，才能再次投票，一个任期投票一次
		r.Vote = None
	}

	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.electionElapsed = 0
	r.setRandomElectionTime()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.electionElapsed = 0
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.id] = true

	r.setRandomElectionTime()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	// 因为leadership 转变了，所以需要重置 nextIndex 和 matchIndex
	for i := range r.Prs {
		r.Prs[i].Next = r.RaftLog.LastIndex() + 1 // 初始化为 Leader 的最后一条LogIndex，如果冲突了不匹配后续递减修改
		r.Prs[i].Match = 0                        // 初始化为 0，避免错误的匹配设置
	}

	// 成为 Leader 之后立马发一条 noop 日志，相当于 HeartBeat 通告集群所有节点 Leadership 转变了
	// 同时通过一条 noop entry 触发其他节点的状态机执行过程，以保持整个集群的一致性。
	// 这里使用三个RPC : AppendEntry\Propose\HearBeat 去取代原文中的 AppendEntry
	// 这里的 HeartBeat 不追加日志，以此减少没有上层应用请求时的无用空数据写入（对比原文的 AppendEntry 中放空数据），但是
	//		对于原文的使用追加日志实现的HeartBeat可以频繁的同步多个节点的日志提交状态
	// 这里的 propose 代替了一次原文的 Append 空 entry，以此实现触发其他节点状态机推动
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, From: r.id, To: r.id, Term: r.Term, Entries: make([]*pb.Entry, 0)}) // 我觉得也可以 append 到r.msg中
}

/* *************************** Step : Msg Handle *************************** */

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 网络请求由上层 RawNode 调用 Step，Local请求（选举超时、心跳包超时）由本节点调用 Step，通知 Step 进行相应的处理
	var err error
	switch r.State {
	case StateFollower:
		err = r.followerStep(m)
	case StateCandidate:
		err = r.candidateStep(m)
	case StateLeader:
		err = r.leaderStep(m)
	}
	return err
}

func (r *Raft) leaderStep(m pb.Message) error {
	// leader 能够接受到的消息类型：
	// 1. MsgBeat：收到后，向其他节点广播心跳包，发送 MsgHeartbeat 消息
	// 2. MsgPropose：上层想要 propose 的条目（TinyKV还用作当选Leader后追加空条目），先向日志条目中追加 Log ，然后再 replicate
	// 3. MsgAppendEntry：日志复制，对需要复制的日志进行判断，如果没有冲突就加入自己的 raftlog 中；否则返回给 Leader
	//    需要更新的nextIndex和matchIndex信息；
	// Leader 为什么也会收到 MsgAppendEntry 呢？因为可能存在网络分区的情况，可能突然两个网络能够通信了，接收到了
	//    另一个Leader的消息
	// 4. MsgAppendResponse：来自 follower 们的回复，如果冲突会带上自己的LastIndex，让Leader更新自己的nextIndex;
	//    如果 follower 回复的 index 不在 entries 里面，则发送snapshot,MsgSnapshot
	// 5. MsgRequestVote：来自 candidate 的投票请求，判断是否大于等于自己的Term，如果等于还要判断lastIndex哪个大
	// 6. MsgHeartBeatResponse：节点对Leader心跳包的回应，同时告知 Leader 的 commit 是否落后，因为可能回复的follower
	// 	  是上一任期的leader，他的commitIndex可能更新
	// 7. MsgTransferLeader：上层要求转移 Leadership

	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		// 向所有节点广播心跳包
		r.bcastHeartBeat()
	case pb.MessageType_MsgPropose:
		// 追加 Log 然后发起 Log replicate（即发送MsgAppendEntry）
		r.handlePropose(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// 接收到了节点对心跳包的回应，可能 follower 会告知 Leader 可以更新 committedIndex
		// 因为可能这个接收者就是上一轮的 Leader 他算出来了更大的 committedIndex ，但是后续他不再是 Leader 了没有再进行appendEntry操作了
		// 所以会出现某一个节点的committedIndex 可能比当前 Leader 的commitIndex更大
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgAppend: // follower 们根据AppendEntry的信息更新自己的commitIndex
		// 收到了其他网络分区的消息
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: // Leader 根据 AppendEntryResponse 的信息来计算自己 term 内的日志是否有复制到大多数节点上，然后更新commitIndex
		// 收到了 follower 们对日志复制的反馈，根据反馈更新 nextIndex 和 matchIndex
		// 如果 nextIndex 小于 firstIndex，需要给 follower 发送snapshot
		// 最后根据接收情况，判断是否需要更新 committedIndex
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		// 如果收到 RequestVote ，通过判断candidate的 lastLogTerm 和 lastLogIndex 是否大于自己进行vote
		r.handleRequestVote(m)
	case pb.MessageType_MsgTransferLeader:
		// 转换 Leadership
		// project 3
	}

	return nil
}

func (r *Raft) candidateStep(m pb.Message) error {
	// candidate 能够接收到的消息类型：
	// 1. MsgHup: 表示选举超时，提示candidate开启新的一轮选举
	// 2. MsgRequestVoteResponse: 选举投票结果
	// 3. MsgHeartBeat: 收到了来自 Leader 的心跳包
	// 4. MsgRequestVote: 可能收到来自其他 candidate 的投票请求，拒绝就行
	// 5. MsgTimeOutNow: 让非 Leader 节点立即选举超时，开启一轮选举；上层做的转移 leadership 的 RPC
	// 6. MsgAppendEntry: 接收到 Append Entry RPC
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeOutNow(m)
	}
	return nil
}

func (r *Raft) followerStep(m pb.Message) error {
	// Follower 能够接收到的消息类型：
	// 1. MsgHup: 表示选举超时，提示candidate开启新的一轮选举
	// 2. MsgHeartBeat: 收到了来自 Leader 的心跳包
	// 3. MsgAppendEntry: 收到了来自 Leader 的日志同步
	// 4. MsgRequestVote: 收到来自 candidate 的投票请求
	// 5. MsgTimeOutNow: 让非 Leader 节点立即选举超时，开启一轮选举；上层做的转移 leadership 的 RPC
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeOutNow(m)
	}

	return nil
}

// Leader send MsgHeartBeat to other nodes
func (r *Raft) bcastHeartBeat() {
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendHeartbeat(to)
	}
	// 重新开始心跳超时计数
	r.heartbeatElapsed = 0
}

// 处理上层或成为Leader时的 MsgPropose
// 先追加日志到本地，然后do_replicate，即发送 MsgAppendEntry 消息
func (r *Raft) handlePropose(m pb.Message) {
	// 先追加日志
	r.prepareForAppendEntries(m.Entries)
	r.RaftLog.appendEntries(m.Entries)

	// 然后进行日志复制，发送 MsgAppendEntry
	if len(r.Prs) == 1 { // 如果集群只有一个节点，那么直接提交
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else { // 因为只有一个节点的话，就不会收到 AppendEntryResponse，也就没法更新committed Index了
		r.bcastAppendEntry()
	}
}

// 为追加日志做处理，需要把 propose 消息中的 Entry 的 Term、Index 信息进行填充
// 上层发送的 propose 信息中只包含数据，因为上层不知道当前集群内部的 Term 和这条日志具体的Index
func (r *Raft) prepareForAppendEntries(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
		// 如果是配置修改信息
		if entries[i].EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entries[i].Index
		}
	}
}

// Leader 广播 AppendEntry，根据 Leader 中记录的 Prs，对每个节点发送[nextIndex, lastIndex]之间的日志
func (r *Raft) bcastAppendEntry() {
	_, confState, err := r.RaftLog.storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := confState.Nodes
	for _, to := range peers {
		if to == r.id {
			continue
		}
		r.sendAppend(to)
	}
}

// handle ReuqestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// 投票：在自己没投票的情况下，保证candidate具有比自己更新的Log序列才能投票
	// 		1. candidate的lastTerm大于接收者的Term
	//		2. candidate的lastTerm和接收者的 Term 一样，但是candidate的lastLogIndex比接收者的lastLogIndex大
	// 拒绝投票：
	//		1. lastTerm < 接收者 Term
	// 		2. lastTerm == r.Term && voted
	//		3. lastTerm == r.Term && lastLogIndex < r.lastLogIndex
	voteResponseMsg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}

	// 只要 candidate 的 preLogTerm 大于自己，那么 candidate 的日志序列一定比自己新
	if m.Term > r.Term { // 这是为了避免在下面的判断中不满足所有条件就不更新 term 了，但是一旦发现有比自己更大的term一定是有人比自己更新，所以需要becomeFollower
		// 这个时候还不确定 Leader，因为自己原本确定的 Leader 一定不是最新的了，但是又不知道现在是谁
		r.becomeFollower(m.Term, None)
	}

	// m.Term 是lastLogTerm
	// 如果接收者上一轮投票投的就是这个candidate，那么说明上一轮就认可了这个candidate，可以继续投
	if ((m.Term > r.Term || m.Term == r.Term) && (r.Vote == None || r.Vote == m.From)) && // 接收者是否可以投票条件
		r.RaftLog.isUpToDateAsMe(m.Term, m.Index) { // candidate 是否具有 up-to-date 的Log序列
		// 选择投票
		r.becomeFollower(m.Term, None) // 不管怎么样，这里已经判断了，candidate 的日志序列更新
		voteResponseMsg.Reject = false
		r.Vote = m.From // 记录投票给谁了
	} else { // 拒绝投票
		voteResponseMsg.Reject = true
	}

	r.msgs = append(r.msgs, voteResponseMsg)
}

// 根据投票信息判断 rolestate 转变成什么
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// 记录投票信息
	r.votes[m.From] = !m.Reject
	count := 0

	for _, agree := range r.votes {
		if agree {
			count++
		}
	}
	majority := len(r.Prs)/2 + 1

	// 如果是拒绝
	if m.Reject {
		if r.Term < m.Term { // 可能是宕机恢复了，回复者的 term 更大
			r.becomeFollower(m.Term, None)
		}

		// 拒绝的人达到了半数以上
		if len(r.votes)-count >= majority {
			r.becomeFollower(r.Term, None)
		}
	} else {
		if count >= majority {
			r.becomeLeader()
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Leader 收到上层的 MsgPropose 后，向其他节点发出 AppendEntry RPC
	//		m.Term 		: Leader 的 term
	//		m.LogTerm 	: preLogTerm
	//		m.Index 	: preLogIndex
	//		m.commit	: leaderCommit
	// 接收者处理：
	// 1. 首先判断发送过来的 term 是否小于本节点的 term，论文：reply false if term < currentterm
	// 2. 判断发过来的消息中带有的 preLogIndex 和 preLogTerm 是否和本节点的日志信息有遗漏或者冲突
	//		2.1 发过来的entries 有遗漏：在 preLogIndex 处都没有这条日志条目，就是判断当前节点的 lastIndex 是否小于 leader 发送来的 preLogIndex
	//			论文：reply false if an entry at prevLogIndex whose term matches prevLogTerm
	//		2.2 判断发过来的 entries 是否和本届点日志冲突: 在 preLogIndex 处有 Log 但是 term 和消息中带的preLogTerm不匹配，删除所有冲突的日志条目，并且全部跟随 leader 的，
	//			这里就是发现 r.lastIndex >= preLogIndex && r.Term(preLogIndex) != preLogTerm
	//			论文：If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// 3. 如果有漏日志或者冲突，那么向Leader reply false，并且附带上冲突日志任期的firstIndex，让leader重新调整自己的 nextIndex 为这个firstIndex（即上一任期的最后一条）
	//		以此避开冲突任期内的所有条目，并且重新发送 AppendEntry 消息，其中带有新的preLogIndex和preLogTerm
	// 4. 如果没有冲突，那么将消息中的entries追加到自己的日志序列中
	//		4.1 如果有重叠的entry，先截断entries并且修改stabledIndex，然后再追加entries，stabledIndex = min(r.RaftLog.stabledIndex, idx - 1),idx是截断日志的firstindex
	//		4.2 如果没有重叠的entry，则直接追加到日志中
	// 5. 返回接收MsgAppendEntryResponse，并更新 committedIndex = min(m.commmitedIndex, m.Index+len(m.entries))

	appendEntryResponseMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	}

	if r.Term > m.Term { // reply false
		appendEntryResponseMsg.Reject = true
		r.msgs = append(r.msgs, appendEntryResponseMsg)
		return
	}

	r.becomeFollower(m.Term, m.From)
	if m.Term > r.Term { // 如果 term 比自己的大那么变成他的 term
		r.Term = m.Term
	}

	if r.RaftLog.LastIndex() < m.Index { // 遗漏了日志
		appendEntryResponseMsg.Index = r.RaftLog.LastIndex() // 让 Leader 更新NextIndex 为 lastIndex + 1
		appendEntryResponseMsg.Reject = true
	}

	if m.Index <= r.RaftLog.LastIndex() { // m.Index <= r.lastIndex, 判断 term 是否冲突
		if logTermOfR, _ := r.RaftLog.Term(m.Index); logTermOfR == m.LogTerm {
			// 这条日志没有冲突，匹配
			// 截断前面可能冲突的日志，然后追加新日志
			if len(m.Entries) > 0 { // 和空 entry 区分
				var (
					idx              = m.Index + 1
					newLogStartIndex = m.Index + 1
				)

				// 找到第一个不匹配的logIndex，准备截断
				for ; idx < r.RaftLog.LastIndex() && idx <= m.Entries[len(m.Entries)-1].Index; idx++ {
					termOfIdxInR, _ := r.RaftLog.Term(idx)
					if termOfIdxInR != m.Entries[idx-newLogStartIndex].Term {
						break
					}
				}

				if idx-newLogStartIndex != uint64(len(m.Entries)) { // 说明是有需要截断的日志，而不是全能匹配
					r.RaftLog.truncate(idx)
					r.RaftLog.stabled = min(r.RaftLog.applied, idx-1)         // idx - 1后面的截断了
					r.RaftLog.appendEntries(m.Entries[idx-newLogStartIndex:]) // 追加日志
				}
			}

			// 在 Leader 更新 commitIndex 后也同步更新 follower 们的 commitIndex
			if m.Commit > r.RaftLog.committed {
				// 论文: 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
			appendEntryResponseMsg.Reject = false                           // 同意
			appendEntryResponseMsg.Index = m.Index + uint64(len(m.Entries)) // 同步nextIndex
			appendEntryResponseMsg.LogTerm, _ = r.RaftLog.Term(appendEntryResponseMsg.Index)
		} else { // 冲突了，返回这个任期的第一条日志的Index
			appendEntryResponseMsg.Reject = true
			conflictTerm := logTermOfR
			for _, entry := range r.RaftLog.entries {
				if entry.Term == conflictTerm {
					appendEntryResponseMsg.Index = entry.Index - 1 // 冲突任期前的第一个条目
					break
				}
			}
		}
	}
	r.msgs = append(r.msgs, appendEntryResponseMsg)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// Your Code Here (2A).
	if m.Reject { // 被拒绝
		if m.Term > r.Term { // 可能是网络分区 或者 选举超时的时间不合理
			r.becomeFollower(m.Term, None)
		} else { // 日志冲突或者漏了
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}

	// replicate 成功，看一下能不能 commit （只有当前任期的leader才能commit，并以此间接commit前面未提交的日志）
	if r.appendResponseHelperMaybeUpdate(m.From, m.Index) { // 说明这条回复可能造成 commitIndex 的更新
		if r.appendResponseHelperMaybeCommit() { // 修改了 commitIndex
			r.bcastAppendEntry()
		}
	}
}

// handleAppendEntriesResponse 中调用的，用于确定接收到的回复是否是过期回复（可能由于网络延迟等原因），进而确认是否可能造成 commitIndex 的更新
func (r *Raft) appendResponseHelperMaybeUpdate(from, index uint64) bool {
	update := false // 不用跟新，表明是过期的消息回复
	if r.Prs[from].Match < index {
		r.Prs[from].Match = index
		r.Prs[from].Next = index + 1
		update = true
	}
	return update
}

// 通过 Prs 的信息判断 commitIndex 是否可以被更新
func (r *Raft) appendResponseHelperMaybeCommit() bool {
	matchSlice := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchSlice = append(matchSlice, progress.Match)
	}
	// 排序获得所有节点的 matchIndex 中位数，中位数就是被大多数节点复制的日志索引
	sort.Sort(matchSlice)
	mid := len(matchSlice)/2 + 1
	readyCommitIndex := matchSlice[mid]

	return r.RaftLog.maybeCommit(readyCommitIndex, r.Term)
}

// TimeOutNow 请求就是上层控制某个节点立即选举超时，开始选举
func (r *Raft) handleTimeOutNow(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	// 直接发起选举
	if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}); err != nil {
		log.Panic(err)
	}
}

// 成为候选者，开启选举，向其他节点发送requestVote RPC
func (r *Raft) handleHup(m pb.Message) {
	r.becomeCandidate()

	// 测试中很多这种处理，只有一个节点，让他成为 candidate 不推进 tick() 就成为了 Leader
	if len(r.Prs) == 1 {
		r.becomeLeader() // 只有一个节点
	}
	// 发起 RequestVote RPC
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote, // 消息类型,发起投票
			To:      id,                            // 节点 ID
			From:    r.id,                          // 候选者编号
			Term:    r.Term,                        // 候选者任期
			Index:   r.RaftLog.LastIndex(),         // 候选者最后一条日志的索引
			LogTerm: r.RaftLog.LastTerm(),          // 候选者最后一条日志的任期
		})
	}

	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleHeartbeat handle Heartbeat RPC request
// 处理 HeartBeat 的，回应HeartBeatResponse MsgType
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 判断 m.Term 是否大于自己，是则变成 follower，否则拒绝
	// 重置选举超时计数，并变成follower，返回 MsgHeartBeatResponse
	HeartBeatResponseMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}

	if r.Term > m.Term { // 论文中的 return false
		HeartBeatResponseMsg.Reject = true
	} else { // 重置选举超时计数，并变成follower
		r.becomeFollower(m.Term, m.From) // 无论是谁接收到了 Leader 的心跳包，说明现在集群中有 leader
	}

	r.msgs = append(r.msgs, HeartBeatResponseMsg)
}

// Leader 处理 HeartBeatResponse 消息
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// 首先判断是否被拒绝心跳包，被拒绝心跳包的唯一原因就是对端的Term大于Leader的Term,可以以此检测网络分区
	if m.Reject {
		r.becomeFollower(m.Term, None) // 不知道谁当 leader 了，等待超时后下一次选举
	} else { // 心跳同步成功，表明双方能够通信，如果需要同步日志，leader 主动
		// 检查该节点的日志是不是和自己是同步的，由于有些节点断开连接并又恢复了链接
		// 因此 leader 需要及时向这些节点同步日志
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() { // 日志
			r.sendAppend(m.From)
		}
	}
}

/* **************** Send Msg to RawNode(store in r.msgs.entries[]),  wait for step ****************** */
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 如果有新的entries就调用 sendAppend RPC 发送 log
// 如果你需要发送消息，只需将其推送到 raft.Raft.msgs ，Raft 收到的所有消息将被传递到 raft.Raft.Step()。
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(uint64(prevLogIndex))

	if err == nil { // 这条 Index 在 Leader 的Log entries中，不是无效index，也不是在snapshot中
		entries := r.RaftLog.entries[prevLogIndex+1:]
		appendEntries := make([]*pb.Entry, 0)
		for i := range entries {
			appendEntries = append(appendEntries, &entries[i])
		}

		sendMsg := pb.Message{
			MsgType:  pb.MessageType_MsgAppend,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			LogTerm:  prevLogTerm, // 需要和follower对齐的依据
			Index:    uint64(prevLogIndex),
			Entries:  appendEntries,
			Commit:   r.RaftLog.committed,
			Snapshot: nil, // 如果发送的不是snapshot 设置为 nil
			// Leader 不用设置 reject 位
		}
		r.msgs = append(r.msgs, sendMsg) // 异步塞进 msgs 里面，等待后续 Step 的处理
		return true
	}

	// 如果错误，且错误的信息是说明在snapshot中
	if err == ErrCompacted {
		// 发送 snapshot 给 follower

		log.Infof("[Snapshot Request] from %d to %d, prevLogIndex %v, dummyIndex %v", r.id, to, prevLogIndex, r.RaftLog.dummyIndex)
	}

	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	heartBeatMsg := pb.Message{ // HeartBeat 只需要发送一条表示Leader的消息即可
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   0, // 测试说传入 LogTerm 和 Index 为 0
		LogTerm: 0,
		// Leader 不用设置 reject 位
	}
	r.msgs = append(r.msgs, heartBeatMsg) // 存在msgs中，异步的等待RawNode取走消息，发送心跳给其他节点
}

/* *************** Node management in raft group ********************* */
// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
