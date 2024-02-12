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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 就是论文里面的committedIndex，节点认为哪些是已经提交的日志的索引
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 论文中的 lastApplied，即节点最新应用到状态机的日志的索引
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 被持久化日志的最后一条日志的索引，后面开始就是未持久化日志的索引
	stabled uint64

	// all entries that have not yet compact.
	// 所有未被 compact 的日志，包括持久化与非持久化。
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 待处理快照, 快照是先 install 到本地但是还没有apply，apply也是需要时间的
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// entries[0]对应的日志索引（因为在持久化或snapshot后，entries从下标0开始），需要记录日志的真实索引
	// 避免多个节点有的进行了持久化，有的没有，如果没有真实索引，使用下标作为索引可能会造成一些错误
	// 在 storage 中写了：ents[i] has raft log position i+snapshot.Metadata.Index
	// 其实这里的dummyIndex就是可以理解成这个snapshot.Metadata.Index,也就是ents[0]的索引
	// 作用：1.可以定位真实index  2.对于matchIndex之类需要 -1 的index来说，可以减少边界判断
	dummyIndex uint64 // 在 (2A) 里面entries全部存下来了，可以使用 entries 的下标选取，但是当有snapshot之后，需要真实索引
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndexInStorage, err := storage.FirstIndex() // 返回的第一个有效log的真实index, entry[0].index + 1
	if err != nil {
		panic(err)
	}

	lastIndexInStorage, err := storage.LastIndex() // 最后一个log的真实index
	if err != nil {
		panic(err)
	}

	entriesInStorage, err := storage.Entries(firstIndexInStorage, lastIndexInStorage+1)
	if err != nil {
		panic(err)
	}

	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	//	snapshot/first.....applied....committed....stabled.....last
	//	--------|------------------------------------------------|
	//	                          log entries
	rlog := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		// 因为recover from storage，那么一定是从snapshot恢复数据状态信息，然后再执行当前commitedIndex之前的数据，即使这些log之前执行过一次，但是这次因为snapshot重置了
		applied:         firstIndexInStorage - 1,
		stabled:         lastIndexInStorage, // 都是从storage中恢复的，一定是stabled
		entries:         entriesInStorage,
		pendingSnapshot: nil,                 // 2A是没有做snapshot的，后面需要
		dummyIndex:      firstIndexInStorage, // 记录的是entries[0]的下标
	}

	return rlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// compact后持久存储中的第一个日志index作为新的dummyIndex
	newfirstIndex, _ := l.storage.FirstIndex()
	if newfirstIndex > l.dummyIndex {
		newEntries := l.entries[newfirstIndex-l.dummyIndex:]
		l.entries = make([]pb.Entry, 0)
		l.entries = append(l.entries, newEntries...)
	}
	l.dummyIndex = newfirstIndex
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries // 初始化的时候是从storage中恢复的，去掉了dummyEntry的
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.LastIndex()-l.stabled == 0 {
		return make([]pb.Entry, 0)
	}
	return l.getEntries(l.stabled+1, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.dummyIndex+1 : l.committed-l.dummyIndex+1]
	}
	return make([]pb.Entry, 0)
}

// 追加新日志，返回最后一条日志index
func (l *RaftLog) appendEntries(entries []*pb.Entry) uint64 {
	for i := range entries {
		l.entries = append(l.entries, *entries[i])
	}
	return l.LastIndex()
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex + uint64(len(l.entries)) - 1
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	lastIndex := l.LastIndex() - l.dummyIndex
	return l.entries[lastIndex].Term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term, nil
	}

	// 存在 pendingSnapshot ，且这个 Index 是在 pendingSnapshot 中的最后一条日志
	// 因为只能确认最后一个log的index和term
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// 否则就只能在pendingSnapshot之前已经apply了的snapshot中
	term, err := l.storage.Term(i)

	return term, err
}

// 根据 candidate 传来的PreLogTerm 和 PreLogIndex 判断是否比自己的日志序列新
func (l *RaftLog) isUpToDateAsMe(mLastLogTerm, mLastLogIndex uint64) bool {
	return mLastLogTerm > l.LastTerm() || (mLastLogTerm == l.LastTerm() && mLastLogIndex >= l.LastIndex())
}

// truncate掉冲突的日志序列，保留前面的
func (l *RaftLog) truncate(startIndex uint64) {
	if len(l.entries) > 0 {
		l.entries = l.entries[:startIndex-l.dummyIndex]
	}
}

func (l *RaftLog) maybeCommit(readyCommitIndex, term uint64) bool {
	commitTerm, _ := l.Term(readyCommitIndex)

	// 只有 Leader 任期内的 Index 才能commit，并以此间接提交之前任期的日志
	if commitTerm == term && readyCommitIndex > l.committed {
		// 只有当该日志被大多数节点复制（函数调用保证），并且日志索引大于当前的commitIndex
		// 并且该日志是当前任期内创建的日志，才可以提交这条日志
		// 【注】为了一致性，Raft 永远不会通过计算副本的方式提交之前任期的日志，只能通过提交当前任期的日志一并提交之前所有的日志
		l.committed = readyCommitIndex
		return true
	}

	return false
}

// 返回 index 在 [start,end) 中的entries
// end = 0 : 表示获取[start:]的日志
func (l *RaftLog) getEntries(start, end uint64) []pb.Entry {
	if end == 0 {
		end = l.LastIndex() + 1
	}

	start, end = start-l.dummyIndex, end-l.dummyIndex
	return l.entries[start:end]
}
