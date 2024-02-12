package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

/*
 * raft worker 轮询 raftCh 以获得消息，这些消息包括驱动 Raft 模块的基本 tick 和作为 Raft 日志项的 Raft 命令；
 */

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	raftCh chan message.Msg
	ctx    *GlobalContext

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh: pm.peerSender,
		ctx:    ctx,
		pr:     pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0]
		select {
		case <-closeCh:
			return
		case msg := <-rw.raftCh: // 接受各种 Msg : 其中包括tick、raft间的同步消息、上层应用的msg等等
			msgs = append(msgs, msg)
		}
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		peerStateMap := make(map[uint64]*peerState)
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}

			// HandleMsg:  根据 Msg 的类型进行处理
			// 如果是 raft 层节点间同步的 MsgTypeRaftMessage，那么直接走 raft 层的 step 函数即可
			// 如果是上层应用 propose 的读写请求，那么走 proposeRaftCommand，先将 requestCmd 转化为 entry 格式，然后再分类处理 AdminRequest(changeConf类型的操作、compact类型) 和 NormalRequest（Get/Put/Delete/Snap）
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleMsg(msg)
		}
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.ctx).HandleRaftReady()
		}
	}
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}
