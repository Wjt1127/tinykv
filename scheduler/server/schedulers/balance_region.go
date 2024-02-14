// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// 为了能够对 suitablestore 进行 region size 排序
type sutibleStoreSlice []*core.StoreInfo

func (a sutibleStoreSlice) Len() int           { return len(a) }
func (a sutibleStoreSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sutibleStoreSlice) Less(i, j int) bool { return a[i].GetRegionSize() < a[j].GetRegionSize() }

// Schedule 做负载均衡，避免太多 region 堆积在一个 store（也可以多加一点优化，做 balance-leader）
// TODO: add balance-leader
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. 选出所有的 suitableStores: 合适的store应该是up的，
	// 	  down的时间不能长于集群的 MaxStoreDownTime
	stores := make(sutibleStoreSlice, 0)
	for _, store := range cluster.GetStores() {
		// 适合被移动的 store 需要满足停机时间不超过 MaxStoreDownTime
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	if len(stores) < 2 { // 如果合适的 store 都没两个，那也不用平衡了
		return nil
	}

	// 2. 遍历 suitableStores，找到目标 region 和 store
	//    根据 store 中的 region 大小对它们进行排序，然后尝试找到 region 大小最大的 store
	//    Scheduler框架提供了三种获取 region 的方法。GetPendingRegionsWithLock、GetFollowersWithLock、
	//    GetLeadersWithLock, 然后你再从中随机选择一个区域。
	sort.Sort(stores)
	var sourceStore, destStore *core.StoreInfo
	var sourceRegion *core.RegionInfo
	for i := len(stores) - 1; i >= 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		sourceRegion = regions.RandomRegion(nil, nil)
		if sourceRegion != nil {
			sourceStore = stores[i]
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		sourceRegion = regions.RandomRegion(nil, nil)
		if sourceRegion != nil {
			sourceStore = stores[i]
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		sourceRegion = regions.RandomRegion(nil, nil)
		if sourceRegion != nil {
			sourceStore = stores[i]
			break
		}
	}
	if sourceStore == nil {
		return nil
	}

	// 3. 判断目标 region 的 store 数量，如果小于 cluster.GetMaxReplicas 直接放弃本次操作
	storeIds := sourceRegion.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}

	// 4. 再次从 suitableStores 里面找到 destStore，destStore 不能在 sourceRegion 里面
	//    从 suitableStore 中最小的 regionSize 开始遍历判断
	for i := 0; i < len(stores); i++ {
		if _, ok := storeIds[stores[i].GetID()]; !ok {
			destStore = stores[i]
			break
		}
	}
	if destStore == nil {
		return nil
	}

	// 5. 判断两个 store 的 region size 差值是否小于 2*ApproximateSize，是的话放弃 region 移动
	//    调度程序希望不要下次又要再移回，所以希望本次的 balance 操作是有价值的
	if sourceStore.GetRegionSize()-destStore.GetRegionSize() < sourceRegion.GetApproximateSize() {
		return nil
	}

	// 6. 在目标 store 上创建一个 peer，然后调用 CreateMovePeerOperator 生成转移请求
	newPeer, _ := cluster.AllocPeer(destStore.GetID()) // 分配 peer 元数据结构
	desc := fmt.Sprintf("move-from-%d-to-%d", sourceStore.GetID(), destStore.GetID())
	op, _ := operator.CreateMovePeerOperator(desc, cluster, sourceRegion, operator.OpBalance, sourceStore.GetID(), destStore.GetID(), newPeer.GetId())

	return op
}
