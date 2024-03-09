package shardctrler

import "sort"

type CtrlerStateMachine struct {
	Configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{Configs: make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (cf *CtrlerStateMachine) Query(num int) (Config, Err) {
	DPrintf("CtrlerStateMachine.Query: %v", num)
	if num < 0 || num >= len(cf.Configs) {
		return cf.Configs[len(cf.Configs)-1], OK
	}
	return cf.Configs[num], OK
}

func (cf *CtrlerStateMachine) Join(groups map[int][]string) Err {
	DPrintf("CtrlerStateMachine.Join: %v", groups)
	num := len(cf.Configs)
	newConfig := cf.Configs[num-1].Clone()
	newConfig.Num = num

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}

	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	for {
		maxGid, minGid := gidWithMaxShardsFrom(gidToShards), gidWithMinShardsFrom(gidToShards)
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}

		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		gidToShards[maxGid] = gidToShards[maxGid][1:]

	}

	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}

	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, *newConfig)

	return OK
}

func (cf *CtrlerStateMachine) Leave(gids []int) Err {
	num := len(cf.Configs)
	newConfig := cf.Configs[num-1].Clone()
	newConfig.Num = num

	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}

	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	var unassignedShards []int
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		for _, shard := range unassignedShards {
			minGid := gidWithMinShardsFrom(gidToShards)
			gidToShards[minGid] = append(gidToShards[minGid], shard)
		}

		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}

	}

	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, *newConfig)
	return OK
}

func (cf *CtrlerStateMachine) Move(shard, gid int) Err {
	num := len(cf.Configs)
	newConfig := cf.Configs[num-1].Clone()
	newConfig.Num = num

	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, *newConfig)
	return OK
}

func gidWithMaxShardsFrom(gidToShards map[int][]int) int {
	if shard, ok := gidToShards[0]; ok && len(shard) > 0 {
		return 0
	}

	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		if len(gidToShards[gid]) > maxShards {
			maxGid, maxShards = gid, len(gidToShards[gid])
		}
	}
	return maxGid
}

func gidWithMinShardsFrom(gidToShards map[int][]int) int {
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToShards[gid]) < minShards {
			minGid, minShards = gid, len(gidToShards[gid])
		}
	}
	return minGid
}
