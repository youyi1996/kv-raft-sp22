package shardctrler

import (
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	Channels    map[int]chan Op
	LastApplied map[int]int
}

type Op struct {
	// Your data here.

	Operation string // Join, Leave, Move or Query

	// Parameters in common
	ClientId int
	SeqNum   int

	// Parameters for Join
	JoinServers map[int][]string

	// Parameters for Leave
	LeaveGIDs []int

	// Parameters for Move
	MoveShard int
	MoveGID   int

	// Parameters for Query
	QueryNum         int
	QueryReplyConfig Config
}

func (sc *ShardCtrler) DebugPrint(format string, a ...interface{}) {
	// DEBUG PRINT
	// THIS METHOD IS NOT SAFE. RACE MAY OCCUR.

	// fmt.Printf("[sc%v] ", sc.me)
	// fmt.Printf(format, a...)
}

func CopyConfig(old *Config, new *Config) {
	// Copy the config from old to new.
	new.Num = old.Num
	new.Groups = make(map[int][]string, 0)
	for i, gid := range old.Shards {
		new.Shards[i] = gid
	}
	for gid, servers := range old.Groups {
		new_servers := make([]string, 0)
		for _, server := range servers {
			new_servers = append(new_servers, server)
		}
		new.Groups[gid] = new_servers
	}
}

func OptimizeShards(config *Config) {

	const UintSize = 32 << (^uint(0) >> 32 & 1)
	const MaxInt = 1<<(UintSize-1) - 1
	smallest_key := MaxInt

	// Find a group with smallest key.
	for k := range config.Groups {
		if k < smallest_key {
			smallest_key = k
		}
	}

	for sid, gid := range config.Shards {
		if len(config.Groups) == 0 {
			config.Shards[sid] = 0
		} else if gid == 0 && len(config.Groups) > 0 {
			config.Shards[sid] = smallest_key
		} else {
			_, isExisted := config.Groups[gid]
			if !isExisted {
				config.Shards[sid] = smallest_key
			}
		}
	}

	for {
		group2shard := make(map[int][]int, 0)

		max_num := 0
		max_gid := -1
		min_num := 999999999
		min_gid := -1

		for gid, _ := range config.Groups {
			group2shard[gid] = make([]int, 0)
		}

		for sid, gid := range config.Shards {
			group2shard[gid] = append(group2shard[gid], sid)
		}
		for gid, shards := range group2shard {
			count := len(shards)

			// These tricks are used to avoid undeterministic behaviors when traversing a map.
			if count > max_num || (count == max_num && gid > max_gid) {
				max_num = count
				max_gid = gid
			}
			if count < min_num || (count == min_num && gid < min_gid) {
				min_num = count
				min_gid = gid
			}
		}
		// fmt.Printf("********max_num %v min_num %v", max_num, min_num)
		// fmt.Printf("********group2shard %v\n", group2shard)

		if max_num-min_num <= 1 {
			// Current configuration is optimal
			break
		} else {
			// Move one shard from the most crowded to the most uncrowded.
			sid := group2shard[max_gid][0]
			config.Shards[sid] = min_gid
		}

	}
	return
}

func (sc *ShardCtrler) GetChannel(LogIndex int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	channel, isExist := sc.Channels[LogIndex]
	if !isExist {
		channel = make(chan Op, 1)
		sc.Channels[LogIndex] = channel
	}

	return channel
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	command := Op{
		Operation: "Join",
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,

		JoinServers: args.Servers,
	}

	logIndex, _, isLeader := sc.rf.Start(command)

	reply.ServerId = sc.me

	if !isLeader {
		reply.WrongLeader = true
		sc.DebugPrint("ERROR! NOT A LEADER.\n")
		return
	}

	channel := sc.GetChannel(logIndex)

	select {
	case msg := <-channel:
		if msg.ClientId == command.ClientId && msg.SeqNum == command.SeqNum {
			reply.Err = OK
			sc.DebugPrint("Success OK. CliendId=%v SeqNum=%v\n", command.ClientId, command.SeqNum)
			return
		} else {
			reply.Err = "REPLY NOT CONSISTENT"
			fmt.Printf("Failed... Received: [%v:%v]. Expected: [%v:%v]\n", msg.ClientId, msg.SeqNum, command.ClientId, command.SeqNum)
			return
		}
	case <-time.After(2 * time.Second):
		reply.Err = "TIMEOUT"
		sc.DebugPrint("Raft timeout...\n")
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	command := Op{
		Operation: "Leave",
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,

		LeaveGIDs: args.GIDs,
	}

	logIndex, _, isLeader := sc.rf.Start(command)

	reply.ServerId = sc.me

	if !isLeader {
		reply.WrongLeader = true
		sc.DebugPrint("ERROR! NOT A LEADER.\n")
		return
	}

	channel := sc.GetChannel(logIndex)

	select {
	case msg := <-channel:
		if msg.ClientId == command.ClientId && msg.SeqNum == command.SeqNum {
			reply.Err = OK
			sc.DebugPrint("Success OK. CliendId=%v SeqNum=%v\n", command.ClientId, command.SeqNum)
			return
		} else {
			reply.Err = "REPLY NOT CONSISTENT"
			fmt.Printf("Failed... Received: [%v:%v]. Expected: [%v:%v]\n", msg.ClientId, msg.SeqNum, command.ClientId, command.SeqNum)
			return
		}
	case <-time.After(2 * time.Second):
		reply.Err = "TIMEOUT"
		sc.DebugPrint("Raft timeout...\n")
		return
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	command := Op{
		Operation: "Move",
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,

		MoveShard: args.Shard,
		MoveGID:   args.GID,
	}

	logIndex, _, isLeader := sc.rf.Start(command)

	reply.ServerId = sc.me

	if !isLeader {
		reply.WrongLeader = true
		sc.DebugPrint("ERROR! NOT A LEADER.\n")
		return
	}

	channel := sc.GetChannel(logIndex)

	select {
	case msg := <-channel:
		if msg.ClientId == command.ClientId && msg.SeqNum == command.SeqNum {
			reply.Err = OK
			sc.DebugPrint("Success OK. CliendId=%v SeqNum=%v\n", command.ClientId, command.SeqNum)
			return
		} else {
			reply.Err = "REPLY NOT CONSISTENT"
			fmt.Printf("Failed... Received: [%v:%v]. Expected: [%v:%v]\n", msg.ClientId, msg.SeqNum, command.ClientId, command.SeqNum)
			return
		}
	case <-time.After(2 * time.Second):
		reply.Err = "TIMEOUT"
		sc.DebugPrint("Raft timeout...\n")
		return
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op{
		Operation: "Query",
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,

		QueryNum: args.Num,
	}

	logIndex, _, isLeader := sc.rf.Start(command)

	reply.ServerId = sc.me

	if !isLeader {
		reply.WrongLeader = true
		sc.DebugPrint("ERROR! NOT A LEADER.\n")
		return
	}

	channel := sc.GetChannel(logIndex)

	select {
	case msg := <-channel:
		if msg.ClientId == command.ClientId && msg.SeqNum == command.SeqNum {
			reply.Err = OK
			reply.Config = msg.QueryReplyConfig
			sc.DebugPrint("Success OK. CliendId=%v SeqNum=%v\n", command.ClientId, command.SeqNum)
			return
		} else {
			reply.Err = "REPLY NOT CONSISTENT"
			fmt.Printf("Failed... Received: [%v:%v]. Expected: [%v:%v]\n", msg.ClientId, msg.SeqNum, command.ClientId, command.SeqNum)
			return
		}
	case <-time.After(2 * time.Second):
		reply.Err = "TIMEOUT"
		sc.DebugPrint("Raft timeout...\n")
		return
	}

}

func (sc *ShardCtrler) receiveApplyMsg() {
	for {
		applyMsg := <-sc.applyCh
		sc.DebugPrint("Received applyMsg %v. \n", applyMsg)

		if applyMsg.CommandValid {
			if applyMsg.CommandIndex <= 0 {
				continue
			}
			command := applyMsg.Command.(Op)
			idx := applyMsg.CommandIndex

			sc.mu.Lock()

			if command.Operation == "Query" {
				if command.QueryNum == -1 || command.QueryNum >= len(sc.configs)-1 {
					command.QueryReplyConfig = sc.configs[len(sc.configs)-1]
				} else {
					command.QueryReplyConfig = sc.configs[command.QueryNum]
				}
			} else {
				// Check if the applyMsg is duplicated...
				lastApplied, isExisted := sc.LastApplied[command.ClientId]
				isDuplicated := isExisted && lastApplied >= command.SeqNum

				// If the applyMsg is duplicated, it means the operation has already been done.
				// Skip these operations and just send a dummy success message back.
				if !isDuplicated {
					if command.Operation == "Join" {
						oldConfig := sc.configs[len(sc.configs)-1]
						var newConfig Config
						CopyConfig(&oldConfig, &newConfig)
						for key, element := range command.JoinServers {
							newConfig.Groups[key] = element
						}
						sc.DebugPrint("New Config before optimize: %v\n", newConfig)
						OptimizeShards(&newConfig)
						newConfig.Num += 1
						sc.DebugPrint("New Config: %v\n", newConfig)
						sc.configs = append(sc.configs, newConfig)

					} else if command.Operation == "Leave" {
						oldConfig := sc.configs[len(sc.configs)-1]
						var newConfig Config
						CopyConfig(&oldConfig, &newConfig)

						for _, gid := range command.LeaveGIDs {
							_, isExisted := newConfig.Groups[gid]
							if isExisted {
								delete(newConfig.Groups, gid)
							}
						}
						OptimizeShards(&newConfig)
						newConfig.Num += 1
						sc.configs = append(sc.configs, newConfig)

					} else if command.Operation == "Move" {
						oldConfig := sc.configs[len(sc.configs)-1]
						var newConfig Config
						CopyConfig(&oldConfig, &newConfig)

						newConfig.Shards[command.MoveShard] = command.MoveGID
						newConfig.Num += 1
						sc.configs = append(sc.configs, newConfig)

					}
				}

			}
			sc.DebugPrint("Configs: %v. \n", sc.configs)

			sc.LastApplied[command.ClientId] = command.SeqNum
			sc.mu.Unlock()
			channel := sc.GetChannel(idx)
			channel <- command

		}

	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)

	// Your code here.
	sc.Channels = make(map[int]chan Op)
	sc.LastApplied = make(map[int]int)
	go sc.receiveApplyMsg()

	// fmt.Printf("******------Server %v started.", me)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// fmt.Printf("******------Server %v's RAFT started.", sc.rf.Log)

	return sc
}
