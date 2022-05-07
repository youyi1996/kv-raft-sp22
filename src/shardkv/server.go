package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Operation string // Get Put or Append
	Key       string
	Value     string

	ClientId int
	SeqNum   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	Data        map[string]string
	Channels    map[int]chan Op
	LastApplied map[int]int
	mck         *shardctrler.Clerk
	config      shardctrler.Config
}

var DEBUG = false

// var DEBUG = true

func (kv *ShardKV) DebugPrint(format string, a ...interface{}) {
	// DEBUG PRINT
	// THIS METHOD IS NOT SAFE. RACE MAY OCCUR.

	if DEBUG {
		fmt.Printf("[kvserver:%v] ", kv.me)
		fmt.Printf(format, a...)
	}
}

func CopyConfig(old *shardctrler.Config, new *shardctrler.Config) {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.DebugPrint("Received GET from client: %v\n", args)

	kv.mu.Lock()
	target := kv.config.Shards[key2shard(args.Key)]
	me := kv.gid
	kv.mu.Unlock()
	if target != me {
		kv.DebugPrint("ErrWrongGroup: me %v Expected %v. %v\n", me, target, kv.config)
		reply.Err = ErrWrongGroup
		return
	}

	command := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}

	logIndex, _, isLeader := kv.rf.Start(command)
	reply.ServerId = kv.me

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DebugPrint("Failed because ErrWrongLeader\n")
		return
	}

	// fmt.Printf("[kv%v] Waiting for Lock in GET...\n", kv.me)
	kv.mu.Lock()
	channel, isExist := kv.Channels[logIndex]
	if !isExist {
		channel = make(chan Op, 1)
		kv.Channels[logIndex] = channel
	}
	kv.mu.Unlock()
	// fmt.Printf("[kv%v] Released Lock in GET\n", kv.me)

	select {
	case msg := <-channel:
		if msg.ClientId == command.ClientId && msg.SeqNum == command.SeqNum {
			reply.Err = OK
			reply.Value = msg.Value
			kv.DebugPrint("Success OK. CliendId=%v SeqNum=%v\n", command.ClientId, command.SeqNum)
			return
		} else {
			reply.Err = ErrWrongLeader
			kv.DebugPrint("Failed... Received: [%v:%v]. Expected: [%v:%v]\n", msg.ClientId, msg.SeqNum, command.ClientId, command.SeqNum)
			return
		}
	case <-time.After(2 * time.Second):
		reply.Err = ErrWrongLeader
		kv.DebugPrint("Raft timeout...\n")
		return
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.DebugPrint("Received PutAppend from client: %v\n", args)

	kv.mu.Lock()
	target := kv.config.Shards[key2shard(args.Key)]
	me := kv.gid
	kv.mu.Unlock()
	if target != me {
		kv.DebugPrint("ErrWrongGroup: me %v Expected %v. %v\n", me, target, kv.config)
		reply.Err = ErrWrongGroup
		return
	}

	command := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}

	logIndex, _, isLeader := kv.rf.Start(command)
	reply.ServerId = kv.me

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.DebugPrint("Unsuccess because not leader: %v\n", args)
		return
	}

	kv.mu.Lock()
	channel, isExist := kv.Channels[logIndex]
	if !isExist {
		channel = make(chan Op, 1)
		kv.Channels[logIndex] = channel
	}
	kv.mu.Unlock()

	select {
	case msg := <-channel:
		if msg.ClientId == command.ClientId && msg.SeqNum == command.SeqNum {
			reply.Err = OK
			return
			// fmt.Printf("[kv%v] Successfully PUTAPPEND: %v\n", kv.me, args)
		} else {
			reply.Err = ErrWrongLeader
			kv.DebugPrint("Unsuccess because of id mismatch: %v\n", args)
			return
		}
	case <-time.After(2 * time.Second):
		reply.Err = ErrWrongLeader
		kv.DebugPrint("Unsuccess because of timeout: %v\n", args)
		return
	}

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) receiveApplyMsg() {
	for {
		applyMsg := <-kv.applyCh
		kv.DebugPrint("Received applyMsg %v. \n", applyMsg)

		if applyMsg.SnapshotValid {
			kv.DebugPrint("Trying to install snapshot...\n")
			go func() {
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.DebugPrint("Raft install successfully.\n")
					kv.recover(applyMsg.Snapshot)
				} else {
					kv.DebugPrint("Raft install Failed.\n")

				}
			}()
			continue
		}

		if applyMsg.CommandValid {
			if applyMsg.CommandIndex <= 0 {
				continue
			}
			command := applyMsg.Command.(Op)
			idx := applyMsg.CommandIndex

			kv.mu.Lock()
			if command.Operation == "Get" {
				command.Value = kv.Data[command.Key]
			} else {
				lastApplied, isExist := kv.LastApplied[command.ClientId]
				if !isExist || lastApplied < command.SeqNum {
					if command.Operation == "Put" {
						kv.Data[command.Key] = command.Value
					} else if command.Operation == "Append" {
						kv.Data[command.Key] += command.Value
					}

					kv.LastApplied[command.ClientId] = command.SeqNum
				}
			}
			channel, isExist := kv.Channels[idx]
			if !isExist {
				channel = make(chan Op, 1)
				kv.Channels[idx] = channel
			}
			channel <- command

			// Check if need snapshot
			if kv.maxraftstate >= 0 && float32(kv.rf.GetPersisterStateSize()) > float32(kv.maxraftstate)*.8 {
				kv.DebugPrint("Need snapshot! maxraftstate:%v, currentSize: %v. currentData: %v\n", kv.maxraftstate, kv.rf.GetPersisterStateSize(), kv.Data)

				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.Data)
				e.Encode(kv.LastApplied)
				snapshot := w.Bytes()
				kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
			}
			kv.mu.Unlock()

		}

		// time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) pollConfig() {
	for {
		// _, isLeader := kv.rf.GetState()
		if true {
			// kv.DebugPrint("PollConfig triggered!\n")
			latest_config := kv.mck.Query(-1)
			if kv.config.Num != latest_config.Num {
				kv.DebugPrint("New config arrived! Updating... %v\n", latest_config)

				var new_config shardctrler.Config

				CopyConfig(&latest_config, &new_config)

				kv.mu.Lock()
				kv.config = new_config
				kv.mu.Unlock()
				kv.DebugPrint("Updated! %v\n", kv.config)

			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.Data = make(map[string]string)
	kv.Channels = make(map[int]chan Op)
	kv.LastApplied = make(map[int]int)
	kv.config = shardctrler.Config{
		Num: 0,
	}
	kv.recover(persister.ReadSnapshot())

	go kv.receiveApplyMsg()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.pollConfig()

	return kv
}

func (kv *ShardKV) recover(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data := make(map[string]string)
	lastApplied := make(map[int]int)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&data)
	d.Decode(&lastApplied)

	kv.DebugPrint("Enter recover.\n")
	// fmt.Println(data)
	// fmt.Println(lastApplied)

	kv.Data = data
	kv.LastApplied = lastApplied

}
