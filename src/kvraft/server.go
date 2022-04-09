package kvraft

import (
	// "fmt"
	"log"
	"sync"
	"sync/atomic"

	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Data        map[string]string
	Channels    map[int]chan Op
	LastApplied map[int]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	command := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}

	logIndex, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
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
			reply.Value = msg.Value
		} else {
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrWrongLeader
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// if args.SeqNum%100 == 0 {
	// 	fmt.Printf("[kv%v] Received PutAppend from client: %v\n", kv.me, args)
	// }

	command := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}

	logIndex, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		// fmt.Printf("[kv%v] Unsuccess because not leader: %v\n", kv.me, args)
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
			// fmt.Printf("[kv%v] Successfully PUTAPPEND: %v\n", kv.me, args)
		} else {
			reply.Err = ErrWrongLeader
			// fmt.Printf("[kv%v] Unsuccess because of id mismatch: %v\n", kv.me, args)
			return
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrWrongLeader
		// fmt.Printf("[kv%v] Unsuccess because of timeout: %v\n", kv.me, args)
		return
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) receiveApplyMsg() {
	for {
		applyMsg := <-kv.applyCh

		if applyMsg.CommandValid {
			if applyMsg.CommandIndex == 0 {
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
			kv.mu.Unlock()
		}

		// time.Sleep(time.Millisecond * 100)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	// You may need initialization code here.

	kv.Data = make(map[string]string)
	kv.Channels = make(map[int]chan Op)
	kv.LastApplied = make(map[int]int)

	go kv.receiveApplyMsg()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// fmt.Printf("[kv%v] Initialized KVServer.\n", kv.me)

	return kv
}
