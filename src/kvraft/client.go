package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	LeaderId int
	ClientId int
	SeqNum   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.LeaderId = 0
	ck.ClientId = int(nrand())
	ck.SeqNum = 0

	// fmt.Printf("[client] Initialized Client.\n")

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// fmt.Printf("[client] Trying GET %v\n", key)
	ck.mu.Lock()
	ck.SeqNum += 1
	seqNum := ck.SeqNum
	leaderId := ck.LeaderId
	clientId := ck.ClientId
	ck.mu.Unlock()

	args := GetArgs{
		Key:      key,
		ClientId: clientId,
		SeqNum:   seqNum,
	}

	first_visited := false

	ret := ""

	for serverId := leaderId; serverId != leaderId || !first_visited; serverId = (serverId + 1) % len(ck.servers) {
		reply := GetReply{}

		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == ErrWrongLeader {
				continue
			} else {
				if reply.Err == OK {
					ret = reply.Value
				}
				ck.mu.Lock()
				ck.LeaderId = serverId
				ck.mu.Unlock()
				break
			}
		}

	}

	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// fmt.Printf("[client] Trying %v %v with %v\n", op, key, value)

	ck.mu.Lock()
	ck.SeqNum += 1
	seqNum := ck.SeqNum
	leaderId := ck.LeaderId
	clientId := ck.ClientId
	ck.mu.Unlock()

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: clientId,
		SeqNum:   seqNum,
	}

	first_visited := false

	for serverId := leaderId; serverId != leaderId || !first_visited; serverId = (serverId + 1) % len(ck.servers) {
		reply := PutAppendReply{}

		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == ErrWrongLeader {
				continue
			} else {
				ck.mu.Lock()
				ck.LeaderId = serverId
				ck.mu.Unlock()
				break
			}
		}

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
