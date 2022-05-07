package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
	SeqNum   int
}

type PutAppendReply struct {
	Err      Err
	ServerId int // For debug purpose
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int
	SeqNum   int
}

type GetReply struct {
	Err      Err
	Value    string
	ServerId int // For debug purpose
}

type MoveArgs struct {
	Shard int
	Num   int
}

type MoveReply struct {
	Err         Err
	Shard       int
	Num         int
	Data        map[string]string
	LastApplied map[int]int
}
