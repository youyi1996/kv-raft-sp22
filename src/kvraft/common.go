package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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
	ServerId int
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
	ServerId int
}
