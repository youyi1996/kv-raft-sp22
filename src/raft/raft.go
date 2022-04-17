package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	Role int // 0 for follower, 1 for candidate, 2 for leader.

	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	ElectionTimer  *time.Timer
	HeartBeatTimer *time.Timer

	NumReceivedVotes int

	SnapShotOffset int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	// fmt.Printf("----[%v] Enter GetState().\n", rf.me)

	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = (rf.Role == 2)
	rf.mu.Unlock()
	// fmt.Printf("----[%v] GetState(), %v, %v.\n", rf.me, term, isleader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	// e.Encode(rf.SnapShotOffset)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	// var SnapShotOffset int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil {
		log.Fatal("error!")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
		// rf.SnapShotOffset = SnapShotOffset
		// fmt.Printf("%v\n", rf.Log)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("[%v:t%v:%v] Entered CondInstallSnapshot. %v\n", rf.me, rf.CurrentTerm, rf.Role, lastIncludedIndex)

	if rf.CommitIndex >= lastIncludedIndex {
		// more committed info than snapshot, do not need snapshot
		return false
	}

	if len(rf.Log)+rf.SnapShotOffset-1 > lastIncludedIndex {
		// have more logs saved than snapshot, turncate them
		// fmt.Printf("[%v:t%v:%v] CurrentSnapshotOffset=%v. lastIncludedIndex=%v. Log=%v, len=%v\n", rf.me, rf.CurrentTerm, rf.Role, rf.SnapShotOffset, lastIncludedIndex, rf.Log, len(rf.Log))
		rf.Log = rf.Log[lastIncludedIndex-rf.SnapShotOffset:]
		// fmt.Printf("[%v:t%v:%v] Turncated logs from %v. \n", rf.me, rf.CurrentTerm, rf.Role, lastIncludedIndex-rf.SnapShotOffset)

	} else {
		// fewer logs than snapshot, discard entirely
		rf.Log = make([]LogEntry, 0)
		dummyLog := LogEntry{
			Term:    lastIncludedTerm,
			Command: "1000000",
		}
		rf.Log = append(rf.Log, dummyLog)
		// fmt.Printf("[%v:t%v:%v] Discarded all logs. \n", rf.me, rf.CurrentTerm, rf.Role)
	}
	rf.SnapShotOffset = lastIncludedIndex
	rf.CommitIndex = lastIncludedIndex
	rf.LastApplied = lastIncludedIndex

	// fmt.Printf("[%v:t%v:%v] Updated Snapshotoffset to %v. \n", rf.me, rf.CurrentTerm, rf.Role, lastIncludedIndex)

	// call presister
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	// e.Encode(rf.SnapShotOffset)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)

	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("[%v:t%v:%v] Received InstallSnapshot from leader. %v\n", rf.me, rf.CurrentTerm, rf.Role, args)

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.Role = 0
	}
	rf.persist()

	reply.Term = rf.CurrentTerm

	// fmt.Printf("[%v:t%v:%v] args.LastIncludedIndex=%v, rf.SnapShotOffset=%v\n", rf.me, rf.CurrentTerm, rf.Role, args.LastIncludedIndex, rf.SnapShotOffset)

	if args.LastIncludedIndex <= rf.SnapShotOffset {
		// No need to install snapshot as it is outdated
		return
	} else {
		message := ApplyMsg{
			CommandValid:  false,
			CommandIndex:  args.LastIncludedIndex,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.applyCh <- message
		// fmt.Printf("[%v:t%v:%v] SnapshotApplied. %v\n", rf.me, rf.CurrentTerm, rf.Role, message)

		return
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	if rf == nil {
		return
	}

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if index > rf.SnapShotOffset {

			rf.Log = rf.Log[index-rf.SnapShotOffset:]
			rf.SnapShotOffset = index
			// fmt.Printf("[%v:t%v:%v] Updated Snapshotoffset to %v. \n", rf.me, rf.CurrentTerm, rf.Role, index)

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(rf.CurrentTerm)
			e.Encode(rf.VotedFor)
			e.Encode(rf.Log)
			// e.Encode(rf.SnapShotOffset)
			data := w.Bytes()
			rf.persister.SaveStateAndSnapshot(data, snapshot)

		} else {
			return
		}

	}()

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ExpectedIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) ResetElectionTimer() {
	// rf.mu.Lock()
	rf.ElectionTimer.Reset(time.Duration(rand.Intn(1000)+300) * time.Millisecond)
	// rf.mu.Unlock()
}

func (rf *Raft) ResetHeartBeatTimer() {
	// rf.mu.Lock()
	rf.HeartBeatTimer.Reset(100 * time.Millisecond)
	// rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// fmt.Printf("[%v:t%v:%v] Received RequestVote from %v. %v\n", rf.me, rf.CurrentTerm, rf.Role, args.CandidateId, args)

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.Role = 0
		rf.ResetElectionTimer()
	}

	if args.Term < rf.CurrentTerm {
		// fmt.Printf("[%v, t%v] No vote for %v due to outdated term.\n", rf.me, rf.CurrentTerm, args.CandidateId)

		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.VotedFor != -1 && args.CandidateId != rf.VotedFor {
		// fmt.Printf("[%v, t%v] No vote for %v due to already voted for %v.\n", rf.me, rf.CurrentTerm, args.CandidateId, rf.VotedFor)

		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	// fmt.Printf("[%v, t%v] len(rf.Log)=%v, SnapShotOffset=%v.\n", rf.me, rf.CurrentTerm, len(rf.Log), rf.SnapShotOffset)
	lastLogTerm := rf.Log[len(rf.Log)-1].Term
	lastLogIndex := len(rf.Log) + rf.SnapShotOffset - 1

	if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		// fmt.Printf("[%v:t%v:%v] Voted for %v. %v, [%v:%v]. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, args.CandidateId, args, lastLogTerm, lastLogIndex, rf.Log)

		rf.Role = 0

		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm

	} else {
		// fmt.Printf("[%v, t%v] No vote for %v due to outdated logs.\n", rf.me, rf.CurrentTerm, args.CandidateId)

		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AskForOneVote(server int) {

	rf.mu.Lock()
	// fmt.Printf("[%v:t%v:%v] Locked in AskForOneVote Phase 1.\n", rf.me, rf.CurrentTerm, rf.Role)

	// fmt.Printf("[%v:t%v:%v] len(rf.Log)=%v, rf.SnapShotOffset=%v.\n", rf.me, rf.CurrentTerm, rf.Role, len(rf.Log), rf.SnapShotOffset)

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Log) + rf.SnapShotOffset - 1,
		LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
	}
	rf.mu.Unlock()
	// fmt.Printf("[%v:t%v:%v] Unlocked in AskForOneVote Phase 1.\n", rf.me, rf.CurrentTerm, rf.Role)

	reply := RequestVoteReply{}

	// fmt.Printf("[%v:t%v:%v] Sends vote request to %v. %v\n", rf.me, rf.CurrentTerm, rf.Role, server, args)

	if rf.sendRequestVote(server, &args, &reply) {
		rf.mu.Lock()
		// fmt.Printf("[%v:t%v:%v] Locked in AskForOneVote Phase 2.\n", rf.me, rf.CurrentTerm, rf.Role)
		// Outdated reply should be dropped.
		if args.Term == rf.CurrentTerm && rf.Role == 1 {
			if reply.VoteGranted {
				// fmt.Printf("[%v] Received vote from %v.\n", rf.me, server)
				rf.NumReceivedVotes += 1
				if rf.NumReceivedVotes > len(rf.peers)/2 {
					rf.Role = 2
					rf.NextIndex = make([]int, len(rf.peers))
					rf.MatchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.NextIndex[i] = rf.LastApplied + 1
						rf.MatchIndex[i] = 0
					}
					rf.persist()
					// fmt.Printf("[%v:t%v:%v] Becomes Leader. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)
					go rf.SendHeartBeat()
				}
			} else if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.Role = 0
				rf.ResetElectionTimer()
				rf.persist()
			}
		}
		rf.mu.Unlock()
		// fmt.Printf("[%v:t%v:%v] Unlocked in AskForOneVote Phase 2.\n", rf.me, rf.CurrentTerm, rf.Role)

		// // fmt.Printf("[%v] AskForOneVote: Unlocked!\n", rf.me)

	}
}

func (rf *Raft) StartElection() {

	rf.mu.Lock()
	// fmt.Printf("[%v:t%v:%v] Locked in StartElection.\n", rf.me, rf.CurrentTerm, rf.Role)
	defer rf.mu.Unlock()
	// defer fmt.Printf("[%v:t%v:%v] Unlocked in StartElection.\n", rf.me, rf.CurrentTerm, rf.Role)

	rf.Role = 1
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.NumReceivedVotes = 1
	rf.persist()

	// fmt.Printf("[%v:t%v:%v] Starts an election!\n", rf.me, rf.CurrentTerm, rf.Role)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go rf.AskForOneVote(peer)

	}
}

func (rf *Raft) SendOneHeartBeat(server int, term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int) {
	// // fmt.Printf("[%v] Enter SendOneHeartBeat! Wait for unlock...\n", rf.me)
	// rf.mu.Lock()
	// // fmt.Printf("[%v] Unlocked!\n", rf.me)

	// fmt.Printf("[%v:t%v:%v] Sends One Heartbeat to peer %v!\n", leaderId, term, 2, server)

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	// rf.mu.Unlock()

	rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

}

func (rf *Raft) SendHeartBeat() {
	// // fmt.Printf("[%v] Sends Heartbeat to all peers!\n", rf.me)
	rf.mu.Lock()
	// fmt.Printf("[%v:t%v:%v] Locked in SendHeartBeat.\n", rf.me, rf.CurrentTerm, rf.Role)

	defer rf.mu.Unlock()
	// defer fmt.Printf("[%v:t%v:%v] Unlocked in SendHeartBeat.\n", rf.me, rf.CurrentTerm, rf.Role)

	lastLogIndex := len(rf.Log) - 1 + rf.SnapShotOffset

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// fmt.Printf("[%v:t%v:%v] lastLogIndex %v, Peer %v NextIndex %v!\n", rf.me, rf.CurrentTerm, rf.Role, lastLogIndex, peer, rf.NextIndex[peer])
		if rf.NextIndex[peer] <= lastLogIndex {
			if rf.NextIndex[peer] <= rf.SnapShotOffset {
				go rf.SendInstallSnapshot(peer, rf.persister.ReadSnapshot(), rf.CurrentTerm, rf.me, rf.SnapShotOffset, rf.Log[0].Term)
			} else {
				entries := make([]LogEntry, len(rf.Log)+rf.SnapShotOffset-rf.NextIndex[peer])
				copy(entries, rf.Log[(rf.NextIndex[peer]-rf.SnapShotOffset):])
				// fmt.Printf("[%v:t%v:%v] %v, %v, %v\n", rf.me, rf.CurrentTerm, rf.Role, len(rf.Log), rf.NextIndex[peer]-1, rf.NextIndex)
				go rf.SendLogs(peer, entries, rf.CurrentTerm, rf.me, rf.NextIndex[peer]-1, rf.Log[rf.NextIndex[peer]-rf.SnapShotOffset-1].Term, rf.CommitIndex)
				// fmt.Printf("[%v:t%v:%v] Send Logs to Peer %v, lastid: %v, snapShotOffset: %v, Entries %v, rf.Log:%v!\n", rf.me, rf.CurrentTerm, rf.Role, peer, lastLogIndex, rf.SnapShotOffset, entries, rf.Log)

			}
		} else {
			entries := make([]LogEntry, 0)
			go rf.SendLogs(peer, entries, rf.CurrentTerm, rf.me, lastLogIndex, rf.Log[lastLogIndex-rf.SnapShotOffset].Term, rf.CommitIndex)
			// fmt.Printf("[%v:t%v:%v] Send Heartbeat to Peer %v, lastid: %v, lastterm %v!\n", rf.me, rf.CurrentTerm, rf.Role, peer, lastLogIndex, rf.Log[lastLogIndex].Term)

		}

	}

}

func (rf *Raft) SendInstallSnapshot(server int, data []byte, term int, leaderId int, lastIncludedIndex int, lastIncludedTerm int) {

	// args := InstallSnapshotArgs{
	// 	Term:              rf.CurrentTerm,
	// 	LeaderId:          rf.me,
	// 	LastIncludedIndex: rf.SnapShotOffset,
	// 	LastIncludedTerm:  rf.Log[0].Term,
	// 	Data:              rf.persister.ReadSnapshot(),
	// }

	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          leaderId,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	reply := InstallSnapshotReply{}

	success := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !success {
		return
	} else {
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.Role = 0
			rf.persist()
			return
		} else {
			rf.MatchIndex[server] = args.LastIncludedIndex
			rf.NextIndex[server] = args.LastIncludedIndex + 1
			// fmt.Printf("[%v:t%v:%v] Update server %v nextIndex to %v by snapshot. \n", rf.me, rf.CurrentTerm, rf.Role, server, rf.NextIndex[server])
		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Reset ElectionTimer
	rf.ResetElectionTimer()

	if len(args.Entries) > 0 {
		// fmt.Printf("[%v:t%v:%v] Received AppendEntries request: args %v. \n", rf.me, rf.CurrentTerm, rf.Role, args)
	} else {
		// fmt.Printf("[%v:t%v:%v] Received Heartbeat: args %v. \n", rf.me, rf.CurrentTerm, rf.Role, args)

	}

	reply.ExpectedIndex = -2

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		// fmt.Printf("[%v:t%v:%v] Reply to leader %v: %v. \n", rf.me, rf.CurrentTerm, rf.Role, args.LeaderId, reply)

		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		// rf.VotedFor = -1
	}

	// Change its role to follower when receiving a valid AppendEntries request.
	rf.Role = 0

	followerLastLogIndex := len(rf.Log) + rf.SnapShotOffset - 1
	// fmt.Printf("[%v:t%v:%v] followerLastLogIndex=%v, args.PrevLogIndex=%v\n", rf.me, rf.CurrentTerm, rf.Role, followerLastLogIndex, args.PrevLogIndex)

	// // fmt.Printf("[%v:t%v] ")

	// fmt.Printf("[%v:t%v:%v] PrevLogIndex=%v. SnapShotOffset=%v. Current Log: %v \n", rf.me, rf.CurrentTerm, rf.Role, args.PrevLogIndex, rf.SnapShotOffset, rf.Log)

	if followerLastLogIndex < args.PrevLogIndex || args.PrevLogIndex < rf.SnapShotOffset || rf.Log[args.PrevLogIndex-rf.SnapShotOffset].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.ExpectedIndex = rf.CommitIndex + 1
		// fmt.Printf("[%v:t%v:%v] Expected %v. Log %v. \n", rf.me, rf.CurrentTerm, rf.Role, rf.CommitIndex+1, rf.Log)

		return
	}

	for id, entry := range args.Entries {
		if followerLastLogIndex+1 > args.PrevLogIndex+id+1 && rf.Log[args.PrevLogIndex-rf.SnapShotOffset+id+1].Term != entry.Term {
			// fmt.Printf("[%v:t%v:%v] Before slice: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)

			rf.Log = rf.Log[:args.PrevLogIndex-rf.SnapShotOffset+id+1]
			followerLastLogIndex = len(rf.Log) - 1 + rf.SnapShotOffset

			// fmt.Printf("[%v:t%v:%v] After slice: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)

		}
		if followerLastLogIndex < args.PrevLogIndex+id+1 {
			rf.Log = append(rf.Log, entry)
			// fmt.Printf("[%v:t%v:%v] Appended Log id=%v to state machine!\n", rf.me, rf.CurrentTerm, rf.Role, args.PrevLogIndex+id+1)
		}
		followerLastLogIndex = len(rf.Log) - 1 + rf.SnapShotOffset

	}

	// fmt.Printf("[%v:t%v:%v] Current Log: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)

	if args.LeaderCommit > rf.CommitIndex {
		// fmt.Printf("[%v:t%v:%v] LeaderCommit=%v, len(rf.Log)=%v\n", rf.me, rf.CurrentTerm, rf.Role, args.LeaderCommit, len(rf.Log))
		if args.LeaderCommit < len(rf.Log)-1 {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = len(rf.Log) - 1 + rf.SnapShotOffset
		}

		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			// fmt.Printf("[%v:t%v:%v] rf.SnapShotOffset=%v. Lastapplied=%v. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.SnapShotOffset, rf.LastApplied, rf.Log)

			if rf.LastApplied < rf.SnapShotOffset {
				// message := ApplyMsg{
				// 	CommandValid: false,
				// 	CommandIndex: i,
				// }
				// fmt.Printf("[%v:t%v:%v] Follower Trying to apply Log %v in snapshot. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, i, rf.Log)
				// rf.applyCh <- message
				// fmt.Printf("[%v:t%v:%v] Applied Log %v. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, i, rf.Log)
			} else {
				message := ApplyMsg{
					CommandValid: true,
					Command:      rf.Log[i-rf.SnapShotOffset].Command,
					CommandIndex: i,
				}
				// fmt.Printf("[%v:t%v:%v] Follower Trying to apply Log %v command=%v. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, i, rf.Log[i-rf.SnapShotOffset].Command, rf.Log)
				rf.applyCh <- message
				// fmt.Printf("[%v:t%v:%v] Applied Log %v command=%v. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, i, rf.Log[i-rf.SnapShotOffset].Command, rf.Log)
			}

		}
		rf.LastApplied = rf.CommitIndex

	}

	reply.Success = true
	reply.Term = rf.CurrentTerm
	// fmt.Printf("[%v:t%v:%v] Reply to leader %v: %v. \n", rf.me, rf.CurrentTerm, rf.Role, args.LeaderId, reply)
	return
}

func (rf *Raft) SendLogs(server int, entries []LogEntry, term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int) {
	// // fmt.Printf("[%v] Enter SendOneHeartBeat! Wait for unlock...\n", rf.me)
	// rf.mu.Lock()
	// // fmt.Printf("[%v] Unlocked!\n", rf.me)

	// entries := append(make([]LogEntry, 0), entry)

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	// rf.mu.Unlock()
	// // fmt.Printf("[%v] Sends Heartbeat to %v!\n", rf.me, server)

	success := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	// fmt.Printf("[%v:t%v:%v] Locked in SendLogs.\n", rf.me, rf.CurrentTerm, rf.Role)

	defer rf.mu.Unlock()
	// defer fmt.Printf("[%v:t%v:%v] Unlocked in SendLogs.\n", rf.me, rf.CurrentTerm, rf.Role)

	defer rf.persist()

	if !success {
		return
	}

	// fmt.Printf("[%v:t%v:%v] Received reply from %v: %v. args: %v \n", rf.me, rf.CurrentTerm, rf.Role, server, reply, args)

	if reply.Success {
		rf.NextIndex[server] = prevLogIndex + 1 + len(entries)
		// fmt.Printf("[%v:t%v:%v] Update server %v nextIndex to %v. \n", rf.me, rf.CurrentTerm, rf.Role, server, rf.NextIndex[server])
		rf.MatchIndex[server] = rf.NextIndex[server] - 1
	} else {
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.Role = 0
			// fmt.Printf("[%v:t%v:%v] Convert to follower because reply.Term > currentTerm\n", rf.me, rf.CurrentTerm, rf.Role)
			rf.ResetElectionTimer()
			return
		} else if reply.ExpectedIndex > 0 {
			// fmt.Printf("[%v:t%v:%v] Server %v expects log %v.\n", rf.me, rf.CurrentTerm, rf.Role, server, reply.ExpectedIndex)
			rf.NextIndex[server] = reply.ExpectedIndex
			rf.MatchIndex[server] = reply.ExpectedIndex - 1
		} else {
			// fmt.Printf("[%v:t%v:%v] Server %v does not say its expected log. Current is %v.\n", rf.me, rf.CurrentTerm, rf.Role, server, prevLogIndex)
			rf.NextIndex[server] = prevLogIndex - 1
			if rf.NextIndex[server] <= 0 {
				rf.NextIndex[server] = 1
			}
		}

	}

	for i := len(rf.Log) + rf.SnapShotOffset - 1; i > rf.CommitIndex; i-- {
		if rf.Log[i-rf.SnapShotOffset].Term == rf.CurrentTerm {
			num_applied := 1
			for j := range rf.peers {
				if rf.MatchIndex[j] >= i {
					num_applied++
				}
			}
			if num_applied > len(rf.peers)/2 {
				rf.CommitIndex = i
				// fmt.Printf("[%v:t%v:%v] Committed Log %v. %v. Current LastApplied: %v \n", rf.me, rf.CurrentTerm, rf.Role, rf.CommitIndex, rf.Log, rf.LastApplied)
				for j := rf.LastApplied + 1; j <= rf.CommitIndex; j++ {
					message := ApplyMsg{
						CommandValid: true,
						Command:      rf.Log[j-rf.SnapShotOffset].Command,
						CommandIndex: j,
					}
					// fmt.Printf("[%v:t%v:%v] Trying Applying Log %v command=%v. %v\n", rf.me, rf.CurrentTerm, rf.Role, j, rf.Log[j-rf.SnapShotOffset].Command, time.Now())
					time.Sleep(10 * time.Millisecond)

					rf.applyCh <- message
					// fmt.Printf("[%v:t%v:%v] Applied Log %v command=%v. %v\n", rf.me, rf.CurrentTerm, rf.Role, j, rf.Log[j-rf.SnapShotOffset].Command, time.Now())

				}
				// fmt.Printf("[%v:t%v:%v] Finished applying Log until %v. \n", rf.me, rf.CurrentTerm, rf.Role, rf.CommitIndex)

				rf.LastApplied = rf.CommitIndex

				// message := ApplyMsg{
				// 	CommandValid: true,
				// 	Command:      rf.Log[i].Command,
				// 	CommandIndex: i,
				// }
				// rf.applyCh <- message
				break
			}
		}
	}
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	// fmt.Printf("[%v:t%v:%v] Received command %v. Trying to get LOCK...\n", rf.me, rf.CurrentTerm, rf.Role, command)
	rf.mu.Lock()
	// fmt.Printf("[%v:t%v:%v] Locked in Start %v.\n", rf.me, rf.CurrentTerm, rf.Role, command)

	defer rf.mu.Unlock()
	// defer fmt.Printf("[%v:t%v:%v] Unlocked in Start %v.\n", rf.me, rf.CurrentTerm, rf.Role, command)

	// fmt.Printf("[%v:t%v:%v] Triggerd Start() with command %v.\n", rf.me, rf.CurrentTerm, rf.Role, command)

	if rf.Role == 2 {
		term = rf.CurrentTerm

		// lastLogIndex := len(rf.Log) - 1
		// lastLogTerm := rf.Log[len(rf.Log)-1].Term

		newLogEntry := LogEntry{
			Command: command,
			Term:    rf.CurrentTerm,
		}
		rf.Log = append(rf.Log, newLogEntry)

		index = len(rf.Log) + rf.SnapShotOffset - 1
		rf.HeartBeatTimer.Reset(1 * time.Millisecond)

		// fmt.Printf("[%v:t%v:%v] lastLogIndex %v, lastLogTerm %v.\n", rf.me, rf.CurrentTerm, rf.Role, lastLogIndex, lastLogTerm)

		// for peer := range rf.peers {
		// 	if peer == rf.me {
		// 		continue
		// 	} else {
		// 		go rf.SendOneLog(peer, newLogEntry, rf.CurrentTerm, rf.me, lastLogIndex, lastLogTerm, rf.CommitIndex)
		// 	}
		// }
		// fmt.Printf("[%v:t%v:%v] Leader applied.\n", rf.me, rf.CurrentTerm, rf.Role)

	} else {
		// fmt.Printf("[%v:t%v:%v] Not a leader.\n", rf.me, rf.CurrentTerm, rf.Role)
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		<-rf.ElectionTimer.C
		rf.mu.Lock()
		// fmt.Printf("[%v:t%v:%v] Locked in ElectionTick.\n", rf.me, rf.CurrentTerm, rf.Role)
		currentRole := rf.Role
		rf.ResetElectionTimer()
		rf.mu.Unlock()
		// fmt.Printf("[%v:t%v:%v] Unlocked in ElectionTick.\n", rf.me, rf.CurrentTerm, rf.Role)

		// // fmt.Printf("[%v: %v] ElectionTimer Ticked! \n", rf.me, currentRole)

		if currentRole == 2 {
			continue
		}

		rf.StartElection()
	}
}

func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		<-rf.HeartBeatTimer.C

		rf.mu.Lock()
		// fmt.Printf("[%v:t%v:%v] Locked in heartBeatTicker.\n", rf.me, rf.CurrentTerm, rf.Role)

		currentRole := rf.Role
		rf.ResetHeartBeatTimer()
		rf.mu.Unlock()
		// fmt.Printf("[%v:t%v:%v] Unlocked in heartBeatTicker.\n", rf.me, rf.CurrentTerm, rf.Role)

		// // fmt.Printf("[%v:%v] Heartbeat Ticked!\n", rf.me, currentRole)

		if currentRole == 2 {
			rf.SendHeartBeat()
		}

	}
}

func (rf *Raft) GetPersisterStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.ElectionTimer = time.NewTimer(time.Duration(rand.Intn(1000)+300) * time.Millisecond)
	rf.HeartBeatTimer = time.NewTimer(time.Duration(200 * time.Millisecond))

	dummyLog := LogEntry{
		Term:    0,
		Command: 10000000,
	}
	rf.Log = append(rf.Log, dummyLog)

	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.CommitIndex = 0
	rf.Role = 0
	rf.SnapShotOffset = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	msg := ApplyMsg{
		CommandValid: true,
		Command:      10000000,
		CommandIndex: 0,
	}

	rf.applyCh <- msg

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeatTicker()

	return rf
}
