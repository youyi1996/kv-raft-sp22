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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	// // fmt.Printf("----[%v] Enter GetState().\n", rf.me)

	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = (rf.Role == 2)
	rf.mu.Unlock()
	// // fmt.Printf("----[%v] GetState(), %v, %v.\n", rf.me, term, isleader)

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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	// fmt.Printf("[%v:t%v:%v] Received RequestVote from %v. %v\n", rf.me, rf.CurrentTerm, rf.Role, args.CandidateId, args)

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.Role = 0
		rf.ResetElectionTimer()
	}

	if args.Term < rf.CurrentTerm {
		// // fmt.Printf("[%v, t%v] No vote for %v due to outdated term.\n", rf.me, rf.CurrentTerm, args.CandidateId)

		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.VotedFor != -1 && args.CandidateId != rf.VotedFor {
		// // fmt.Printf("[%v, t%v] No vote for %v due to already voted for %v.\n", rf.me, rf.CurrentTerm, args.CandidateId, rf.VotedFor)

		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	lastLogTerm := rf.Log[len(rf.Log)-1].Term
	lastLogIndex := len(rf.Log) - 1

	if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		// fmt.Printf("[%v:t%v:%v] Voted for %v. %v, [%v:%v]. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, args.CandidateId, args, lastLogTerm, lastLogIndex, rf.Log)

		rf.Role = 0

		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm

	} else {
		// // fmt.Printf("[%v, t%v] No vote for %v due to outdated logs.\n", rf.me, rf.CurrentTerm, args.CandidateId)

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
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Log) - 1,
		LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
	}
	rf.mu.Unlock()

	reply := RequestVoteReply{}

	// fmt.Printf("[%v:t%v:%v] Sends vote request to %v. %v\n", rf.me, rf.CurrentTerm, rf.Role, server, args)

	if rf.sendRequestVote(server, &args, &reply) {
		rf.mu.Lock()
		// Outdated reply should be dropped.
		if args.Term == rf.CurrentTerm && rf.Role == 1 {
			if reply.VoteGranted {
				// // fmt.Printf("[%v] Received vote from %v.\n", rf.me, server)
				rf.NumReceivedVotes += 1
				if rf.NumReceivedVotes > len(rf.peers)/2 {
					rf.Role = 2
					rf.NextIndex = make([]int, len(rf.peers))
					rf.MatchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.NextIndex[i] = rf.LastApplied + 1
						rf.MatchIndex[i] = 0
					}
					// fmt.Printf("[%v:t%v:%v] Becomes Leader. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)
					go rf.SendHeartBeat()
				}
			}
		}
		rf.mu.Unlock()
		// // fmt.Printf("[%v] AskForOneVote: Unlocked!\n", rf.me)

	}
}

func (rf *Raft) StartElection() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Role = 1
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.NumReceivedVotes = 1

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
	defer rf.mu.Unlock()

	lastLogIndex := len(rf.Log) - 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// fmt.Printf("[%v:t%v:%v] lastLogIndex %v, Peer %v NextIndex %v!\n", rf.me, rf.CurrentTerm, rf.Role, lastLogIndex, peer, rf.NextIndex[peer])
		if rf.NextIndex[peer] <= lastLogIndex {
			entries := make([]LogEntry, len(rf.Log)-rf.NextIndex[peer])
			copy(entries, rf.Log[rf.NextIndex[peer]:])
			go rf.SendLogs(peer, entries, rf.CurrentTerm, rf.me, rf.NextIndex[peer]-1, rf.Log[rf.NextIndex[peer]-1].Term, rf.CommitIndex)
			// fmt.Printf("[%v:t%v:%v] Send Heartbeat to Peer %v, lastid: %v, lastterm %v!\n", rf.me, rf.CurrentTerm, rf.Role, peer, lastLogIndex, rf.Log[lastLogIndex].Term)
		} else {
			entries := make([]LogEntry, 0)
			go rf.SendLogs(peer, entries, rf.CurrentTerm, rf.me, lastLogIndex, rf.Log[lastLogIndex].Term, rf.CommitIndex)
			// fmt.Printf("[%v:t%v:%v] Send Heartbeat to Peer %v, lastid: %v, lastterm %v!\n", rf.me, rf.CurrentTerm, rf.Role, peer, lastLogIndex, rf.Log[lastLogIndex].Term)

		}

	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reset ElectionTimer
	rf.ResetElectionTimer()

	if len(args.Entries) > 0 {
		// fmt.Printf("[%v:t%v:%v] Received AppendEntries request: args %v. \n", rf.me, rf.CurrentTerm, rf.Role, args)
	} else {
		// fmt.Printf("[%v:t%v:%v] Received Heartbeat: args %v. \n", rf.me, rf.CurrentTerm, rf.Role, args)

	}

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

	followerLastLogIndex := len(rf.Log) - 1
	// fmt.Printf("[%v:t%v:%v] followerLastLogIndex=%v, args.PrevLogIndex=%v\n", rf.me, rf.CurrentTerm, rf.Role, followerLastLogIndex, args.PrevLogIndex)

	// // fmt.Printf("[%v:t%v] ")

	if followerLastLogIndex < args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		// reply.ExpectedIndex = rf.CommitIndex
		// fmt.Printf("[%v:t%v:%v] Reply to leader %v: %v. \n", rf.me, rf.CurrentTerm, rf.Role, args.LeaderId, reply)

		return
	}

	for id, entry := range args.Entries {
		if followerLastLogIndex+1 > args.PrevLogIndex+id+1 && rf.Log[args.PrevLogIndex+id+1].Term != entry.Term {
			// fmt.Printf("[%v:t%v:%v] Before slice: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)

			rf.Log = rf.Log[:args.PrevLogIndex+id+1]
			followerLastLogIndex = len(rf.Log) - 1

			// fmt.Printf("[%v:t%v:%v] After slice: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)

		}
		if followerLastLogIndex < args.PrevLogIndex+id+1 {
			rf.Log = append(rf.Log, entry)
			// fmt.Printf("[%v:t%v:%v] Appended Log id=%v to state machine!\n", rf.me, rf.CurrentTerm, rf.Role, args.PrevLogIndex+id+1)
		}
		followerLastLogIndex = len(rf.Log) - 1

	}

	// fmt.Printf("[%v:t%v:%v] Current Log: %v\n", rf.me, rf.CurrentTerm, rf.Role, rf.Log)

	if args.LeaderCommit > rf.CommitIndex {
		// fmt.Printf("[%v:t%v:%v] LeaderCommit=%v, len(rf.Log)=%v\n", rf.me, rf.CurrentTerm, rf.Role, args.LeaderCommit, len(rf.Log))
		if args.LeaderCommit < len(rf.Log)-1 {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = len(rf.Log) - 1
		}

		for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
			message := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- message
			// fmt.Printf("[%v:t%v:%v] Applied Log %v command=%v. Logs: %v\n", rf.me, rf.CurrentTerm, rf.Role, i, rf.Log[i].Command, rf.Log)

		}
		rf.LastApplied = rf.CommitIndex

	}

	reply.Success = true
	reply.Term = rf.CurrentTerm
	// fmt.Printf("[%v:t%v:%v] Reply to leader %v: %v. \n", rf.me, rf.CurrentTerm, rf.Role, args.LeaderId, reply)

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
	defer rf.mu.Unlock()

	if !success {
		return
	}

	// fmt.Printf("[%v:t%v:%v] Received reply from %v: %v. args: %v \n", rf.me, rf.CurrentTerm, rf.Role, server, reply, args)

	if reply.Success {
		rf.NextIndex[server] = prevLogIndex + 1 + len(entries)
		rf.MatchIndex[server] = rf.NextIndex[server] - 1
	} else {
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.Role = 0
			// fmt.Printf("[%v:t%v:%v] Convert to follower because reply.Term > currentTerm\n", rf.me, rf.CurrentTerm, rf.Role)
			rf.ResetElectionTimer()
			return
		} else if reply.ExpectedIndex > 0 {
			rf.NextIndex[server] = reply.ExpectedIndex
		} else {
			rf.NextIndex[server] = prevLogIndex - 1
		}

	}

	for i := len(rf.Log) - 1; i > rf.CommitIndex; i-- {
		if rf.Log[i].Term == rf.CurrentTerm {
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
						Command:      rf.Log[j].Command,
						CommandIndex: j,
					}
					rf.applyCh <- message
					// fmt.Printf("[%v:t%v:%v] Applied Log %v command=%v. \n", rf.me, rf.CurrentTerm, rf.Role, j, rf.Log[j].Command)

				}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

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

		index = len(rf.Log) - 1

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
		currentRole := rf.Role
		rf.ResetElectionTimer()
		rf.mu.Unlock()

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
		currentRole := rf.Role
		rf.ResetHeartBeatTimer()
		rf.mu.Unlock()

		// // fmt.Printf("[%v:%v] Heartbeat Ticked!\n", rf.me, currentRole)

		if currentRole == 2 {
			rf.SendHeartBeat()
		}

	}
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
		Command: nil,
	}
	rf.Log = append(rf.Log, dummyLog)
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.CommitIndex = 0
	rf.Role = 0
	rf.applyCh = applyCh

	msg := ApplyMsg{
		CommandValid: true,
		Command:      rf.Log[0].Command,
		CommandIndex: 0,
	}

	rf.applyCh <- msg

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeatTicker()

	return rf
}
