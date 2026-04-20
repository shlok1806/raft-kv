package raft

// RequestVote RPC — sent by candidates during elections.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntries RPC — used for both heartbeats (empty Entries) and replication.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// fast backup: follower tells us the conflicting term and where it starts
	// so the leader can skip the whole term in one round-trip
	ConflictTerm  int // -1 if follower doesn't have PrevLogIndex at all
	ConflictIndex int
}

// InstallSnapshot RPC — sent when a follower is too far behind for AppendEntries.
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
