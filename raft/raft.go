package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shlok1806/raft-kv/config"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// ApplyMsg is what Raft pushes onto applyCh once an entry is committed.
// For snapshots, Snapshot is set and Command is nil.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Raft is a single node in the consensus group. Exported methods are safe
// to call from multiple goroutines.
type Raft struct {
	mu   sync.Mutex
	me   int // this node's index in cfg.Peers
	cfg  *config.ClusterConfig
	dead int32 // set by Kill(); checked by killed()

	persister Persister
	applyCh   chan ApplyMsg

	// network — used to send RPCs to peers
	sendRPC func(peer int, method string, args, reply interface{}) bool

	// persistent state (written to disk before any reply)
	currentTerm int
	votedFor    int // -1 = nobody
	log         *raftLog

	// volatile
	commitIndex int
	lastApplied int
	state       NodeState

	// leader only — reinitialized on each election win
	nextIndex  []int
	matchIndex []int

	// channel that wakes up the applier goroutine
	applyNotify chan struct{}

	// election timer reset signal
	resetTimer chan struct{}
}

// Make creates a new Raft node and starts its background goroutines.
// peers is the list of all peer addresses (including this node).
func Make(
	me int,
	cfg *config.ClusterConfig,
	persister Persister,
	applyCh chan ApplyMsg,
	sendRPC func(peer int, method string, args, reply interface{}) bool,
) *Raft {
	r := &Raft{
		me:          me,
		cfg:         cfg,
		persister:   persister,
		applyCh:     applyCh,
		sendRPC:     sendRPC,
		votedFor:    -1,
		log:         newRaftLog(),
		state:       Follower,
		applyNotify: make(chan struct{}, 1),
		resetTimer:  make(chan struct{}, 1),
	}

	r.nextIndex = make([]int, len(cfg.Peers))
	r.matchIndex = make([]int, len(cfg.Peers))

	// restore crash state if any
	r.readPersist(persister.ReadRaftState())
	snap := persister.ReadSnapshot()
	if len(snap) > 0 {
		r.lastApplied = r.log.snapshotIndex
		r.commitIndex = r.log.snapshotIndex
	}

	go r.electionLoop()
	go r.applyLoop()

	return r
}

func (r *Raft) killed() bool {
	return atomic.LoadInt32(&r.dead) == 1
}

func (r *Raft) Kill() {
	atomic.StoreInt32(&r.dead, 1)
}

// GetState returns (currentTerm, isLeader). Used by tests.
func (r *Raft) GetState() (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.state == Leader
}

// Start proposes a command. Returns (logIndex, term, isLeader).
// If not leader, returns false and callers should redirect.
func (r *Raft) Start(command interface{}) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, false
	}

	idx := r.log.lastIndex() + 1
	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   idx,
		Command: command,
	}
	r.log.append(entry)
	r.persist()

	// kick off replication immediately instead of waiting for the heartbeat tick
	go r.broadcastAppendEntries()

	return idx, r.currentTerm, true
}

// -----------------------------------------------------------------------
// Election loop
// -----------------------------------------------------------------------

func (r *Raft) electionLoop() {
	for !r.killed() {
		timeout := r.randomTimeout()

		select {
		case <-time.After(timeout):
			r.mu.Lock()
			if r.state != Leader {
				r.mu.Unlock()
				r.startElection()
			} else {
				r.mu.Unlock()
			}

		case <-r.resetTimer:
			// timer was reset by an incoming RPC — loop back and re-arm
		}
	}
}

func (r *Raft) randomTimeout() time.Duration {
	min := r.cfg.ElectionTimeoutMin
	max := r.cfg.ElectionTimeoutMax
	spread := int64(max - min)
	return min + time.Duration(rand.Int63n(spread))
}

func (r *Raft) tickReset() {
	// non-blocking send — if there's already a pending reset that's fine
	select {
	case r.resetTimer <- struct{}{}:
	default:
	}
}

// -----------------------------------------------------------------------
// Leader election
// -----------------------------------------------------------------------

func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.me
	r.persist()

	term := r.currentTerm
	lastIdx := r.log.lastIndex()
	lastTerm := r.log.lastTerm()
	r.mu.Unlock()

	votes := int32(1) // already voted for ourselves
	quorum := int32(len(r.cfg.Peers)/2 + 1)
	won := int32(0)

	for i := range r.cfg.Peers {
		if i == r.me {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  r.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}

			if !r.sendRPC(peer, "Raft.RequestVote", args, reply) {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				return
			}

			// stale reply
			if r.state != Candidate || r.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				total := atomic.AddInt32(&votes, 1)
				if total >= quorum && atomic.CompareAndSwapInt32(&won, 0, 1) {
					r.becomeLeader()
				}
			}
		}(i)
	}
}

func (r *Raft) becomeFollower(term int) {
	// caller must hold r.mu
	r.state = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.persist()
}

func (r *Raft) becomeLeader() {
	// caller must hold r.mu
	r.state = Leader

	// initialize per-peer bookkeeping
	nextIdx := r.log.lastIndex() + 1
	for i := range r.cfg.Peers {
		r.nextIndex[i] = nextIdx
		r.matchIndex[i] = 0
	}

	// send immediate heartbeats then start the periodic ticker
	go r.broadcastAppendEntries()
	go r.heartbeatLoop()
}

// -----------------------------------------------------------------------
// RequestVote RPC handler
// -----------------------------------------------------------------------

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.VoteGranted = false

	if args.Term < r.currentTerm {
		return nil
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
		reply.Term = r.currentTerm
	}

	alreadyVoted := r.votedFor != -1 && r.votedFor != args.CandidateId
	if alreadyVoted {
		return nil
	}

	// candidate's log must be at least as up-to-date as ours
	lastTerm := r.log.lastTerm()
	lastIdx := r.log.lastIndex()
	candidateUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if !candidateUpToDate {
		return nil
	}

	r.votedFor = args.CandidateId
	r.persist()
	reply.VoteGranted = true
	r.tickReset()
	return nil
}

// -----------------------------------------------------------------------
// Heartbeat / AppendEntries loop
// -----------------------------------------------------------------------

func (r *Raft) heartbeatLoop() {
	for !r.killed() {
		time.Sleep(r.cfg.HeartbeatInterval)

		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		go r.broadcastAppendEntries()
	}
}

func (r *Raft) broadcastAppendEntries() {
	for i := range r.cfg.Peers {
		if i == r.me {
			continue
		}
		go r.sendAppendEntries(i)
	}
}

func (r *Raft) sendAppendEntries(peer int) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	ni := r.nextIndex[peer]
	prevIdx := ni - 1
	prevTerm := 0
	if prevIdx >= r.log.snapshotIndex {
		prevTerm = r.log.termAt(prevIdx)
	} else {
		// follower is behind our snapshot — send InstallSnapshot instead
		r.mu.Unlock()
		r.sendSnapshot(peer)
		return
	}

	var entries []LogEntry
	if r.log.lastIndex() >= ni {
		entries = r.log.slice(ni, r.log.lastIndex()+1)
		// copy so we can release the lock
		cp := make([]LogEntry, len(entries))
		copy(cp, entries)
		entries = cp
	}

	args := &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderId:     r.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	term := r.currentTerm
	r.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !r.sendRPC(peer, "Raft.AppendEntries", args, reply) {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return
	}

	if r.state != Leader || r.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := prevIdx + len(args.Entries)
		if newMatch > r.matchIndex[peer] {
			r.matchIndex[peer] = newMatch
		}
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.maybeAdvanceCommit()
	} else {
		// fast backup
		if reply.ConflictTerm == -1 {
			r.nextIndex[peer] = reply.ConflictIndex
		} else {
			// find the last entry we have for ConflictTerm
			newNext := reply.ConflictIndex
			for i := r.log.lastIndex(); i > r.log.snapshotIndex; i-- {
				if r.log.termAt(i) == reply.ConflictTerm {
					newNext = i + 1
					break
				}
			}
			r.nextIndex[peer] = newNext
		}
		if r.nextIndex[peer] < 1 {
			r.nextIndex[peer] = 1
		}
	}
}

// -----------------------------------------------------------------------
// AppendEntries RPC handler
// -----------------------------------------------------------------------

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	if args.Term < r.currentTerm {
		return nil
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
		reply.Term = r.currentTerm
	} else if r.state == Candidate {
		// another node won the election for this term
		r.state = Follower
	}

	r.tickReset()

	// consistency check
	if args.PrevLogIndex > r.log.lastIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = r.log.lastIndex() + 1
		return nil
	}

	if args.PrevLogIndex > r.log.snapshotIndex {
		if r.log.termAt(args.PrevLogIndex) != args.PrevLogTerm {
			ct := r.log.termAt(args.PrevLogIndex)
			reply.ConflictTerm = ct
			// find first index of that term
			ci := args.PrevLogIndex
			for ci > r.log.snapshotIndex && r.log.termAt(ci-1) == ct {
				ci--
			}
			reply.ConflictIndex = ci
			return nil
		}
	}

	// append new entries, truncating conflicting ones
	idx := args.PrevLogIndex
	for _, e := range args.Entries {
		idx++
		if idx <= r.log.lastIndex() {
			if r.log.termAt(idx) == e.Term {
				continue
			}
			r.log.truncateFrom(idx)
		}
		r.log.append(e)
	}
	r.persist()
	reply.Success = true

	// advance commit index
	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, r.log.lastIndex())
		r.notifyApply()
	}

	return nil
}

// -----------------------------------------------------------------------
// Commit advancement (leader only)
// -----------------------------------------------------------------------

func (r *Raft) maybeAdvanceCommit() {
	// find the highest N such that a majority has matchIndex >= N
	// and log[N].term == currentTerm (Figure 8 safety condition)
	for n := r.log.lastIndex(); n > r.commitIndex; n-- {
		if r.log.termAt(n) != r.currentTerm {
			continue
		}
		count := 1 // leader itself
		for i, m := range r.matchIndex {
			if i != r.me && m >= n {
				count++
			}
		}
		if count > len(r.cfg.Peers)/2 {
			r.commitIndex = n
			r.notifyApply()
			break
		}
	}
}

// -----------------------------------------------------------------------
// Apply loop — sends committed entries to the state machine
// -----------------------------------------------------------------------

func (r *Raft) notifyApply() {
	select {
	case r.applyNotify <- struct{}{}:
	default:
	}
}

func (r *Raft) applyLoop() {
	for !r.killed() {
		<-r.applyNotify

		r.mu.Lock()
		var msgs []ApplyMsg
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			e := r.log.at(r.lastApplied)
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			})
		}
		r.mu.Unlock()

		for _, msg := range msgs {
			r.applyCh <- msg
		}
	}
}

// -----------------------------------------------------------------------
// Snapshot
// -----------------------------------------------------------------------

// Snapshot is called by the KV layer when it wants to compact the log
// up to and including `index`. `data` is the encoded state machine state.
func (r *Raft) Snapshot(index int, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.log.snapshotIndex {
		return
	}

	term := r.log.termAt(index)
	r.log.compactThrough(index, term)
	r.persistWithSnapshot(data)
}

// CondInstallSnapshot is called by the KV layer after receiving a snapshot
// from Raft. Returns true if the install should proceed.
func (r *Raft) CondInstallSnapshot(lastIncludedTerm, lastIncludedIndex int, snapshot []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if lastIncludedIndex <= r.commitIndex {
		return false
	}

	r.log.compactThrough(lastIncludedIndex, lastIncludedTerm)
	r.commitIndex = lastIncludedIndex
	r.lastApplied = lastIncludedIndex
	r.persistWithSnapshot(snapshot)
	return true
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
