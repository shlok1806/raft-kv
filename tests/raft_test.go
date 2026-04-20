package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shlok1806/raft-kv/config"
	"github.com/shlok1806/raft-kv/raft"
)

// cluster is an in-process test harness. RPCs are dispatched as direct method
// calls so we don't need real sockets.
type cluster struct {
	t   *testing.T
	mu  sync.Mutex
	cfg *config.ClusterConfig

	nodes      []*raft.Raft
	persisters []*raft.MemPersister
	applied    [][]raft.ApplyMsg // applied[i] collects msgs from node i
	disconn    map[int]bool
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Peers = make([]string, n)
	for i := range cfg.Peers {
		cfg.Peers[i] = fmt.Sprintf("node:%d", i)
	}

	cl := &cluster{
		t:          t,
		cfg:        cfg,
		nodes:      make([]*raft.Raft, n),
		persisters: make([]*raft.MemPersister, n),
		applied:    make([][]raft.ApplyMsg, n),
		disconn:    make(map[int]bool),
	}
	for i := 0; i < n; i++ {
		cl.startNode(i)
	}
	return cl
}

func (cl *cluster) startNode(i int) {
	p := &raft.MemPersister{}
	cl.persisters[i] = p

	ch := make(chan raft.ApplyMsg, 256)
	me := i

	sendRPC := func(peer int, method string, args, reply interface{}) bool {
		cl.mu.Lock()
		down := cl.disconn[me] || cl.disconn[peer]
		cl.mu.Unlock()
		if down {
			return false
		}
		return cl.dispatch(peer, method, args, reply)
	}

	cl.nodes[i] = raft.Make(i, cl.cfg, p, ch, sendRPC)

	// one goroutine per node collects applied messages into cl.applied[i]
	go func() {
		for msg := range ch {
			cl.mu.Lock()
			cl.applied[me] = append(cl.applied[me], msg)
			cl.mu.Unlock()
		}
	}()
}

func (cl *cluster) dispatch(dst int, method string, args, reply interface{}) bool {
	cl.mu.Lock()
	node := cl.nodes[dst]
	cl.mu.Unlock()
	if node == nil {
		return false
	}
	switch method {
	case "Raft.RequestVote":
		return node.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply)) == nil
	case "Raft.AppendEntries":
		return node.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply)) == nil
	case "Raft.InstallSnapshot":
		return node.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply)) == nil
	}
	return false
}

func (cl *cluster) disconnect(i int) {
	cl.mu.Lock()
	cl.disconn[i] = true
	cl.mu.Unlock()
}

func (cl *cluster) reconnect(i int) {
	cl.mu.Lock()
	cl.disconn[i] = false
	cl.mu.Unlock()
}

func (cl *cluster) findLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cl.mu.Lock()
		for i, rf := range cl.nodes {
			if cl.disconn[i] {
				continue
			}
			_, ok := rf.GetState()
			if ok {
				cl.mu.Unlock()
				return i
			}
		}
		cl.mu.Unlock()
		time.Sleep(15 * time.Millisecond)
	}
	cl.t.Fatalf("no leader elected within %v", timeout)
	return -1
}

func (cl *cluster) leaderCount() int {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	n := 0
	for i, rf := range cl.nodes {
		if cl.disconn[i] {
			continue
		}
		_, ok := rf.GetState()
		if ok {
			n++
		}
	}
	return n
}

func (cl *cluster) submitToLeader(cmd interface{}) (int, int) {
	for {
		for _, rf := range cl.nodes {
			idx, term, ok := rf.Start(cmd)
			if ok {
				return idx, term
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// waitCommitted blocks until at least one node has applied the entry at idx
// and returns the committed command value.
func (cl *cluster) waitCommitted(idx int, timeout time.Duration) interface{} {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cl.mu.Lock()
		for _, msgs := range cl.applied {
			for _, m := range msgs {
				if m.CommandValid && m.CommandIndex == idx {
					cl.mu.Unlock()
					return m.Command
				}
			}
		}
		cl.mu.Unlock()
		time.Sleep(15 * time.Millisecond)
	}
	cl.t.Fatalf("index %d not committed within %v", idx, timeout)
	return nil
}

func (cl *cluster) cleanup() {
	for _, rf := range cl.nodes {
		if rf != nil {
			rf.Kill()
		}
	}
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

func TestInitialElection(t *testing.T) {
	cl := newCluster(t, 3)
	defer cl.cleanup()

	cl.findLeader(2 * time.Second)
	if n := cl.leaderCount(); n != 1 {
		t.Fatalf("want 1 leader, got %d", n)
	}
}

func TestReelection(t *testing.T) {
	cl := newCluster(t, 3)
	defer cl.cleanup()

	old := cl.findLeader(2 * time.Second)
	cl.disconnect(old)

	newL := cl.findLeader(2 * time.Second)
	if newL == old {
		t.Fatal("old leader re-elected after disconnect")
	}

	cl.reconnect(old)
	time.Sleep(300 * time.Millisecond)
	if n := cl.leaderCount(); n > 1 {
		t.Fatalf("two leaders after reconnect: %d", n)
	}
}

func TestBasicAgreement(t *testing.T) {
	cl := newCluster(t, 3)
	defer cl.cleanup()

	cl.findLeader(2 * time.Second)

	for i := 1; i <= 5; i++ {
		cmd := fmt.Sprintf("entry-%d", i)
		idx, _ := cl.submitToLeader(cmd)
		got := cl.waitCommitted(idx, 2*time.Second)
		if got != cmd {
			t.Fatalf("index %d: expected %q got %v", idx, cmd, got)
		}
	}
}

func TestNetworkPartition(t *testing.T) {
	cl := newCluster(t, 5)
	defer cl.cleanup()

	leader := cl.findLeader(2 * time.Second)
	minority := (leader + 1) % 5

	cl.disconnect(leader)
	cl.disconnect(minority)

	time.Sleep(600 * time.Millisecond)
	newL := cl.findLeader(2 * time.Second)
	if newL == leader {
		t.Fatal("isolated node still shows as leader")
	}

	// make sure the majority can commit
	idx, _ := cl.submitToLeader("after-partition")
	cl.waitCommitted(idx, 3*time.Second)

	cl.reconnect(leader)
	cl.reconnect(minority)
	time.Sleep(500 * time.Millisecond)
	if n := cl.leaderCount(); n != 1 {
		t.Fatalf("want 1 leader after heal, got %d", n)
	}
}

func TestTermMonotonicity(t *testing.T) {
	// 5-node cluster so we can kill 2 consecutive leaders and still have
	// a quorum of 3 for the third election
	cl := newCluster(t, 5)
	defer cl.cleanup()

	prevTerm := 0
	for i := 0; i < 3; i++ {
		l := cl.findLeader(2 * time.Second)
		term, _ := cl.nodes[l].GetState()
		if term < prevTerm {
			t.Fatalf("term decreased: %d -> %d", prevTerm, term)
		}
		prevTerm = term
		cl.disconnect(l)
		time.Sleep(200 * time.Millisecond)
	}
}
