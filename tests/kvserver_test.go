package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shlok1806/raft-kv/config"
	"github.com/shlok1806/raft-kv/kvserver"
	"github.com/shlok1806/raft-kv/raft"
)

// kvCluster extends the basic raft cluster with KV servers layered on top.
// The KV servers own the applyCh goroutine so we rebuild the nodes here
// with fresh channels that the KV servers consume.
type kvCluster struct {
	t       *testing.T
	mu      sync.Mutex
	cfg     *config.ClusterConfig
	disconn map[int]bool
	nodes   []*raft.Raft
	kvs     []*kvserver.KVServer
}

func newKVCluster(t *testing.T, n int) *kvCluster {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.Peers = make([]string, n)
	for i := range cfg.Peers {
		cfg.Peers[i] = fmt.Sprintf("node:%d", i)
	}

	kvc := &kvCluster{
		t:       t,
		cfg:     cfg,
		disconn: make(map[int]bool),
		nodes:   make([]*raft.Raft, n),
		kvs:     make([]*kvserver.KVServer, n),
	}

	for i := 0; i < n; i++ {
		kvc.startNode(i)
	}
	return kvc
}

func (kvc *kvCluster) startNode(i int) {
	p := &raft.MemPersister{}
	ch := make(chan raft.ApplyMsg, 256)
	me := i

	sendRPC := func(peer int, method string, args, reply interface{}) bool {
		kvc.mu.Lock()
		down := kvc.disconn[me] || kvc.disconn[peer]
		kvc.mu.Unlock()
		if down {
			return false
		}
		return kvc.dispatch(peer, method, args, reply)
	}

	kvc.nodes[i] = raft.Make(i, kvc.cfg, p, ch, sendRPC)
	kvc.kvs[i] = kvserver.NewKVServer(i, kvc.nodes[i], ch, -1)
}

func (kvc *kvCluster) dispatch(dst int, method string, args, reply interface{}) bool {
	kvc.mu.Lock()
	node := kvc.nodes[dst]
	kvc.mu.Unlock()
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
	case "KVServer.Get":
		return kvc.kvs[dst].Get(args.(*kvserver.GetArgs), reply.(*kvserver.GetReply)) == nil
	case "KVServer.Put":
		return kvc.kvs[dst].Put(args.(*kvserver.PutArgs), reply.(*kvserver.PutReply)) == nil
	case "KVServer.Delete":
		return kvc.kvs[dst].Delete(args.(*kvserver.DeleteArgs), reply.(*kvserver.DeleteReply)) == nil
	}
	return false
}

func (kvc *kvCluster) disconnect(i int) {
	kvc.mu.Lock()
	kvc.disconn[i] = true
	kvc.mu.Unlock()
}

func (kvc *kvCluster) reconnect(i int) {
	kvc.mu.Lock()
	kvc.disconn[i] = false
	kvc.mu.Unlock()
}

func (kvc *kvCluster) findLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, rf := range kvc.nodes {
			_, ok := rf.GetState()
			if ok {
				return i
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	kvc.t.Fatalf("no leader elected within %v", timeout)
	return -1
}

func (kvc *kvCluster) cleanup() {
	for i := range kvc.nodes {
		if kvc.nodes[i] != nil {
			kvc.nodes[i].Kill()
			kvc.kvs[i].Kill()
		}
	}
}

// inProcessClient sends RPCs in-process through the dispatch table.
type inProcessClient struct {
	kvc      *kvCluster
	leaderId int
	clientId int64
	reqSeq   int64
}

func newInProcessClient(kvc *kvCluster) *inProcessClient {
	return &inProcessClient{kvc: kvc, clientId: time.Now().UnixNano()}
}

func (c *inProcessClient) nextReq() int64 {
	c.reqSeq++
	return c.reqSeq
}

func (c *inProcessClient) Get(key string) (string, error) {
	args := &kvserver.GetArgs{Key: key, ClientId: c.clientId, RequestId: c.nextReq()}
	for {
		reply := &kvserver.GetReply{}
		ok := c.kvc.dispatch(c.leaderId, "KVServer.Get", args, reply)
		if !ok || reply.Err == kvserver.ErrNotLeader || reply.Err == kvserver.ErrTimeout {
			c.leaderId = (c.leaderId + 1) % len(c.kvc.nodes)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == kvserver.ErrNoKey {
			return "", nil
		}
		return reply.Value, nil
	}
}

func (c *inProcessClient) Put(key, value string) error {
	args := &kvserver.PutArgs{Key: key, Value: value, ClientId: c.clientId, RequestId: c.nextReq()}
	for {
		reply := &kvserver.PutReply{}
		ok := c.kvc.dispatch(c.leaderId, "KVServer.Put", args, reply)
		if !ok || reply.Err == kvserver.ErrNotLeader || reply.Err == kvserver.ErrTimeout {
			c.leaderId = (c.leaderId + 1) % len(c.kvc.nodes)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return nil
	}
}

func (c *inProcessClient) Delete(key string) error {
	args := &kvserver.DeleteArgs{Key: key, ClientId: c.clientId, RequestId: c.nextReq()}
	for {
		reply := &kvserver.DeleteReply{}
		ok := c.kvc.dispatch(c.leaderId, "KVServer.Delete", args, reply)
		if !ok || reply.Err == kvserver.ErrNotLeader || reply.Err == kvserver.ErrTimeout {
			c.leaderId = (c.leaderId + 1) % len(c.kvc.nodes)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return nil
	}
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

func TestBasicKV(t *testing.T) {
	kvc := newKVCluster(t, 3)
	defer kvc.cleanup()

	kvc.findLeader(2 * time.Second)
	c := newInProcessClient(kvc)

	if err := c.Put("hello", "world"); err != nil {
		t.Fatal(err)
	}
	val, err := c.Get("hello")
	if err != nil || val != "world" {
		t.Fatalf("Get: want 'world', got %q err=%v", val, err)
	}

	if err := c.Delete("hello"); err != nil {
		t.Fatal(err)
	}
	val, _ = c.Get("hello")
	if val != "" {
		t.Fatalf("want empty after delete, got %q", val)
	}
}

func TestDuplicateRequests(t *testing.T) {
	kvc := newKVCluster(t, 3)
	defer kvc.cleanup()

	kvc.findLeader(2 * time.Second)
	c := newInProcessClient(kvc)

	_ = c.Put("x", "init")

	// same client sending the same requestId twice should be idempotent
	// we simulate this by manually crafting duplicate args
	args := &kvserver.PutArgs{
		Key:       "x",
		Value:     "once",
		ClientId:  c.clientId,
		RequestId: c.nextReq(),
	}
	for i := 0; i < 3; i++ {
		reply := &kvserver.PutReply{}
		for {
			ok := kvc.dispatch(c.leaderId, "KVServer.Put", args, reply)
			if !ok || reply.Err == kvserver.ErrNotLeader || reply.Err == kvserver.ErrTimeout {
				c.leaderId = (c.leaderId + 1) % len(kvc.nodes)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			break
		}
	}

	val, _ := c.Get("x")
	if val != "once" {
		t.Fatalf("want x='once', got %q", val)
	}
}

func TestConcurrentClients(t *testing.T) {
	kvc := newKVCluster(t, 3)
	defer kvc.cleanup()

	kvc.findLeader(2 * time.Second)

	const nClients = 5
	const nOps = 20

	var wg sync.WaitGroup
	errs := make([]error, nClients)

	for i := 0; i < nClients; i++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			c := newInProcessClient(kvc)
			for j := 0; j < nOps; j++ {
				key := fmt.Sprintf("c%d-k%d", cid, j)
				want := fmt.Sprintf("v%d", j)
				if err := c.Put(key, want); err != nil {
					errs[cid] = err
					return
				}
				got, err := c.Get(key)
				if err != nil {
					errs[cid] = err
					return
				}
				if got != want {
					errs[cid] = fmt.Errorf("key %s: want %q got %q", key, want, got)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	kvc := newKVCluster(t, 3)
	defer kvc.cleanup()

	leader := kvc.findLeader(2 * time.Second)
	c := newInProcessClient(kvc)

	_ = c.Put("key", "v1")

	follower := (leader + 1) % 3
	kvc.disconnect(follower)
	time.Sleep(100 * time.Millisecond)

	_ = c.Put("key2", "v2")

	kvc.reconnect(follower)
	time.Sleep(300 * time.Millisecond)

	val, _ := c.Get("key")
	if val != "v1" {
		t.Fatalf("want key=v1, got %q", val)
	}
	val, _ = c.Get("key2")
	if val != "v2" {
		t.Fatalf("want key2=v2, got %q", val)
	}
}
