package kvserver

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shlok1806/raft-kv/raft"
)

const opTimeout = 500 * time.Millisecond

// result carries the outcome of a committed Op back to the waiting RPC handler.
type result struct {
	value string
	err   Err
}

// KVServer sits on top of a Raft node and applies committed log entries to
// an in-memory key-value store.
type KVServer struct {
	mu   sync.Mutex
	me   int
	rf   *raft.Raft
	dead int32

	maxRaftState int // snapshot when persisted state exceeds this (bytes); -1 = never

	// the actual data
	store map[string]string

	// dedup table: ClientId -> last applied RequestId
	// prevents a retried RPC from being applied twice
	lastReqId map[int64]int64

	// per log-index channel that the apply goroutine signals when committed
	pending map[int]chan result

	applyCh chan raft.ApplyMsg
}

func NewKVServer(me int, rf *raft.Raft, applyCh chan raft.ApplyMsg, maxRaftState int) *KVServer {
	gob.Register(Op{})

	kv := &KVServer{
		me:           me,
		rf:           rf,
		maxRaftState: maxRaftState,
		store:        make(map[string]string),
		lastReqId:    make(map[int64]int64),
		pending:      make(map[int]chan result),
		applyCh:      applyCh,
	}

	go kv.applyLoop()
	return kv
}

// -----------------------------------------------------------------------
// RPC handlers — each submits an Op to Raft and waits for it to commit
// -----------------------------------------------------------------------

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	val, err := kv.submit(op)
	reply.Value = val
	reply.Err = err
	return nil
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) error {
	op := Op{
		Type:      "Put",
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	_, err := kv.submit(op)
	reply.Err = err
	return nil
}

func (kv *KVServer) Delete(args *DeleteArgs, reply *DeleteReply) error {
	op := Op{
		Type:      "Delete",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	_, err := kv.submit(op)
	reply.Err = err
	return nil
}

func (kv *KVServer) submit(op Op) (string, Err) {
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrNotLeader
	}

	ch := make(chan result, 1)
	kv.mu.Lock()
	kv.pending[idx] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.pending, idx)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		return res.value, res.err
	case <-time.After(opTimeout):
		return "", ErrTimeout
	}
}

// -----------------------------------------------------------------------
// Apply loop — reads from applyCh and drives the state machine
// -----------------------------------------------------------------------

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.SnapshotValid {
			kv.installSnapshot(msg.Snapshot, msg.SnapshotIndex, msg.SnapshotTerm)
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()

		var res result
		// dedup check: skip if we already applied this request
		if kv.lastReqId[op.ClientId] >= op.RequestId {
			if op.Type == "Get" {
				res.value = kv.store[op.Key]
			}
			res.err = ErrOK
		} else {
			res = kv.apply(op)
			kv.lastReqId[op.ClientId] = op.RequestId
		}

		ch, hasCh := kv.pending[msg.CommandIndex]
		kv.mu.Unlock()

		if hasCh {
			ch <- res
		}
	}
}

func (kv *KVServer) apply(op Op) result {
	// caller holds kv.mu
	switch op.Type {
	case "Get":
		v, ok := kv.store[op.Key]
		if !ok {
			return result{err: ErrNoKey}
		}
		return result{value: v, err: ErrOK}
	case "Put":
		kv.store[op.Key] = op.Value
		return result{err: ErrOK}
	case "Delete":
		delete(kv.store, op.Key)
		return result{err: ErrOK}
	}
	return result{err: ErrOK}
}

// -----------------------------------------------------------------------
// Snapshot support
// -----------------------------------------------------------------------

func (kv *KVServer) snapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(kv.store)
	enc.Encode(kv.lastReqId)

	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *KVServer) installSnapshot(data []byte, index, term int) {
	if !kv.rf.CondInstallSnapshot(term, index, data) {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	var store map[string]string
	var lastReq map[int64]int64
	if err := dec.Decode(&store); err != nil {
		return
	}
	if err := dec.Decode(&lastReq); err != nil {
		return
	}

	kv.store = store
	kv.lastReqId = lastReq
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}
