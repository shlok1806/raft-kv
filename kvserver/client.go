package kvserver

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/shlok1806/raft-kv/transport"
)

// Client talks to the KV cluster. It tracks which node is the current
// leader and retries on failures so callers don't have to worry about it.
type Client struct {
	servers  []string // peer addresses
	tr       *transport.Transport
	leaderId int
	clientId int64
	reqSeq   int64
}

func NewClient(servers []string) *Client {
	tr := transport.New("") // outbound-only transport (no server side)
	return &Client{
		servers:  servers,
		tr:       tr,
		leaderId: 0,
		clientId: rand.Int63(),
	}
}

func (c *Client) nextReq() int64 {
	return atomic.AddInt64(&c.reqSeq, 1)
}

func (c *Client) Get(key string) (string, error) {
	args := &GetArgs{
		Key:       key,
		ClientId:  c.clientId,
		RequestId: c.nextReq(),
	}

	for {
		reply := &GetReply{}
		err := c.tr.Call(c.servers[c.leaderId], "KVServer.Get", args, reply)
		if err != nil || reply.Err == ErrNotLeader || reply.Err == ErrTimeout {
			c.leaderId = (c.leaderId + 1) % len(c.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == ErrNoKey {
			return "", nil
		}
		return reply.Value, nil
	}
}

func (c *Client) Put(key, value string) error {
	args := &PutArgs{
		Key:       key,
		Value:     value,
		ClientId:  c.clientId,
		RequestId: c.nextReq(),
	}

	for {
		reply := &PutReply{}
		err := c.tr.Call(c.servers[c.leaderId], "KVServer.Put", args, reply)
		if err != nil || reply.Err == ErrNotLeader || reply.Err == ErrTimeout {
			c.leaderId = (c.leaderId + 1) % len(c.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return nil
	}
}

func (c *Client) Delete(key string) error {
	args := &DeleteArgs{
		Key:       key,
		ClientId:  c.clientId,
		RequestId: c.nextReq(),
	}

	for {
		reply := &DeleteReply{}
		err := c.tr.Call(c.servers[c.leaderId], "KVServer.Delete", args, reply)
		if err != nil || reply.Err == ErrNotLeader || reply.Err == ErrTimeout {
			c.leaderId = (c.leaderId + 1) % len(c.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return nil
	}
}
