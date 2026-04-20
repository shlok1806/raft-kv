package transport

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const callTimeout = 200 * time.Millisecond

// Transport wraps net/rpc and handles peer dialing with a simple connection
// pool. Failed dials return an error — callers must handle that gracefully.
type Transport struct {
	addr string
	srv  *rpc.Server

	mu      sync.Mutex
	clients map[string]*rpc.Client // addr -> live client
}

func New(addr string) *Transport {
	return &Transport{
		addr:    addr,
		srv:     rpc.NewServer(),
		clients: make(map[string]*rpc.Client),
	}
}

// Register exposes svc under the default name (type name of svc).
func (t *Transport) Register(svc interface{}) error {
	return t.srv.Register(svc)
}

// Listen starts accepting connections. Blocks until the listener closes.
func (t *Transport) Listen() error {
	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", t.addr, err)
	}
	go t.srv.Accept(ln)
	return nil
}

// Call invokes method on peer. Returns error on timeout or network failure.
// Never panics — dead peers are a normal operating condition.
func (t *Transport) Call(peer, method string, args, reply interface{}) error {
	client, err := t.getClient(peer)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- client.Call(method, args, reply)
	}()

	select {
	case err := <-done:
		if err != nil {
			// drop the cached client so the next call re-dials
			t.mu.Lock()
			delete(t.clients, peer)
			t.mu.Unlock()
		}
		return err
	case <-time.After(callTimeout):
		t.mu.Lock()
		delete(t.clients, peer)
		t.mu.Unlock()
		return fmt.Errorf("rpc to %s timed out", peer)
	}
}

func (t *Transport) getClient(peer string) (*rpc.Client, error) {
	t.mu.Lock()
	c, ok := t.clients[peer]
	t.mu.Unlock()
	if ok {
		return c, nil
	}

	// dial outside the lock so we don't block other goroutines
	c, err := rpc.Dial("tcp", peer)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", peer, err)
	}

	t.mu.Lock()
	// another goroutine might have dialed in the meantime; just keep both
	// and let the old one close naturally via the call above
	t.clients[peer] = c
	t.mu.Unlock()

	return c, nil
}

// Close shuts down all cached connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for addr, c := range t.clients {
		c.Close()
		delete(t.clients, addr)
	}
}
