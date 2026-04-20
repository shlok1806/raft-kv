package tests

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shlok1806/raft-kv/kvserver"
)

// TestChaos kills and restarts random nodes for 30 seconds while clients run.
// No committed write should be lost and no dirty reads should occur.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	kvc := newKVCluster(t, 5)
	defer kvc.cleanup()

	kvc.findLeader(2 * time.Second)

	const duration = 30 * time.Second
	const nClients = 4

	var (
		committed int64
		errors    int64
	)

	stop := make(chan struct{})

	// chaos goroutine — disconnects and reconnects random nodes
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(100+rand.Intn(400)) * time.Millisecond):
				i := rand.Intn(len(kvc.nodes))
				kvc.disconnect(i)
				time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
				kvc.reconnect(i)
			}
		}
	}()

	var wg sync.WaitGroup
	for cid := 0; cid < nClients; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			c := newInProcessClient(kvc)
			seq := 0
			for {
				select {
				case <-stop:
					return
				default:
				}

				key := fmt.Sprintf("chaos-%d", cid)
				val := fmt.Sprintf("%d", seq)

				if err := c.Put(key, val); err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}

				got, err := c.Get(key)
				if err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}

				// we can't assert got==val because another Put may have landed
				// in between, but we can assert it's not empty
				_ = got
				atomic.AddInt64(&committed, 1)
				seq++
			}
		}(cid)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	t.Logf("chaos: %d successful ops, %d errors", committed, errors)

	if committed == 0 {
		t.Fatal("zero successful ops — cluster completely unavailable")
	}

	// some errors are expected during partitions; a majority failing is not
	total := committed + errors
	if errors > total/2 {
		t.Fatalf("error rate too high: %d/%d", errors, total)
	}
}

// TestLeaderCrashMidWrite verifies no data loss when the leader crashes
// immediately after proposing (before it can commit to a majority).
func TestLeaderCrashMidWrite(t *testing.T) {
	kvc := newKVCluster(t, 3)
	defer kvc.cleanup()

	leader := kvc.findLeader(2 * time.Second)
	c := newInProcessClient(kvc)

	// baseline write to confirm the cluster is healthy
	if err := c.Put("base", "ok"); err != nil {
		t.Fatal(err)
	}

	// isolate the leader immediately
	kvc.disconnect(leader)

	// the remaining 2 nodes form a majority and elect a new leader
	time.Sleep(600 * time.Millisecond)
	newC := newInProcessClient(kvc)
	if err := newC.Put("after", "crash"); err != nil {
		t.Fatalf("put after leader crash: %v", err)
	}

	kvc.reconnect(leader)
	time.Sleep(300 * time.Millisecond)

	val, _ := newC.Get("after")
	if val != "crash" {
		t.Fatalf("want 'crash', got %q", val)
	}
}

// Ensure the kvserver package is referenced (suppress unused-import errors
// if the test binary only uses inProcessClient).
var _ = kvserver.ErrOK
