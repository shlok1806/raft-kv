package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/shlok1806/raft-kv/config"
	"github.com/shlok1806/raft-kv/kvserver"
	"github.com/shlok1806/raft-kv/raft"
	"github.com/shlok1806/raft-kv/transport"
)

func main() {
	var (
		id      = flag.Int("id", 0, "this node's index in the peers list")
		peers   = flag.String("peers", "", "comma-separated list of all peer addresses")
		dataDir = flag.String("data", "data", "directory for persistent state")
	)
	flag.Parse()

	if *peers == "" {
		fmt.Fprintln(os.Stderr, "usage: server -id N -peers addr1,addr2,...")
		os.Exit(1)
	}

	peerList := strings.Split(*peers, ",")
	myAddr := peerList[*id]

	cfg := config.DefaultConfig()
	cfg.Peers = peerList

	nodeDir := filepath.Join(*dataDir, fmt.Sprintf("node%d", *id))
	persister, err := raft.NewFilePersister(nodeDir)
	if err != nil {
		log.Fatalf("persister: %v", err)
	}

	tr := transport.New(myAddr)

	applyCh := make(chan raft.ApplyMsg, 64)

	sendRPC := func(peer int, method string, args, reply interface{}) bool {
		err := tr.Call(peerList[peer], method, args, reply)
		return err == nil
	}

	rf := raft.Make(*id, cfg, persister, applyCh, sendRPC)
	kv := kvserver.NewKVServer(*id, rf, applyCh, -1)

	// register both services on the same rpc server
	if err := tr.Register(rf); err != nil {
		log.Fatalf("register raft: %v", err)
	}
	if err := tr.Register(kv); err != nil {
		log.Fatalf("register kv: %v", err)
	}

	if err := tr.Listen(); err != nil {
		log.Fatalf("listen: %v", err)
	}

	log.Printf("node %d listening on %s", *id, myAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down")
	rf.Kill()
	kv.Kill()
	tr.Close()
}
