package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/shlok1806/raft-kv/kvserver"
)

func main() {
	peers := flag.String("peers", "", "comma-separated list of server addresses")
	flag.Parse()

	if *peers == "" || flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: client -peers addr1,addr2,... <get|put|delete> key [value]")
		os.Exit(1)
	}

	serverList := strings.Split(*peers, ",")
	c := kvserver.NewClient(serverList)

	cmd := flag.Arg(0)
	key := flag.Arg(1)

	switch cmd {
	case "get":
		val, err := c.Get(key)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if val == "" {
			fmt.Println("(not found)")
		} else {
			fmt.Println(val)
		}

	case "put":
		if flag.NArg() < 3 {
			fmt.Fprintln(os.Stderr, "put requires a value")
			os.Exit(1)
		}
		if err := c.Put(key, flag.Arg(2)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("ok")

	case "delete":
		if err := c.Delete(key); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("ok")

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(1)
	}
}
