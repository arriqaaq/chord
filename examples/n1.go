package main

import (
	"github.com/arriqaaq/chord"
	"github.com/arriqaaq/chord/internal"
	"log"
	"math/big"
	"os"
	"os/signal"
	"time"
)

func createNode(id string, addr string, sister *internal.Node) {
	val := big.NewInt(0)
	val.SetString(id, 10)

	cnf := chord.DefaultConfig()
	cnf.Id = val.Bytes()
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Second
	cnf.MaxIdle = 10 * time.Second

	n, err := chord.NewNode(cnf, sister)
	log.Println("hook", id, n, err)
}

func main() {
	createNode("1", "0.0.0.0:8001", nil)

	val := big.NewInt(0)
	val.SetString("1", 10)

	sister := chord.NewInode(val.Bytes(), "0.0.0.0:8001")
	createNode("2", "0.0.0.0:8002", sister)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
}
