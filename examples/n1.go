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

func createNode(id string, addr string, sister *internal.Node) (*chord.Node, error) {
	val := big.NewInt(0)
	val.SetString(id, 10)

	cnf := chord.DefaultConfig()
	cnf.Id = val.Bytes()
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond

	n, err := chord.NewNode(cnf, sister)
	// log.Println("hook", id, n, err)
	return n, err
}

func main() {
	var x *internal.Node
	var y error

	createNode("1", "0.0.0.0:8001", nil)

	val := big.NewInt(0)
	val.SetString("1", 10)

	sister := chord.NewInode(val.Bytes(), "0.0.0.0:8001")
	h, err := createNode("3", "0.0.0.0:8002", sister)
	if err != nil {
		log.Println(h, err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	t := big.NewInt(0)
	t.SetString("4", 10)
	x, y = h.Find(t.Bytes())
	log.Println("found-------->", x, y)
	<-c
}
