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

	cnf := chord.DefaultConfig()
	cnf.Id = id
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond

	n, err := chord.NewNode(cnf, sister)
	// log.Println("hook", id, n, err)
	return n, err
}

func createID(id string) []byte {
	val := big.NewInt(0)
	val.SetString(id, 10)
	return val.Bytes()
}

func main() {

	createNode("1", "0.0.0.0:8001", nil)

	id1 := "1"
	sister := chord.NewInode(id1, "0.0.0.0:8001")
	_, err := createNode("4", "0.0.0.0:8002", sister)
	if err != nil {
		log.Println(err)
		return
	}
	h, err := createNode("8", "0.0.0.0:8003", sister)
	if err != nil {
		log.Println(h, err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-time.After(20 * time.Second)
	var x *internal.Node
	var y error
	x, y = h.Find("3")
	log.Println("found-------->", y)
	log.Printf("id %x\n", x.Id)
	<-c
}
