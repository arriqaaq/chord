package main

import (
	"log"
	"math/big"
	"os"
	"os/signal"

	"github.com/zebra-uestc/chord"
	cm "github.com/zebra-uestc/chord/models/chord"

	// "strconv"
	"time"
)

func createNode(id string, addr string, sister *cm.Node) (*chord.Node, error) {

	cnf := chord.DefaultConfig()
	cnf.Id = id
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond

	n, err := chord.NewNode(cnf, sister)
	return n, err
}

func createID(id string) []byte {
	val := big.NewInt(0)
	val.SetString(id, 10)
	return val.Bytes()
}

func main() {

	joinNode := chord.NewInode("1", "0.0.0.0:8001")

	h, err := createNode("8", "0.0.0.0:8003", joinNode)
	if err != nil {
		log.Fatalln(err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	h.Stop()
}
