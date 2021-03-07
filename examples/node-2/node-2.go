package main

import (
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/zebra-uestc/chord"
	cm "github.com/zebra-uestc/chord/models/chord")

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

	id1 := "1"
	sister := chord.NewInode(id1, "0.0.0.0:8001")

	h, err := createNode("4", "0.0.0.0:8002", sister)
	if err != nil {
		log.Fatalln(err)
		return
	}

	shut := make(chan bool)
	var count int
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				count++
				key := strconv.Itoa(count)
				value := fmt.Sprintf(`{"graph_id" : %d, "nodes" : ["node-%d","node-%d","node-%d"]}`, count, count+1, count+2, count+3)
				sErr := h.Set([]byte(key), []byte(value))
				if sErr != nil {
					log.Println("err: ", sErr)
				}
			case <-shut:
				ticker.Stop()
				return
			}
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	shut <- true
	h.Stop()
}
