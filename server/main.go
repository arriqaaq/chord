package server

import (
	"github.com/zebra-uestc/chord/dhtnode"
	"log"
	"math/big"
	"os"
	"os/signal"
	"time"

	"github.com/zebra-uestc/chord"
	"github.com/zebra-uestc/chord/models"
)

var mainNode *dhtnode.MainNode

func createMainNode(id string, addr string) (*dhtnode.MainNode, error) {
	h, err := createNode(id, addr, nil)
	if err != nil {
		log.Fatalln(err)
	}
	mainNode = &dhtnode.MainNode{DhtNode: h}
	return mainNode, err
}

func createNode(id string, addr string, sister *models.Node) (*dhtnode.DhtNode, error) {
	node, err := chord.NewNode(cnf, joinNode)

	cnf := chord.DefaultConfig()
	cnf.Id = id
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond

	n, err := dhtnode.NewDhtNode(cnf, sister)
	return n, err
}

func createID(id string) []byte {
	val := big.NewInt(0)
	val.SetString(id, 10)
	return val.Bytes()
}

var addresses = []string{
	"0.0.0.0:8002",
	"0.0.0.0:8003",
	"0.0.0.0:8004",
}
var mainAddress = "0.0.0.0:8001"

func Main() {
	//创建mainNode节点
	sister, err := createMainNode("0", mainAddress)
	if err != nil {
		log.Fatalln(err)
	}
	//根据主节点创建其他节点，构成哈希环
	for index, address := range addresses {
		_, err := createNode(string(index+1), address, sister.Node.Node)
		if err != nil {
			log.Fatalln(err)
		}
	}

	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-time.After(10 * time.Second)
	<-c
}
