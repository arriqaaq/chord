package server

import (
	"fmt"
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

func createMainNode(id string, addr string) error {
	h, err := createNode(id, addr, nil)
	if err != nil {
		log.Fatalln(err)
	}
	mainNode = &dhtnode.MainNode{DhtNode: h}
	return nil
}

func createNode(id string, addr string, sister *models.Node) (*dhtnode.DhtNode, error) {

	cnf := chord.DefaultConfig()
	fmt.Println(cnf.HashSize)
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

func main() {
	createMainNode("0", mainAddress)
	for index, address := range addresses {
		_, err := createNode(string(index+1), address, nil)
		if err != nil {
			log.Fatalln(err)
		}
	}

	fmt.Println(h.FingerTableString())
	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-time.After(10 * time.Second)
	<-c
}
