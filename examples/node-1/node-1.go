package main

import (
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"time"

	"github.com/zebra-uestc/chord"
	cm "github.com/zebra-uestc/chord/models/chord"
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

	h, err := createNode("1", "0.0.0.0:8001", nil)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(h.FingerTableString())
	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-time.After(10 * time.Second)
	<-c
	h.Stop()

}
