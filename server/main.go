package main

import (
	"log"
	"os"
	"os/signal"
	// "strconv"
	"time"

	"github.com/zebra-uestc/chord/dhtnode"
)

var mainNode dhtnode.MainNode

var addresses = []string{
	"0.0.0.0:8001",
	"0.0.0.0:8002",
	"0.0.0.0:8003",
	"0.0.0.0:8004",
}

func main() {
	mainNode, err := dhtnode.NewMainNode()
	mainNode.StartDht("0", addresses[0])
	mainNode.StartTransBlockServer(addresses[1])
	mainNode.StartTransMsgServer(addresses[2])

	if err != nil {
		log.Fatalln(err)
	}
	//根据主节点创建其他节点，构成哈希环
	// for index, address := range addresses {
	// 	go func (index int,address string)  {
	// 		var err error
	// 		if index == 0 {
	// 			mainNode, err = dhtnode.NewMainNode("0", address)
	// 		} else {
	// 			err = mainNode.AddNode(strconv.Itoa(index+1), address)
	// 		}
	// 		if err != nil {
	// 			log.Fatalln(err)
	// 		}
	// 	}(index,address)
	// }

	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-time.After(10 * time.Second)
	<-c
	mainNode.Stop()
}
