package main

import (
	"context"
	"fmt"

	bm "github.com/zebra-uestc/chord/models/bridge"
	"google.golang.org/grpc"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"time"
)

type orderer struct {
	*bm.UnimplementedBlockTranserServer
	shutdownCh       chan struct{}
	receiveBlockChan chan *bm.Block
	addr             string
	//stack Stack
	firstBlock *bm.Block
	*grpc.Server
}

func (or *orderer) CreateID(id string) []byte {
	val := big.NewInt(0)
	val.SetString(id, 10)
	return val.Bytes()
}

func (or *orderer) GenBlock() *bm.Block {
	var emptyMentadata [][]byte
	genBlockHeader := &bm.BlockHeader{Number: 1, PreviousHash: or.CreateID("0"), DataHash: nil}
	genBlockMentadata := &bm.BlockMetadata{Metadata: emptyMentadata}
	return &bm.Block{Header: genBlockHeader, Data: nil, Metadata: genBlockMentadata}
}

func newOrderer(addr string) *orderer {

	order := &orderer{addr: addr}
	order.firstBlock = order.GenBlock()
	//order.stack.Push(block)
	//order.stack.len = 10
	return order
}

func (or *orderer) TransBlock(ctx context.Context, block *bm.Block) (*bm.DhtStatus, error) {
	or.receiveBlockChan <- block
	return nil, nil
}

func (or *orderer) Stop() {
	close(or.shutdownCh)
}

func (or *orderer) LoadConfig(context.Context, *bm.DhtStatus) (*bm.Block, error) {

	lastBlock := or.firstBlock

	config := &bm.Block{Header: lastBlock.Header, Data: lastBlock.Data, Metadata: lastBlock.Metadata}
	return config, nil
}

func main() {

	msgNum := uint64(1000)

	emptyEnvelope := &bm.Envelope{Payload: nil, Signature: nil}

	mainNodeAddress := "0.0.0.0:8001"

	addr := "0.0.0.0:50222"

	or := newOrderer(addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	bm.RegisterBlockTranserServer(s, or)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("server err: %v", err)
	}

	conn, err := grpc.Dial(mainNodeAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	c := bm.NewMsgTranserClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shut := make(chan bool)
	go func() {
		ticker := time.NewTicker(500 * time.Second)
		for {
			select {
			case <-ticker.C:
				msgNum++
				msg := &bm.Msg{ConfigSeq: msgNum, NormalMsg: emptyEnvelope, ConfigMsg: emptyEnvelope}
				_, err = c.TransMsg(ctx, msg)
				if err != nil {
					log.Fatalf("could not transcation msg: %v", err)
				}

			case <-shut:
				ticker.Stop()
				return
			}
		}
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for {
			select {
			case block := <-or.receiveBlockChan:
				fmt.Println(block)

			case <-shut:
				ticker.Stop()
				return

			}
		}
	}()

	m := make(chan os.Signal, 1)
	signal.Notify(m, os.Interrupt)
	<-m
	shut <- true
	or.Stop()

}
