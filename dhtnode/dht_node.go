package dhtnode

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/zebra-uestc/chord"
	bm "github.com/zebra-uestc/chord/models/bridge"
	cm "github.com/zebra-uestc/chord/models/chord"
	"google.golang.org/grpc"
)

var (
	emptyPrevHash = []byte{}
	//TODO:传入主节点的addr,传入configtx.yaml文件中的batchTimeout
	mainNodeAddress = "0.0.0.0:8001"
	batchTimeout    = 2 * time.Second
)

type dhtNode struct {
	bm.UnimplementedBlockTranserServer
	bm.UnimplementedMsgTranserServer
	exitChan chan struct{}
	preBlock preprocess
	*chord.Node

	mn mainNodeInside
}

func (dhtn *dhtNode) DhtInsideTransBlock(block *bm.Block) error {
	if dhtn.Addr != mainNodeAddress {
		conn, err := grpc.Dial(mainNodeAddress, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		c := bm.NewBlockTranserClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = c.TransBlock(ctx, &bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
		if err != nil {
			log.Fatalf("could not transcation Block: %v", err)
		}
		return err
	} else {
		// 将生成的Block放到mainNode底下的Channel中
		dhtn.mn.SendPrevBlockToChan(block)

	}
	return nil
}

func NewDhtNode(cnf *chord.Config, joinNode *cm.Node) (*dhtNode, error) {
	node, err := chord.NewNode(cnf, joinNode)
	dhtnode := &dhtNode{Node: node}

	if err != nil {
		log.Println("transport start error:", err)
		return nil, err
	}

	txStore, ok := dhtnode.GetStorage().(chord.TxStorage)
	if !ok {
		log.Fatal("Storage Error")
		return nil, errors.New("Storage Error")
	}
	sendMsgChan := txStore.GetMsgChan()
	//生成prevBlock
	go func() {
		var timer <-chan time.Time
		for {
			select {

			case msg := <-sendMsgChan:
				if msg.ConfigMsg == nil {
					batches, pending := dhtnode.preBlock.Ordered(msg.NormalMsg)
					//出块并发送给mainnode或者orderer
					for _, batch := range batches {
						block := dhtnode.preBlock.PreCreateNextBlock(batch)
						//将PreCreateNextBlock传给MainNode
						err := dhtnode.DhtInsideTransBlock(block)
						if err != nil {
							log.Fatalf("could not transcation Block: %v", err)
						}
					}

					switch {
					case timer != nil && !pending:
						// Timer is already running but there are no messages pending, stop the timer
						timer = nil
					case timer == nil && pending:
						// Timer is not already running and there are messages pending, so start it
						//默认时间1s
						timer = time.After(batchTimeout)
						logger.Debugf("Just began %s batch timer", batchTimeout.String())
					default:
						// Do nothing when:
						// 1. Timer is already running and there are messages pending
						// 2. Timer is not set and there are no messages pending
					}
				} else {
					batch := dhtnode.preBlock.Cut()
					if batch != nil {
						block := dhtnode.preBlock.PreCreateNextBlock(batch)
						err := dhtnode.DhtInsideTransBlock(&bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
						if err != nil {
							log.Fatalf("could not transcation Block: %v", err)
						}
					}
					block := dhtnode.preBlock.PreCreateNextBlock([]*bm.Envelope{msg.ConfigMsg})
					err := dhtnode.DhtInsideTransBlock(&bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
					if err != nil {
						log.Fatalf("could not transcation Block: %v", err)
					}
					timer = nil
				}
			case <-timer:
				//clear the timer
				timer = nil
				batch := dhtnode.preBlock.Cut()
				if len(batch) == 0 {
					logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
					continue
				}
				logger.Debugf("Batch timer expired, creating block")
				block := dhtnode.preBlock.PreCreateNextBlock(batch)
				err := dhtnode.DhtInsideTransBlock(&bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
				if err != nil {
					log.Fatalf("could not transcation Block: %v", err)
				}

			case <-dhtnode.GetShutdownCh():
				logger.Debugf("Exiting")
				return
			}
		}
	}()
	return dhtnode, err
}
