package dhtnode

import (
	"context"
	"errors"
	"fmt"
	"github.com/zebra-uestc/chord"
	bm "github.com/zebra-uestc/chord/dhtnode/bridge"
	"github.com/zebra-uestc/chord/models"
	"google.golang.org/grpc"
	"log"
	"time"
)

var (
	emptyPrevHash = []byte{}
	MainNodeAddress = "127.0.0.1:3433"
	batchTimeout = time.Second
)

//TODO:需要哪些配置
type DhtConfig struct {
	Addr string
}

type DhtNode struct {

	bm.UnimplementedBlockTranserServer
	bm.UnimplementedMsgTranserServer
	exitChan chan struct{}
	preBlock	preprocess
	*chord.Node
	//*bm.UnimplementedBridgeToOrderServer
	//*bm.UnimplementedBridgeToDhtServer
	//*dhtnode.dht_node
}


//TODO:node的cnf和dhtNode的cnf还得分开
func NewDhtNode(cnf *chord.Config, joinNode *models.Node) (*DhtNode, error) {
	node, err := chord.NewNode(cnf, joinNode)
	chord.InitNode((&DhtNode{Node: node}).Node)
	adress := MainNodeAddress
	dhtnode := &DhtNode{Node: node}

	if err != nil {
		log.Println("transport start error:", err)
		return nil, err
	}

	txStore, ok:= dhtnode.Storage.(chord.TxStorage)
	if !ok {
		log.Fatal("Storage Error")
		return nil, errors.New("Storage Error")
	}
	sendMsgChan := txStore.GetMsgChan()

	go func() {
		var timer <-chan time.Time
		for {
			select {

			case msg := <-sendMsgChan:
				if node.Addr == MainNodeAddress {
					adress = OrdererAddress
				}
				if node.(*MainNode) != nil{
					node.(*MainNode).
				}
				conn, err := grpc.Dial(adress, grpc.WithInsecure(), grpc.WithBlock())
				if err != nil {
					log.Fatalf("did not connect: %v", err)
				}
				c := bm.NewBlockTranserClient(conn)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				if msg.configMsg == nil {
					batches, pending := dhtnode.preBlock.Ordered(msg.normalMsg)
					//出块并发送给mainnode或者orderer
					for _, batch := range batches {
						block := dhtnode.preBlock.PreCreateNextBlock(batch)
						//想把r去掉 = =
						r, err := c.TransBlock(ctx, &bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
						fmt.Println(r)
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
						r, err := c.TransBlock(ctx, &bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
						fmt.Println(r)
						if err != nil {
							log.Fatalf("could not transcation Block: %v", err)
						}

					}

					block := dhtnode.preBlock.PreCreateNextBlock([]*bm.Envelope{msg.configMsg})
					r, err := c.TransBlock(ctx, &bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
					fmt.Println(r)
					if err != nil {
						log.Fatalf("could not transcation Block: %v", err)
					}
					timer = nil
				}
			case <-timer:
				//clear the timer
				timer = nil
				if node.Addr == MainNodeAddress {
					adress = OrdererAddress
				}
				conn, err := grpc.Dial(adress, grpc.WithInsecure(), grpc.WithBlock())
				if err != nil {
					log.Fatalf("did not connect: %v", err)
				}
				c := bm.NewBlockTranserClient(conn)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				batch := dhtnode.preBlock.Cut()
				if len(batch) == 0 {
					logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
					continue
				}
				logger.Debugf("Batch timer expired, creating block")
				block := dhtnode.preBlock.PreCreateNextBlock(batch)
				r, err := c.TransBlock(ctx, &bm.Block{Header: block.Header, Data: block.Data, Metadata: block.Metadata /*参数v*/})
				fmt.Println(r)
				if err != nil {
					log.Fatalf("could not transcation Block: %v", err)
				}

			//TODO:退出机制
			case <-dhtnode.exitChan:
				logger.Debugf("Exiting")
				return
			}

}
	}()

	return &DhtNode{Node: node}, err
}



//func (dn *DhtNode) TransMsg(ctx context.Context, msg *Msg) (*StatusA, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method TransMsg not implemented")
//}
//func (dn *DhtNode) LoadConfig(ctx context.Context, status *StatusA) (*Config, error) {
//	return nil, status.Errorf(codes.Unimplemented, "method LoadConfig not implemented")
//}

