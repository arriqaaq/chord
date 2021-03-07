package dhtnode

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/zebra-uestc/chord"
	bm "github.com/zebra-uestc/chord/models/bridge"
)

var (
	eMptyRequest   = &bm.DhtStatus{}
	OrdererAddress = "0.0.0.0:50222"
)

type MainNode interface {
	AddNode(id string, addr string) error
	Stop()
}

type server struct{}

type mainNode struct {
	*dhtNode
	prevBlockChan chan *bm.Block
	sendBlockChan chan *bm.Block
	bm.UnimplementedBlockTranserServer
	bm.UnimplementedMsgTranserServer
	lastBlock *bm.Block
}

func NewMainNode(id string, addr string) (MainNode, error) {
	mainNodeAddress = addr
	nodeCnf := chord.DefaultConfig()
	nodeCnf.Id = id
	nodeCnf.Addr = addr
	nodeCnf.Timeout = 10 * time.Millisecond
	nodeCnf.MaxIdle = 100 * time.Millisecond
	node, err := NewDhtNode(nodeCnf, nil)

	mainNode := &mainNode{dhtNode: node}

	//向orderer询问lastBlock
	conn, err := grpc.Dial(OrdererAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	c := bm.NewBlockTranserClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.LoadConfig(ctx, nil)
	mainNode.lastBlock = r
	if err != nil {
		log.Fatalf("could not transcation Block: %v", err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	bm.RegisterBlockTranserServer(s, mainNode)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("fail to  serve: %v", err)
	}

	//给prevBlock编号
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case prevBlock := <-mainNode.prevBlockChan:
				mainNode.sendBlockChan <- mainNode.FinalBlock(mainNode.lastBlock, prevBlock)

			case <-mainNode.GetShutdownCh():
				ticker.Stop()
			}
		}
	}()

	//给orderer发Block
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case finalBlock := <-mainNode.sendBlockChan:
				conn, err := grpc.Dial(OrdererAddress, grpc.WithInsecure(), grpc.WithBlock())
				if err != nil {
					log.Fatalf("did not connect: %v", err)
				}
				c := bm.NewBlockTranserClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, err = c.TransBlock(ctx, &bm.Block{Header: finalBlock.Header, Data: finalBlock.Data, Metadata: finalBlock.Metadata /*参数v*/})

			case <-mainNode.GetShutdownCh():
				ticker.Stop()
			}
		}
	}()

	return mainNode, err
}

type mainNodeInside interface {
	SendPrevBlockToChan(*bm.Block)
}

func (mn *mainNode) SendPrevBlockToChan(block *bm.Block) {
	mn.prevBlockChan <- block
}

func (mn *mainNode) AddNode(id string, addr string) error {
	cnf := chord.DefaultConfig()
	cnf.Id = id
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond
	_, err := NewDhtNode(cnf, mn.dhtNode.Node.Node)
	return err
}

// order To dht的处理
func (mn *mainNode) TransMsg(ctx context.Context, msg *bm.Msg) (*bm.DhtStatus, error) {
	key, err := proto.Marshal(msg)
	if err != nil {
		log.Println("Marshal err: ", err)
	}
	hashVal, err := mn.hashValue(key)
	if err != nil {
		log.Println("hashVal err: ", err)
	}
	//通过dht环转发到其他节点并存储在storage里面,并且放在同到Msgchan
	err = mn.Set(key, hashVal)
	return nil, err
}

//接收其他节点的block
func (mainNode *mainNode) TransBlock(ctx context.Context, block *bm.Block) (*bm.DhtStatus, error) {
	mainNode.prevBlockChan <- block
	return nil, nil
}

//给区块编号
func (mainNode *mainNode) FinalBlock(lastBlock *bm.Block, block *bm.Block) *bm.Block {
	block.Header.PreviousHash = lastBlock.Header.PreviousHash
	block.Header.Number = lastBlock.Header.Number + 1
	return block
}

func (mainNode *mainNode) hashValue(val []byte) ([]byte, error) {
	h := mainNode.Node.GetConfig().Hash()
	if _, err := h.Write(val); err != nil {
		return nil, err
	}
	hashVal := h.Sum(nil)
	return hashVal, nil
}

func (mainNode *mainNode) Stop() {
	close(mainNode.GetShutdownCh())
}
