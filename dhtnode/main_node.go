package dhtnode

import (
	"log"
	"context"
	"google.golang.org/grpc"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/zebra-uestc/chord"
	bm "github.com/zebra-uestc/chord/models/bridge"
)

var (
	eMptyRequest   = &bm.DhtStatus{}
	OrdererAddress = "3123123412"
)

type MainNode interface{
	AddNode(id string, addr string)error
}

func NewMainNode(id string, addr string) (MainNode,error){
	mainNodeAddress = addr
	return &mainNode{}, nil
}

type mainNodeInside interface{
	
}

type mainNode struct {
	*dhtNode
	sendBlockChan chan *bm.Block
	bm.UnimplementedBlockTranserServer
	bm.UnimplementedMsgTranserServer
	*bm.Config
}

func (mn *mainNode) AddNode(id string, addr string) error{
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

//接收其他节点的block，放到通道Blockchan中
func (mainNode *mainNode) TransBlock(ctx context.Context, block *bm.Block) (*bm.DhtStatus, error) {
	conn, err := grpc.Dial(OrdererAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := bm.NewBlockTranserClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.TransBlock(ctx, &bm.Block{ /*参数v*/ })
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	return r, err
}

//TODO:需要有一个给区块标号的方法
func FinalBlock(config *bm.Config, block *bm.Block) *bm.Block {
	// block.Header.Number = config.
	return nil
}

// dht调用，orderer实现
func (mainNode *mainNode) LoadConfig(context.Context, *bm.DhtStatus) (*bm.Config, error) {
	return nil, nil
}

func (mainNode *mainNode) hashValue(val []byte) ([]byte, error) {
	h := mainNode.Node.GetConfig().Hash()
	if _, err := h.Write(val); err != nil {
		return nil, err
	}
	hashVal := h.Sum(nil)
	return hashVal, nil
}
