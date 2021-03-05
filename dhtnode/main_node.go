package dhtnode

import (
	"context"
	"google.golang.org/grpc"
	"time"

	"github.com/golang/protobuf/proto"

	bm "github.com/zebra-uestc/chord/dhtnode/bridge"
	"log"
)

var (
	emptyRequest   = &bm.StatusA{}
	OrdererAddress = "3123123412"
)

type MainNode struct {
	*DhtNode
	sendBlockChan chan *bm.Block
	bm.UnimplementedBlockTranserServer
	bm.UnimplementedMsgTranserServer
	//*dhtnode.dht_node
}

// order To dht的处理
func (mainNode *MainNode) TransMsg(ctx context.Context, msg *bm.Msg) (*bm.StatusA, error) {

	val, err := proto.Marshal(msg)
	if err != nil {
		log.Println("Marshal err: ", err)
	}
	key, err := mainNode.HashKey(val)
	if err != nil {
		log.Println("Hashkey err: ", err)
	}
	//通过dht环转发到其他节点并存储在storage里面
	err = mainNode.DhtNode.Set(key, val)
	return emptyRequest, err
}
func (mainNode *MainNode) TransBlock(ctx context.Context, block *bm.Block) (*bm.StatusA, error) {
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

// dht调用，orderer实现
func (mainNode *MainNode) LoadConfig(context.Context, *bm.Status) (*bm.Config, error) {
	return nil, nil
}
