package dhtnode

import (
	"context"
	"google.golang.org/grpc"
	"time"
	"unsafe"

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
	hahsVal, err := mainNode.hashValue(val)
	if err != nil {
		log.Println("hashVal err: ", err)
	}

	key := mainNode.byteToString(val)

	//通过dht环转发到其他节点并存储在storage里面,并且放在同到Msgchan
	err = mainNode.DhtNode.Set(key, hahsVal)
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

func (mainNode *MainNode) byteToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (mainNode *MainNode) hashValue(val []byte) ([]byte, error) {
	h := mainNode.Node.Cnf.Hash()
	if _, err := h.Write(val); err != nil {
		return nil, err
	}
	hashVal := h.Sum(nil)
	return hashVal, nil
}
