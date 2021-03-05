package dhtnode

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/zebra-uestc/chord"
	bm "github.com/zebra-uestc/chord/dhtnode/bridge"
	"github.com/zebra-uestc/chord/models"
	"log"
)

var (
	emptyRequest = &bm.Status{}
)

type MainNode struct {
	*DhtNode
	bm.UnimplementedBridgeToDhtServer
	//*dhtnode.dht_node
}

// order To dht的处理
func (mainNode *MainNode) TransMsg(ctx context.Context, msg *bm.Msg) (*bm.Status, error) {

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

// dht调用，orderer实现
func (mainNode *MainNode) LoadConfig(context.Context, *bm.Status) (*bm.Config, error) {
	return nil, nil
}
