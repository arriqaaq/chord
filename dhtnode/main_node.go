package dhtnode

import (
	"context"
	bm "github.com/zebra-uestc/chord/dhtnode/bridge"
)

type MainNode struct {
	*DhtNode
	bm.UnimplementedBridgeServer
	//*dhtnode.dht_node
}

// order To dht
func (mainNode *MainNode) TransMsg(context.Context, *bm.Msg) (*bm.Status, error) {

}

// dht to order
func (mainNode *MainNode) TransBlock(context.Context, *bm.Block) (*bm.Status, error) {
	return nil, nil
}

// dht调用，orderer实现
func (mainNode *MainNode) LoadConfig(context.Context, *bm.Status) (*bm.Config, error) {

}
