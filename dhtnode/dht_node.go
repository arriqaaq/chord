package dhtnode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/zebra-uestc/chord"
	"github.com/zebra-uestc/chord/dhtnode/bridge"
	"github.com/zebra-uestc/chord/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"time"
)

var (
	emptyPrevHash = []byte{}
)

//TODO:需要哪些配置
type DhtConfig struct {
	Addr string
}

type DhtNode struct {
	*chord.Node
	*bridge.UnimplementedBridgeToOrderServer
	//*dhtnode.dht_node
	dhttransport dhtTransport
}

//TODO:node的cnf和dhtNode的cnf还得分开
func NewDhtNode(cnf *chord.Config, joinNode *models.Node) (*DhtNode, error) {
	node, err := chord.NewNode(cnf, joinNode)
	dhtnode := &DhtNode{Node: node}
	// Start RPC server
	transport, err := NewGrpcdhtTransport(cnf)
	if err != nil {
		log.Println("transport start error:", err)
		return nil, err
	}

	//开启dht内部传输通道
	bridge.RegisterBridgeToDhtServer(transport.server, dhtnode)

	dhtnode.dhttransport.Start()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				// fmt.Println("stabilize()...")
				node.stabilize()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	return &DhtNode{Node: node}, err
}

// CreateNextBlock creates a new block with the next block number, and the given contents.
func (dn *DhtNode) PreCreateNextBlock(messages []*bridge.Envelope) *bridge.Block {
	// previousBlockHash := protoutil.BlockHeaderHash(bw.lastBlock.Header)

	data := &bridge.BlockData{
		Data: make([][]byte, len(messages)),
	}

	var err error
	for i, msg := range messages {
		data.Data[i], err = proto.Marshal(msg)
		if err != nil {
			logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	// block := protoutil.NewBlock(bw.lastBlock.Header.Number+1, previousBlockHash)
	block := protoutil.NewBlock(0, emptyPrevHash)
	block.Header.DataHash = BlockDataHash(data)
	block.Data = data

	return block
}

func (UnimplementedBridgeToOrderServer) TransBlock(context.Context, *Block) (*Status, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TransBlock not implemented")
}

func BlockDataHash(b *bridge.BlockData) []byte {
	sum := sha256.Sum256(bytes.Join(b.Data, nil))
	return sum[:]
}

func NewBlock(seqNum uint64, previousHash []byte) *cb.Block {
	block := &cb.Block{}
	block.Header = &cb.BlockHeader{}
	block.Header.Number = seqNum
	block.Header.PreviousHash = previousHash
	block.Header.DataHash = []byte{}
	block.Data = &cb.BlockData{}

	var metadataContents [][]byte
	for i := 0; i < len(cb.BlockMetadataIndex_name); i++ {
		metadataContents = append(metadataContents, []byte{})
	}
	block.Metadata = &cb.BlockMetadata{Metadata: metadataContents}

	return block
}
