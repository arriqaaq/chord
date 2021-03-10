package dhtnode

import (
	"bytes"
	"crypto/sha256"
	"github.com/zebra-uestc/chord/models/bridge"
	"time"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"google.golang.org/protobuf/proto"
)

/*
PrevBlock生成类
*/

var logger = flogging.MustGetLogger("orderer.consensus.dht")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

type DhtConfig struct {
	// Simply specified as number of messages for now, in the future
	// we may want to allow this to be specified by size in bytes
	MaxMessageCount uint32
	// The byte count of the serialized messages in a batch cannot
	// exceed this value.
	AbsoluteMaxBytes uint32
	// The byte count of the serialized messages in a batch should not
	// exceed this value.
	PreferredMaxBytes uint32

	MainNodeAddress string

	BatchTimeout time.Duration
}

func (dhtn *dhtNode) DefaultDhtConfig() *DhtConfig {
	return &DhtConfig{MaxMessageCount: 500,
		AbsoluteMaxBytes:  10 * 1024 * 1024,
		PreferredMaxBytes: 2 * 1024 * 1024,
		MainNodeAddress:   "0.0.0.0:8001",
		BatchTimeout:      2 * time.Second}
}

func (dhtn *dhtNode) load() {

}

// CreateNextBlock creates a new block with the next block number, and the given contents.
func (dhtn *dhtNode) PreCreateNextBlock(messages []*bridge.Envelope) *bridge.Block {
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
	block := NewBlock(0, emptyPrevHash)
	block.Header.DataHash = BlockDataHash(data)
	block.Data = data

	return block
}

func BlockDataHash(b *bridge.BlockData) []byte {
	sum := sha256.Sum256(bytes.Join(b.Data, nil))
	return sum[:]
}

func NewBlock(seqNum uint64, previousHash []byte) *bridge.Block {
	block := &bridge.Block{}
	block.Header = &bridge.BlockHeader{}
	block.Header.Number = seqNum
	block.Header.PreviousHash = previousHash
	block.Header.DataHash = []byte{}
	block.Data = &bridge.BlockData{}

	var metadataContents [][]byte
	for i := 0; i < len(bridge.BlockMetadataIndex_name); i++ {
		metadataContents = append(metadataContents, []byte{})
	}
	block.Metadata = &bridge.BlockMetadata{Metadata: metadataContents}

	return block
}

// Ordered should be invoked sequentially as messages are ordered
//
// messageBatches length: 0, pending: false
//   - impossible, as we have just received a message
// messageBatches length: 0, pending: true
//   - no batch is cut and there are messages pending
// messageBatches length: 1, pending: false
//   - the message count reaches BatchSize.MaxMessageCount
// messageBatches length: 1, pending: true
//   - the current message will cause the pending batch size in bytes to exceed BatchSize.PreferredMaxBytes.
// messageBatches length: 2, pending: false
//   - the current message size in bytes exceeds BatchSize.PreferredMaxBytes, therefore isolated in its own batch.
// messageBatches length: 2, pending: true
//   - impossible
//
// Note that messageBatches can not be greater than 2.
func (dhtn *dhtNode) Ordered(msg *bridge.Envelope) (messageBatches [][]*bridge.Envelope, pending bool) {
	if len(dhtn.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		dhtn.PendingBatchStartTime = time.Now()
	}

	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > dhtn.dhtConfig.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, dhtn.dhtConfig.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(dhtn.pendingBatch) > 0 {
			messageBatch := dhtn.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*bridge.Envelope{msg})

		// Record that this batch took no time to fill
		dhtn.Metrics.BlockFillDuration.With("channel", dhtn.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := dhtn.pendingBatchSizeBytes+messageSizeBytes > dhtn.dhtConfig.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, dhtn.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := dhtn.Cut()
		dhtn.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	dhtn.pendingBatch = append(dhtn.pendingBatch, msg)
	dhtn.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(dhtn.pendingBatch)) >= dhtn.dhtConfig.MaxMessageCount {
		logger.Debugf("Batch size met, cutting batch")
		messageBatch := dhtn.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut returns the current batch and starts a new one
func (dhtn *dhtNode) Cut() []*bridge.Envelope {
	if dhtn.pendingBatch != nil {
		dhtn.Metrics.BlockFillDuration.With("channel", dhtn.ChannelID).Observe(time.Since(dhtn.PendingBatchStartTime).Seconds())
	}
	dhtn.PendingBatchStartTime = time.Time{}
	batch := dhtn.pendingBatch
	dhtn.pendingBatch = nil
	dhtn.pendingBatchSizeBytes = 0
	return batch
}

func messageSizeBytes(message *bridge.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
