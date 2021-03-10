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

//TODO: BatchSize定义（从orderer获得？还是自定义？）
type preprocess struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          []*bridge.Envelope
	pendingBatchSizeBytes uint32
	MaxMessageCount       uint32
	AbsoluteMaxBytes      uint32
	PreferredMaxBytes     uint32
	PendingBatchStartTime time.Time
	ChannelID             string
	Metrics               *Metrics
	batchSize             BatchSize
}

//type BatchSize struct {
//	// Simply specified as number of messages for now, in the future
//	// we may want to allow this to be specified by size in bytes
//	MaxMessageCount uint32 = 500
//	// The byte count of the serialized messages in a batch cannot
//	// exceed this value.
//	AbsoluteMaxBytes uint32 = 10 MB
//	// The byte count of the serialized messages in a batch should not
//	// exceed this value.
//	PreferredMaxBytes    uint32   2 MB
//
//}

func (pr *preprocess) load() {

}

// CreateNextBlock creates a new block with the next block number, and the given contents.
func (pr *preprocess) PreCreateNextBlock(messages []*bridge.Envelope) *bridge.Block {
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
func (pr *preprocess) Ordered(msg *bridge.Envelope) (messageBatches [][]*bridge.Envelope, pending bool) {
	if len(pr.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		pr.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := pr.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()

	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(pr.pendingBatch) > 0 {
			messageBatch := pr.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*bridge.Envelope{msg})

		// Record that this batch took no time to fill
		pr.Metrics.BlockFillDuration.With("channel", pr.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := pr.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, pr.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := pr.Cut()
		pr.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	pr.pendingBatch = append(pr.pendingBatch, msg)
	pr.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(pr.pendingBatch)) >= batchSize.MaxMessageCount {
		logger.Debugf("Batch size met, cutting batch")
		messageBatch := pr.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut returns the current batch and starts a new one
func (pr *preprocess) Cut() []*bridge.Envelope {
	if pr.pendingBatch != nil {
		pr.Metrics.BlockFillDuration.With("channel", pr.ChannelID).Observe(time.Since(pr.PendingBatchStartTime).Seconds())
	}
	pr.PendingBatchStartTime = time.Time{}
	batch := pr.pendingBatch
	pr.pendingBatch = nil
	pr.pendingBatchSizeBytes = 0
	return batch
}

func messageSizeBytes(message *bridge.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
