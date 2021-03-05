package dhtnode

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/protobuf/proto"

	"./bridge"
)

var logger = flogging.MustGetLogger("orderer.consensus.dht")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

type message struct {
	configSeq uint64
	normalMsg *bridge.Envelope
	configMsg *bridge.Envelope
}

type dht_node struct {
	//*bridge.UnimplementedChordServer

	blockPivot   bool // 是否为node0
	pivotAddress string
	// node0独有
	//lastblock    *bridge.Block      // bw.lastBlock.Header.Number, previousBlockHash
	//preblockChan chan *bridge.Block //in
	//blockChan    chan *bridge.Block // out
	//// 加工block用
	//sendChan chan *message // in
	//exitChan chan struct{}
	//retChan  chan *message // out
	//
	//// 以下各个需要从orderer获取，如何获取？？？
	//sharedConfigFetcher   OrdererConfigFetcher
	//pendingBatch          []*bridge.Envelope
	//pendingBatchSizeBytes uint32
	//
	//PendingBatchStartTime time.Time
	//ChannelID             string
	//Metrics               *Metrics
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given dht_node (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger

func new_dht_node(bp bool) *dht_node {
	return &dht_node{
		blockPivot:   bp,
		pivotAddress: "",
		sendChan:     make(chan *message),
		exitChan:     make(chan struct{}),
		retChan:      make(chan *message),
		preblockChan: make(chan *bridge.Block),
		blockChan:    make(chan *bridge.Block),
	}
}

func (ch *dht_node) Start(bp bool) {
	new_dht_node(bp)
	// 启动node
	go ch.main()
	// 判断是否为node0，并启动pivot
	for ch.blockPivot == false {
	}
	go ch.StartPivot()

}

func (dn *dht_node) StartPivot() {
	go dn.CreateNextBlock()
	go dn.ReturnBlock()
}

func (ch *dht_node) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *dht_node) WaitReady() error {
	return nil
}

// Errored only closes on exit
func (ch *dht_node) Errored() <-chan struct{} {
	return ch.exitChan
}

func (ch *dht_node) main() {
	var timer <-chan time.Time
	var err error

	for {
		// seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.sendChan:
			if msg.configMsg == nil {
				// NormalMsg

				// // ProcessMsg必须放在orderer节点上做

				// if msg.configSeq < seq {
				// 	_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
				// 	if err != nil {
				// 		logger.Warningf("Discarding bad normal message: %s", err)
				// 		continue
				// 	}
				// }

				// 将要打包成一个块的msg存到一个batch里
				batches, pending := ch.Ordered(msg.normalMsg)
				// 修改Pre打包逻辑
				for _, batch := range batches {
					// block := ch.support.CreateNextBlock(batch)
					// ch.support.WriteBlock(block, nil)
					block := ch.PreCreateNextBlock(batch)
					ch.retChan <- block
				}

				switch {
				case timer != nil && !pending:
					// Timer is already running but there are no messages pending, stop the timer
					timer = nil
				case timer == nil && pending:
					// Timer is not already running and there are messages pending, so start it
					timer = time.After(ch.support.SharedConfig().BatchTimeout())
					logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
				default:
					// Do nothing when:
					// 1. Timer is already running and there are messages pending
					// 2. Timer is not set and there are no messages pending
				}

			} else {
				// ConfigMsg
				// if msg.configSeq < seq {
				// 	msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
				// 	if err != nil {
				// 		logger.Warningf("Discarding bad config message: %s", err)
				// 		continue
				// 	}
				// }
				batch := ch.Cut()
				if batch != nil {
					// block := ch.support.CreateNextBlock(batch)
					block := ch.PreCreateNextBlock(batch)
					// ch.support.WriteBlock(block, nil)
					ch.retChan <- block
				}

				block := ch.PreCreateNextBlock([]*bridge.Envelope{msg.configMsg})
				ch.retChan <- block
				// ch.support.WriteConfigBlock(block, nil)
				timer = nil
			}
		case <-timer:
			//clear the timer
			timer = nil

			batch := ch.Cut()
			if len(batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			block := ch.PreCreateNextBlock(batch)
			ch.retChan <- block
			// ch.support.WriteBlock(block, nil)
		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}

// CreateNextBlock creates a new block with the next block number, and the given contents.
func (dn *dht_node) PreCreateNextBlock(messages []*bridge.Envelope) *bridge.Block {
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

	// protoutiles/blockutiles.go
	// 	// NewBlock constructs a block with no data and no metadata.
	// func NewBlock(seqNum uint64, previousHash []byte) *bridge.Block {
	// 	block := &bridge.Block{}
	// 	block.Header = &bridge.BlockHeader{}
	// 	block.Header.Number = seqNum
	// 	block.Header.PreviousHash = previousHash
	// 	block.Header.DataHash = []byte{}
	// 	block.Data = &bridge.BlockData{}

	// 	var metadataContents [][]byte
	// 	for i := 0; i < len(bridge.BlockMetadataIndex_name); i++ {
	// 		metadataContents = append(metadataContents, []byte{})
	// 	}
	// 	block.Metadata = &bridge.BlockMetadata{Metadata: metadataContents}

	// 	return block
	// }

	// block := protoutil.NewBlock(bw.lastBlock.Header.Number+1, previousBlockHash)
	block := protoutil.NewBlock(0, 0)
	block.Header.DataHash = protoutil.BlockDataHash(data)
	block.Data = data

	return block
}

// CreateNextBlock creates a new block with the next block number, and the given contents.
func (dn *dht_node) CreateNextBlock() error {
	previousBlockHash := protoutil.BlockHeaderHash(dn.lastblock.Header)
	// block := protoutil.NewBlock(0, 0) // 改
	var block *bridge.Block
	for {
		select {
		case block <- dn.preblockChan:
			if dn.lastblock != nil {
				block.Header.Number = dn.lastblock.Header.Number + 1 // seqNum
				block.Header.PreviousHash = previousBlockHash        // previousHash
			} else {
				block.Header.Number = 0       // seqNum
				block.Header.PreviousHash = 0 // previousHash
			}
			dn.lastblock = block
			dn.blockChan <- block
		default:

		}
	}
	return nil
}

// msg: dht上某个node -> preblock加工
func (dn *dht_node) ReceiveMsg() error {
	// orderer->dht环->ReceiveMsg()
	// 如何得到dht环node上的msg？？？

	// 转发到sendChan
	select {
	case dn.sendChan <- &message{
		configSeq: configSeq,
		normalMsg: env,
	}:
		return nil
	case <-dn.exitChan:
		return fmt.Errorf("Exiting")
	}

	return nil
}

// preblock: other nodes -> node0
func (dn *dht_node) SendBlock() error {
	var preblock *bridge.Block
	// 从retChan获得preblock
	select {
	case preblock <- dn.retChan:
		// 如何跨主机通讯，传输prevlock给node0？？？

		return nil
	case <-dn.exitChan:
		return fmt.Errorf("Exiting")
	}

	return nil
}

// block: node0 -> orderer
func (dn *dht_node) ReturnBlock() error {
	var block *bridge.Block
	for {
		select {
		case block <- dn.blockChan:
			// 把block转发给orderer
		default:

		}
	}
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
func (r *dht_node) Ordered(msg *bridge.Envelope) (messageBatches [][]*bridge.Envelope, pending bool) {
	if len(r.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		r.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()

	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingBatch) > 0 {
			messageBatch := r.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*bridge.Envelope{msg})

		// Record that this batch took no time to fill
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := r.Cut()
		r.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		logger.Debugf("Batch size met, cutting batch")
		messageBatch := r.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut returns the current batch and starts a new one
func (r *dht_node) Cut() []*bridge.Envelope {
	if r.pendingBatch != nil {
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	}
	r.PendingBatchStartTime = time.Time{}
	batch := r.pendingBatch
	r.pendingBatch = nil
	r.pendingBatchSizeBytes = 0
	return batch
}

func messageSizeBytes(message *bridge.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
