package chord

import (
	"errors"
	"hash"

	bm "github.com/zebra-uestc/chord/models/bridge"
	cm "github.com/zebra-uestc/chord/models/chord"
	"google.golang.org/protobuf/proto"
)

var emptyMethodError = errors.New("Not Implemented Method")

type TxStorage interface {
	Storage
	GetMsgChan() chan *bm.Msg
}

func NewtxStorage(hashFunc func() hash.Hash) TxStorage {
	return &txStorage{}
}

type txStorage struct {
	setMsgChan chan *bm.Msg
}

func (txs *txStorage) Set(key []byte, value []byte) error {
	var msg *bm.Msg
	if err := proto.Unmarshal(value, msg); err != nil{
		return err
	}
	txs.setMsgChan <- msg
	return nil
}

func (txs *txStorage) GetMsgChan() chan *bm.Msg {
	return txs.setMsgChan
}

func (*txStorage) Get([]byte) ([]byte, error) {
	return nil, emptyMethodError
}
func (*txStorage) Delete([]byte) error {
	return emptyMethodError
}
func (*txStorage) Between([]byte, []byte) ([]*cm.KV, error) {
	return nil, emptyMethodError
}
func (*txStorage) MDelete(...[]byte) error {
	return emptyMethodError
}
