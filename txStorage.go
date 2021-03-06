package chord

import (
	"errors"
	bm "github.com/zebra-uestc/chord/dhtnode/bridge"
	"github.com/zebra-uestc/chord/models"
	"hash"
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

func (txs *txStorage) Set(key string, value []byte) error {
	txs.setMsgChan <- value
	return nil
}

func (txs *txStorage) GetMsgChan() chan *bm.Msg {
	return txs.setMsgChan
}

func (*txStorage) Get(string) ([]byte, error) {
	return nil, emptyMethodError
}
func (*txStorage) Delete(string) error {
	return emptyMethodError
}
func (*txStorage) Between([]byte, []byte) ([]*models.KV, error) {
	return nil, emptyMethodError
}
func (*txStorage) MDelete(...string) error {
	return emptyMethodError
}
