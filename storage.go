package chord

import (
	bm "github.com/zebra-uestc/chord/dhtnode/bridge"
	"github.com/zebra-uestc/chord/models"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, []byte) error
	Delete(string) error
	Between([]byte, []byte) ([]*models.KV, error)
	MDelete(...string) error
}
