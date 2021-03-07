package chord

import cm "github.com/zebra-uestc/chord/models/chord"

type Storage interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
	Delete([]byte) error
	Between([]byte, []byte) ([]*cm.KV, error)
	MDelete(...[]byte) error
}
