package chord

import (
	// "errors"
	"github.com/arriqaaq/chord/internal"
	"hash"
	// "math/big"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
	Between([]byte, []byte) ([]*internal.KV, error)
	MDelete(...string) error
}

func NewMapStore(hashFunc func() hash.Hash) Storage {
	return &mapStore{
		data: make(map[string]string),
		Hash: hashFunc,
	}
}

type mapStore struct {
	data map[string]string
	Hash func() hash.Hash // Hash function to use

}

func (a *mapStore) hashKey(key string) ([]byte, error) {
	h := a.Hash()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

func (a *mapStore) Get(key string) ([]byte, error) {
	val, ok := a.data[key]
	if !ok {
		return nil, ERR_KEY_NOT_FOUND
	}
	return []byte(val), nil
}

func (a *mapStore) Set(key, value string) error {
	a.data[key] = value
	return nil
}

func (a *mapStore) Delete(key string) error {
	delete(a.data, key)
	return nil
}

func (a *mapStore) Between(from []byte, to []byte) ([]*internal.KV, error) {
	vals := make([]*internal.KV, 0, 10)
	for k, v := range a.data {
		hashedKey, err := a.hashKey(k)
		if err != nil {
			continue
		}
		if betweenRightIncl(hashedKey, from, to) {
			pair := &internal.KV{
				Key:   k,
				Value: v,
			}
			vals = append(vals, pair)
		}
	}
	return vals, nil
}

func (a *mapStore) MDelete(keys ...string) error {
	for _, k := range keys {
		delete(a.data, k)
	}
	return nil
}
