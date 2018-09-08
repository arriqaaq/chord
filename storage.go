package chord

import (
	"github.com/arriqaaq/chord/internal"
	"hash"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
	Between([]byte, []byte) ([]*internal.KV, error)
	MDelete(...string) error
}

func NewArrayStore(hashFunc hash.Hash) Storage {
	return &arrayStore{
		data: make(map[string]string),
		Hash: hashFunc,
	}
}

type arrayStore struct {
	data map[string]string
	Hash hash.Hash // Hash function to use

}

func (a *arrayStore) hashKey(key string) ([]byte, error) {
	h := a.Hash
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	h.Reset()
	return val, nil
}

func (a *arrayStore) Get(key string) ([]byte, error) {
	val, ok := a.data[key]
	if !ok {
		return nil, ERR_KEY_NOT_FOUND
	}
	return []byte(val), nil
}

func (a *arrayStore) Set(key, value string) error {
	a.data[key] = value
	return nil
}

func (a *arrayStore) Delete(key string) error {
	delete(a.data, key)
	return nil
}

func (a *arrayStore) Between(from []byte, to []byte) ([]*internal.KV, error) {
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

func (a *arrayStore) MDelete(keys ...string) error {
	for _, k := range keys {
		delete(a.data, k)
	}
	return nil
}
