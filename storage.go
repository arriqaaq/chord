package chord

import (
	// "errors"
	"hash"

	"github.com/zebra-uestc/chord/models"
	// "math/big"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
	Between([]byte, []byte) ([]*models.KV, error)
	MDelete(...string) error
}

func NewMapStore(hashFunc func() hash.Hash) Storage {
	return &mapStore{
		data: make(map[string]string),
		Hash: hashFunc,
	}
}

// 存储key到数据的映射关系，和hash函数，即全部数据存储的数据结构
type mapStore struct {
	data map[string]string
	Hash func() hash.Hash // Hash function to use

}

// 计算key的hash值
func (a *mapStore) hashKey(key string) ([]byte, error) {
	h := a.Hash()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

// 返回存在的key的数据值
func (a *mapStore) Get(key string) ([]byte, error) {
	val, ok := a.data[key]
	if !ok {
		return nil, ERR_KEY_NOT_FOUND
	}
	return []byte(val), nil
}

// 指定key对应的数据值
func (a *mapStore) Set(key, value string) error {
	a.data[key] = value
	return nil
}

// 从mapStore的data中删除键为key的映射记录
func (a *mapStore) Delete(key string) error {
	delete(a.data, key)
	return nil
}

// 取出hash(key)值在from到to(左闭右闭区间)之间的数据键值对{key，value}，返回字典数组
func (a *mapStore) Between(from []byte, to []byte) ([]*models.KV, error) {
	// message KV {
	// 	string key = 1;
	// 	string value = 2;
	// }

	// 初始元素个数为0，元素初始值为0，预留10个元素存储单元
	vals := make([]*models.KV, 0, 10)
	for k, v := range a.data {
		hashedKey, err := a.hashKey(k)
		if err != nil {
			continue
		}
		// between(右闭区间)
		if betweenRightIncl(hashedKey, from, to) {
			pair := &models.KV{
				Key:   k,
				Value: v,
			}
			vals = append(vals, pair)
		}
	}
	return vals, nil
}

// ...string：可变长度参数列表：[]string
// 批量删除键值对
func (a *mapStore) MDelete(keys ...string) error {
	for _, k := range keys {
		delete(a.data, k)
	}
	return nil
}
