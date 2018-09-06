package chord

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"math/big"
	"math/rand"
	"time"
)

var (
	ERR_NO_SUCCESSOR = errors.New("cannot find successor")
	ERR_NODE_EXISTS  = errors.New("node with id already exists")
)

func randStabilize(min, max time.Duration) time.Duration {
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

// check if key is between a and b, right inclusive
func betweenRightIncl(key, a, b []byte) bool {
	return between(key, a, b) || bytes.Equal(key, b)
}

// Checks if a key is STRICTLY between two ID's exclusively
func between(key, a, b []byte) bool {
	switch bytes.Compare(a, b) {
	case 1:
		return bytes.Compare(a, key) == -1 || bytes.Compare(b, key) >= 0
	case -1:
		return bytes.Compare(a, key) == -1 && bytes.Compare(b, key) >= 0
	case 0:
		return bytes.Compare(a, key) != 0
	}
	return false
}

// hashKey hashes a string to its appropriate size.
func hashKey(key string) ([]byte, error) {
	h := sha1.New()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	v := h.Sum(nil)

	return v[:8], nil
}

// NewID takes a string representing
func NewID(str string) ([]byte, error) {
	i := big.NewInt(0)
	i.SetString(str, 0)
	id := i.Bytes()
	if len(id) == 0 {
		return nil, errors.New("invalid ID")
	}

	return padID(id), nil
}

func padID(id []byte) []byte {
	n := 8 - len(id)
	if n < 0 {
		n = 0
	}

	_id := make([]byte, n)
	id = append(_id, id...)

	return id[:8]
}

// idsEqual returns if a and b are equal.
func idsEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}
