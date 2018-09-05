package chord

import (
	"bytes"
	"errors"
	"math/rand"
	"time"
)

var (
	ERR_NO_SUCCESSOR = errors.New("cannot find successor")
)

func randStabilize(min, max time.Duration) time.Duration {
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

// check if key is between a and b, right inclusive
func betweenRightIncl(key, a, b []byte) bool {
	if bytes.Compare(a, b) == 1 {
		return bytes.Compare(a, key) == -1 || bytes.Compare(b, key) >= 0
	}
	return bytes.Compare(a, key) == -1 && bytes.Compare(b, key) >= 0
}

// Checks if a key is STRICTLY between two ID's exclusively
func between(key, a, b []byte) bool {
	// Check for ring wrap around
	if bytes.Compare(a, b) == 1 {
		return bytes.Compare(a, key) == -1 ||
			bytes.Compare(b, key) == 1
	}

	// Handle the normal case
	return bytes.Compare(a, key) == -1 &&
		bytes.Compare(b, key) == 1
}
