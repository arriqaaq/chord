package chord

import (
	"math/rand"
	"time"
)

func randStabilize(min, max time.Duration) time.Duration {
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}
