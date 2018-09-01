package chord

import (
	"github.com/arriqaaq/chord/internal"
	"google.golang.org/grpc"
	"hash"
	"sync"
	"time"
)

type Config struct {
	id           []byte
	addr         string
	serverOpts   []grpc.ServerOption
	dialOpts     []grpc.DialOption
	HashFunc     func() hash.Hash // Hash function to use
	StabilizeMin time.Duration    // Minimum stabilization time
	StabilizeMax time.Duration    // Maximum stabilization time
	Timeout      time.Duration
	MaxIdle      time.Duration
}

type Storage interface {
	Get(string) string
	Put(string, string) error
}

type Node struct {
	*internal.Node

	predecessor *internal.Node
	predMtx     sync.RWMutex

	successor *internal.Node
	succMtx   sync.RWMutex

	shutdownCh chan struct{}

	fingerTable fingerTable
	ftMtx       sync.RWMutex

	storage Storage
	stMtx   sync.RWMutex

	transport internal.ChordClient
	tsMtx     sync.RWMutex

	lastStablized time.Time
}
