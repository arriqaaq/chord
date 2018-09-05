package chord

import (
	"github.com/arriqaaq/chord/internal"
	"golang.org/x/net/context"
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

	transport Transport
	tsMtx     sync.RWMutex

	lastStablized time.Time
}

/*
	Fig 5 implementation for find_succesor
	First check if key present in local table, if not
	then look for how to travel in the ring
*/
func (n *Node) findSuccessor(id []byte) (*internal.Node, error) {
	n.succMtx.RLock()
	defer n.succMtx.RUnlock()

	curr := n.Node
	succ := n.successor
	if betweenRightIncl(id, curr.Id, succ.Id) {
		return succ, nil
	} else {
		prec := n.closestPrecedingNode(id)
		succ, err := n.findSuccessorRPC(prec)
		if err != nil {
			return nil, err
		}
		if succ == nil {
			// not able to wrap around, current node is the successor
			return curr, nil
		}

	}
	return nil, nil
}

// Fig 5 implementation for closest_preceding_node
func (n *Node) closestPrecedingNode(id []byte) *internal.Node {
	n.predMtx.RLock()
	defer n.predMtx.RUnlock()

	curr := n.Node

	m := len(n.fingerTable) - 1
	for i := m; i >= 0; i-- {
		f := n.fingerTable[i]
		if f == nil || f.Node == nil {
			continue
		}
		if between(f.Id, curr.Id, id) {
			return f.Node
		}
	}
	return curr
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getSuccessorRPC(node *internal.Node) (*internal.Node, error) {
	return n.transport.GetSuccessor(node)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *internal.Node) (*internal.Node, error) {
	return n.transport.FindSuccessor(node)
}

/*
	RPC interface implementation
*/

// GetSuccessor gets the successor on the node..
func (n *Node) GetSuccessor(ctx context.Context, r *internal.ER) (*internal.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	return succ, nil
}

func (n *Node) FindSuccessor(ctx context.Context, node *internal.ID) (*internal.Node, error) {

	succ, err := n.findSuccessor(node.Id)
	if err != nil {
		return nil, err
	}

	if succ == nil {
		return nil, ERR_NO_SUCCESSOR
	}

	return succ, nil

}
