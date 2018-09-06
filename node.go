package chord

import (
	"fmt"
	"github.com/arriqaaq/chord/internal"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"hash"
	"sync"
	"time"
)

func DefaultConfig() *Config {
	return &Config{
		ServerOpts: make([]grpc.ServerOption, 1),
		DialOpts:   make([]grpc.DialOption, 1),
	}
}

type Config struct {
	Id           []byte
	Addr         string
	ServerOpts   []grpc.ServerOption
	DialOpts     []grpc.DialOption
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

func NewInode(id []byte, addr string) *internal.Node {
	return &internal.Node{
		Id:   id,
		Addr: addr,
	}
}

// NewNode creates a Chord node with a pre-defined ID (useful for
// testing) if a non-nil id is provided.
func NewNode(cnf *Config, sister *internal.Node) (*Node, error) {
	node := &Node{
		Node:       new(internal.Node),
		shutdownCh: make(chan struct{}),
	}

	if cnf.Id != nil {
		node.Node.Id = cnf.Id
	} else {
		id, err := hashKey(cnf.Addr)
		if err != nil {
			return nil, err
		}
		node.Node.Id = id
	}
	node.Node.Addr = cnf.Addr

	// Populate finger table
	node.fingerTable = newFingerTable(node.Node)

	// Start RPC server
	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	internal.RegisterChordServer(transport.server, node)

	node.transport.Start()

	// Join this node to the same chord ring as parent
	var joinNode *internal.Node
	// // Ask if our id exists on the ring.
	if sister != nil {
		remoteNode, err := node.findSuccessorRPC(sister)
		if err != nil {
			return nil, err
		}

		if idsEqual(remoteNode.Id, node.Id) {
			return nil, ERR_NODE_EXISTS
		}
		joinNode = sister
	} else {
		joinNode = node.Node
	}

	if err := node.join(joinNode); err != nil {
		return nil, err
	}

	return node, nil
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

// join allows this node to join an existing ring that a remote node
// is a part of (i.e., other).
func (n *Node) join(other *internal.Node) error {
	succ, err := n.findSuccessorRPC(other)
	if err != nil {
		return err
	}
	fmt.Println("found succ for, ", n.Id, succ.Id)
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()

	return nil
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
	if succ == nil {
		return curr, nil
	}

	if betweenRightIncl(id, curr.Id, succ.Id) {
		// fmt.Println("1ad", n.Id, id, curr.Id, succ.Id)
		return succ, nil
	} else {
		pred := n.closestPrecedingNode(id)
		succ, err := n.getSuccessorRPC(pred)
		// fmt.Println("2ad", n.Id, id, pred.Id, succ.Id, betweenRightIncl(id, pred.Id, succ.Id))
		if err != nil {
			return nil, err
		}
		if succ == nil {
			// not able to wrap around, current node is the successor
			return curr, nil
		}
		return succ, nil

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

//TODO
func (n *Node) ClosestPrecedingFinger(ctx context.Context, node *internal.ID) (*internal.Node, error) {
	return nil, nil
}

//TODO
func (n *Node) GetPredecessor(ctx context.Context, r *internal.ER) (*internal.Node, error) {
	return nil, nil
}

//TODO
func (n *Node) Notify(ctx context.Context, node *internal.Node) (*internal.ER, error) {
	return nil, nil
}
