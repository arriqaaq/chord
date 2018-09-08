package chord

import (
	"crypto/sha1"
	"fmt"
	"github.com/arriqaaq/chord/internal"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"hash"
	"sync"
	"time"
)

func DefaultConfig() *Config {
	n := &Config{
		Hash:     sha1.New(),
		DialOpts: make([]grpc.DialOption, 0, 5),
	}
	n.HashSize = n.Hash.Size() * 10
	n.DialOpts = append(n.DialOpts,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
		grpc.FailOnNonTempDialError(true),
		grpc.WithInsecure(),
	)
	return n
}

type Config struct {
	Id           []byte
	Addr         string
	ServerOpts   []grpc.ServerOption
	DialOpts     []grpc.DialOption
	Hash         hash.Hash // Hash function to use
	HashSize     int
	StabilizeMin time.Duration // Minimum stabilization time
	StabilizeMax time.Duration // Maximum stabilization time
	Timeout      time.Duration
	MaxIdle      time.Duration
}

func (c *Config) Validate() error {
	// hashsize shouldnt be less than hash func size
	return nil
}

func NewInode(id []byte, addr string) *internal.Node {
	return &internal.Node{
		Id:   id,
		Addr: addr,
	}
}

/*
	NewNode creates a new Chord node. Returns error if node already
	exists in the chord ring
*/
func NewNode(cnf *Config, joinNode *internal.Node) (*Node, error) {
	if err := cnf.Validate(); err != nil {
		return nil, err
	}
	node := &Node{
		Node:       new(internal.Node),
		shutdownCh: make(chan struct{}),
		cnf:        cnf,
		storage:    NewArrayStore(cnf.Hash),
	}

	if cnf.Id != nil {
		node.Node.Id = cnf.Id
	} else {
		id, err := node.hashKey(cnf.Addr)
		if err != nil {
			return nil, err
		}
		node.Node.Id = id
	}
	node.Node.Addr = cnf.Addr

	// Populate finger table
	node.fingerTable = newFingerTable(node.Node, cnf.HashSize)

	// Start RPC server
	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	internal.RegisterChordServer(transport.server, node)

	node.transport.Start()

	if err := node.join(joinNode); err != nil {
		return nil, err
	}

	// Peridoically stabilize the node.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.stabilize()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Peridoically fix finger tables.
	go func() {
		next := 0
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				next = node.fixFinger(next)
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Peridoically checkes whether predecessor has failed.

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.checkPredecessor()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	return node, nil
}

type Node struct {
	*internal.Node

	cnf *Config

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

func (n *Node) join(joinNode *internal.Node) error {
	// First check if node already present in the circle
	// Join this node to the same chord ring as parent
	var foo *internal.Node
	// // Ask if our id already exists on the ring.
	if joinNode != nil {
		remoteNode, err := n.findSuccessorRPC(joinNode, n.Id)
		if err != nil {
			return err
		}

		if isEqual(remoteNode.Id, n.Id) {
			return ERR_NODE_EXISTS
		}
		foo = joinNode
		// fmt.Println("got sister", n.Id, foo.Id)
	} else {
		foo = n.Node
	}

	succ, err := n.findSuccessorRPC(foo, n.Id)
	if err != nil {
		return err
	}
	// fmt.Println("found succ for, ", n.Id, succ.Id)
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()

	// request keys from parent node
	n.transferKeys()

	return nil
}

/*
	Public storage implementation
*/

func (n *Node) Find(key string) (*internal.Node, error) {
	return n.locate(key)
}

func (n *Node) Get(key string) ([]byte, error) {
	return n.get(key)
}
func (n *Node) Set(key, value string) error {
	return n.set(key, value)
}
func (n *Node) Delete(key string) error {
	return n.delete(key)
}

/*
	Finds the node for the key
*/
func (n *Node) locate(key string) (*internal.Node, error) {
	id, err := n.hashKey(key)
	if err != nil {
		return nil, err
	}
	return n.findSuccessor(id)
}

func (n *Node) get(key string) ([]byte, error) {
	node, err := n.locate(key)
	if err != nil {
		return nil, err
	}
	val, err := n.getKeyRPC(node, key)
	if err != nil {
		return nil, err
	}
	return val.Value, nil
}

func (n *Node) set(key, value string) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.setKeyRPC(node, key, value)
	return err
}

func (n *Node) delete(key string) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.deleteKeyRPC(node, key)
	return err
}

func (n *Node) transferKeys() {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	keys, err := n.requestKeys(succ)
	if len(keys) > 0 {
		fmt.Println("transfering: ", keys, err)
	}
	delKeyList := make([]string, 0, 10)
	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		n.storage.Set(item.Key, item.Value)
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from the other node
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}

}

func (n *Node) deleteKeys(node *internal.Node, keys []string) error {
	return n.deleteKeysRPC(node, keys)
}

// When a new node joins, it requests keys from it's successor
func (n *Node) requestKeys(succ *internal.Node) ([]*internal.KV, error) {

	/*
		Get successor's predecessor, as current node is new
		and predecessor is initally nil
	*/
	pred, err := n.getPredecessorRPC(succ)
	if err != nil {
		return nil, err
	}

	if isEqual(n.Id, pred.Id) {
		return nil, nil
	}

	return n.requestKeysRPC(
		succ, pred.Id, n.Id,
	)
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
		//9ad [2] id:"\003" addr:"0.0.0.0:8002"  [3]
		pred := n.closestPrecedingNode(id)
		// fmt.Println("closest node ", n.Id, id, pred.Id)
		/*
			NOT SURE ABOUT THIS, RECHECK from paper!!!
			if preceeding node and current node are the same,
			store the key on this node
		*/
		if isEqual(pred.Id, n.Id) {
			return curr, nil
		}

		succ, err := n.findSuccessorRPC(pred, id)
		// fmt.Println("successor to closest node ", succ, err)
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

/*
	Periodic functions implementation
*/

func (n *Node) stabilize() {
	// fmt.Println("stabilize: ", n.Id, n.successor, n.predecessor)
	n.succMtx.RLock()
	succ := n.successor
	if succ == nil {
		n.succMtx.RUnlock()
		return
	}
	n.succMtx.RUnlock()

	x, err := n.getPredecessorRPC(succ)
	if err != nil || x == nil {
		fmt.Println("error getting predecessor, ", err, x)
		return
	}
	if x.Id != nil && between(x.Id, n.Id, succ.Id) {
		n.succMtx.Lock()
		n.successor = x
		n.succMtx.Unlock()
		// fmt.Println("setting successor ", n.Id, x.Id)
	}
	n.notifyRPC(succ, n.Node)
}

// called periodically. refreshes finger table entries.
// next stores the index of the next finger to fix.
func (n *Node) fixFinger(next int) int {
	nextHash := fingerID(n.Id, next, n.cnf.HashSize)
	succ, err := n.findSuccessor(nextHash)
	nextNum := (next + 1) % n.cnf.HashSize
	if err != nil || succ == nil {
		fmt.Println("finger lookup failed", n.Id, nextHash)
		// TODO: this will keep retrying, check what to do
		// return next
		return nextNum
	}

	finger := newFingerEntry(nextHash, succ)
	n.ftMtx.Lock()
	n.fingerTable[next] = finger
	// for _, v := range n.fingerTable {
	// 	fmt.Println("finger data ", n.Id, v.Id, v.Node.Id)
	// }
	n.ftMtx.Unlock()

	return nextNum
}

func (n *Node) checkPredecessor() {
	// implement using rpc func
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	err := n.transport.CheckPredecessor(pred)

	if err != nil {
		fmt.Println("predecessor failed!")
		n.predMtx.Lock()
		n.predecessor = nil
		n.predMtx.Unlock()
	}
}

/*
	RPC callers implementation
*/

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getSuccessorRPC(node *internal.Node) (*internal.Node, error) {
	return n.transport.GetSuccessor(node)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *internal.Node, id []byte) (*internal.Node, error) {
	return n.transport.FindSuccessor(node, id)
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getPredecessorRPC(node *internal.Node) (*internal.Node, error) {
	return n.transport.GetPredecessor(node)
}

// notifyRPC notifies a remote node that pred is its predecessor.
func (n *Node) notifyRPC(node, pred *internal.Node) error {
	return n.transport.Notify(node, pred)
}

func (n *Node) getKeyRPC(node *internal.Node, key string) (*internal.GetResponse, error) {
	return n.transport.GetKey(node, key)
}
func (n *Node) setKeyRPC(node *internal.Node, key, value string) error {
	return n.transport.SetKey(node, key, value)
}
func (n *Node) deleteKeyRPC(node *internal.Node, key string) error {
	return n.transport.DeleteKey(node, key)
}

func (n *Node) requestKeysRPC(
	node *internal.Node, from []byte, to []byte,
) ([]*internal.KV, error) {
	return n.transport.RequestKeys(node, from, to)
}

func (n *Node) deleteKeysRPC(
	node *internal.Node, keys []string,
) error {
	return n.transport.DeleteKeys(node, keys)
}

/*
	RPC interface implementation
*/

// GetSuccessor gets the successor on the node..
func (n *Node) GetSuccessor(ctx context.Context, r *internal.ER) (*internal.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()
	if succ == nil {
		return emptyNode, nil
	}

	return succ, nil
}

func (n *Node) FindSuccessor(ctx context.Context, id *internal.ID) (*internal.Node, error) {
	succ, err := n.findSuccessor(id.Id)
	if err != nil {
		return nil, err
	}

	if succ == nil {
		return nil, ERR_NO_SUCCESSOR
	}

	return succ, nil

}

func (n *Node) CheckPredecessor(ctx context.Context, id *internal.ID) (*internal.ER, error) {
	return emptyRequest, nil
}

func (n *Node) GetPredecessor(ctx context.Context, r *internal.ER) (*internal.Node, error) {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()
	if pred == nil {
		return emptyNode, nil
	}
	return pred, nil
}

func (n *Node) Notify(ctx context.Context, node *internal.Node) (*internal.ER, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()
	pred := n.predecessor
	if pred == nil || between(node.Id, pred.Id, n.Id) {
		// fmt.Println("setting predecessor", n.Id, node.Id)
		n.predecessor = node
	}
	return emptyRequest, nil
}

func (n *Node) XGet(ctx context.Context, req *internal.GetRequest) (*internal.GetResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Get(req.Key)
	if err != nil {
		return emptyGetResponse, err
	}
	return &internal.GetResponse{Value: val}, nil
}

func (n *Node) XSet(ctx context.Context, req *internal.SetRequest) (*internal.SetResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.Set(req.Key, req.Value)
	return emptySetResponse, err
}

func (n *Node) XDelete(ctx context.Context, req *internal.DeleteRequest) (*internal.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.Delete(req.Key)
	return emptyDeleteResponse, err
}

func (n *Node) XRequestKeys(ctx context.Context, req *internal.RequestKeysRequest) (*internal.RequestKeysResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Between(req.From, req.To)
	if err != nil {
		return emptyRequestKeysResponse, err
	}
	return &internal.RequestKeysResponse{Values: val}, nil
}

func (n *Node) XMultiDelete(ctx context.Context, req *internal.MultiDeleteRequest) (*internal.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.MDelete(req.Keys...)
	return emptyDeleteResponse, err
}
