package chord

import (
	"crypto/sha1"
	"fmt"
	"hash"
	"math/big"
	"sync"
	"time"

	"github.com/arriqaaq/chord/models"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func DefaultConfig() *Config {
	n := &Config{
		Hash:     sha1.New,
		DialOpts: make([]grpc.DialOption, 0, 5),
	}
	// n.HashSize = n.Hash().Size()
	n.HashSize = n.Hash().Size() * 8

	n.DialOpts = append(n.DialOpts,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
		grpc.FailOnNonTempDialError(true),
		grpc.WithInsecure(),
	)
	return n
}

// 一个Node一个Config
type Config struct {
	Id   string
	Addr string

	ServerOpts []grpc.ServerOption
	DialOpts   []grpc.DialOption

	Hash     func() hash.Hash // Hash function to use
	HashSize int

	StabilizeMin time.Duration // Minimum stabilization time
	StabilizeMax time.Duration // Maximum stabilization time

	Timeout time.Duration
	MaxIdle time.Duration
}

// hashsize是finger表的记录个数。。。
func (c *Config) Validate() error {
	// hashsize shouldnt be less than hash func size？？？
	return nil
}

//新建单个节点
func NewInode(id string, addr string) *models.Node {
	h := sha1.New()
	if _, err := h.Write([]byte(id)); err != nil {
		return nil
	}
	val := h.Sum(nil)

	return &models.Node{
		Id:   val,
		Addr: addr,
	}
}

/*
	NewNode creates a new Chord node. Returns error if node already
	exists in the chord ring
*/
//将单个节点(models.Node)按配置构造环上的节点(Node)，并加入chord环，启动该节点
func NewNode(cnf *Config, joinNode *models.Node) (*Node, error) {
	if err := cnf.Validate(); err != nil {
		return nil, err
	}
	node := &Node{
		Node:       new(models.Node),
		shutdownCh: make(chan struct{}),
		cnf:        cnf,
		storage:    NewMapStore(cnf.Hash),
	}

	var nID string
	if cnf.Id != "" {
		nID = cnf.Id
	} else {
		nID = cnf.Addr
	}
	id, err := node.hashKey(nID)
	if err != nil {
		return nil, err
	}
	aInt := (&big.Int{}).SetBytes(id)

	fmt.Printf("new node id %d, \n", aInt)

	node.Node.Id = id
	node.Node.Addr = cnf.Addr

	// Populate finger table
	node.fingerTable = newFingerTable(node.Node, cnf.HashSize)

	// Start RPC server
	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	models.RegisterChordServer(transport.server, node)

	node.transport.Start()

	// 找到joinNode的后继节点succ
	if err := node.join(joinNode); err != nil {
		return nil, err
	}

	// Peridoically stabilize the node.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
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
		ticker := time.NewTicker(100 * time.Millisecond)
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

// Go语言类型内嵌和结构体内嵌
// 外层结构体通过 outer.in1 直接进入内层结构体的字段，内嵌结构体甚至可以来自其他包。内层结构体被简单的插入或者内嵌进外层结构体。这个简单的“继承”机制提供了一种方式，使得可以从另外一个或一些类型继承部分或全部实现。
// 此处的Node为chord环上的节点，而protocal buffer中定义的Node为单个节点

// message Node {
//     bytes id = 1;
//     string addr = 2;
// }

type Node struct {
	*models.Node

	cnf *Config

	predecessor *models.Node
	predMtx     sync.RWMutex

	successor *models.Node
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

func (n *Node) hashKey(key string) ([]byte, error) {
	h := n.cnf.Hash()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

// n.findSuccessorRPC(joinNode, n.Id)？？？
// 把joinnode加入环？？？
func (n *Node) join(joinNode *models.Node) error {
	// First check if node already present in the circle
	// Join this node to the same chord ring as parent
	var foo *models.Node
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
	} else {
		foo = n.Node
	}

	succ, err := n.findSuccessorRPC(foo, n.Id)
	if err != nil {
		return err
	}
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()

	return nil
}

/*
	RPC流程：

	Client本地调用 ->
	node.go中对transport.go中RPC方法的封装 ->
	transport.go中的RPC方法 ->
	(网络传输)
	transport.go中的RPC方法 ->
	Server端对应函数 ->
	(返回)
*/

/*
	Public storage implementation
	Client本地调用
*/

func (n *Node) Find(key string) (*models.Node, error) {
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
func (n *Node) locate(key string) (*models.Node, error) {
	id, err := n.hashKey(key)
	if err != nil {
		return nil, err
	}
	succ, err := n.findSuccessor(id)
	return succ, err
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

// 将pred和succ节点之间的键值对转移给当前节点？不加判断？？？感觉不对
func (n *Node) transferKeys(pred, succ *models.Node) {

	keys, err := n.requestKeys(pred, succ)
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
	// delete the keys from the successor node, as current node
	// is responsible for the keys
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}

}

// 实现Node删除时将Node上的所有数据转移给其successor？？？逻辑不对
func (n *Node) moveKeysFromLocal(pred, succ *models.Node) {

	keys, err := n.storage.Between(pred.Id, succ.Id)
	if len(keys) > 0 {
		fmt.Println("transfering: ", keys, succ, err)
	}
	delKeyList := make([]string, 0, 10)
	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		err := n.setKeyRPC(succ, item.Key, item.Value)
		if err != nil {
			fmt.Println("error transfering key: ", item.Key, succ.Addr)
		}
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from the successor node, as current node
	// is responsible for the keys
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}

}

func (n *Node) deleteKeys(node *models.Node, keys []string) error {
	return n.deleteKeysRPC(node, keys)
}

// When a new node joins, it requests keys from it's successor
// 返回pred到succ之间所有键值对
func (n *Node) requestKeys(pred, succ *models.Node) ([]*models.KV, error) {

	if isEqual(n.Id, succ.Id) {
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

// 在finger表中找键值对应该放在哪个节点时，每次找到hash值包括节点hash的区间，取左边界hash映射的的节点，线判断键值对hash值是否在该节点到其后继节点之间：若是，则放后继节点；若否，则？？？感觉逻辑不对
func (n *Node) findSuccessor(id []byte) (*models.Node, error) {
	// Check if lock is needed throughout the process
	n.succMtx.RLock()
	defer n.succMtx.RUnlock()
	curr := n.Node
	succ := n.successor

	if succ == nil {
		return curr, nil
	}

	var err error

	if betweenRightIncl(id, curr.Id, succ.Id) {
		return succ, nil
	} else {
		pred := n.closestPrecedingNode(id)
		/*
			NOT SURE ABOUT THIS, RECHECK from paper!!!？？？
			if preceeding node and current node are the same,
			store the key on this node
		*/

		if isEqual(pred.Id, n.Id) {
			succ, err = n.getSuccessorRPC(pred)
			if err != nil {
				return nil, err
			}
			if succ == nil {
				// not able to wrap around, current node is the successor
				return pred, nil
			}
			return succ, nil
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
		return succ, nil

	}
	return nil, nil
}

// Fig 5 implementation for closest_preceding_node
// 返回当前节点finger表中不小于键值对hash值的最小id映射的节点？？？(找存储该键值对的节点的过程)
func (n *Node) closestPrecedingNode(id []byte) *models.Node {
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
// The stabilization protocol works as follows:
// Stabilize(): n asks its successor for its predecessor p and decides whether p should be n‘s successor instead (this is the case if p recently joined the system).
// Notify(): notifies n‘s successor of its existence, so it can change its predecessor to n
// Fix_fingers(): updates finger tables 这一步在New_node中做
func (n *Node) stabilize() {

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
	}
	n.notifyRPC(succ, n.Node)
}

// checkes whether predecessor has failed. Newnode()中周期调用
func (n *Node) checkPredecessor() {
	// implement using rpc func
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if pred != nil {
		err := n.transport.CheckPredecessor(pred)
		if err != nil {
			fmt.Println("predecessor failed!", err)
			n.predMtx.Lock()
			n.predecessor = nil
			n.predMtx.Unlock()
		}
	}
}

/*
	RPC callers implementation
	只是对transport包中的rpc方法进行封装(Client端调用)
*/
// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getSuccessorRPC(node *models.Node) (*models.Node, error) {
	return n.transport.GetSuccessor(node)
}

// setSuccessorRPC sets the successor of a given node.
func (n *Node) setSuccessorRPC(node *models.Node, succ *models.Node) error {
	return n.transport.SetSuccessor(node, succ)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *models.Node, id []byte) (*models.Node, error) {
	return n.transport.FindSuccessor(node, id)
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getPredecessorRPC(node *models.Node) (*models.Node, error) {
	return n.transport.GetPredecessor(node)
}

// setPredecessorRPC sets the predecessor of a given node.
func (n *Node) setPredecessorRPC(node *models.Node, pred *models.Node) error {
	return n.transport.SetPredecessor(node, pred)
}

// notifyRPC notifies a remote node that pred is its predecessor.
func (n *Node) notifyRPC(node, pred *models.Node) error {
	return n.transport.Notify(node, pred)
}

func (n *Node) getKeyRPC(node *models.Node, key string) (*models.GetResponse, error) {
	return n.transport.GetKey(node, key)
}
func (n *Node) setKeyRPC(node *models.Node, key, value string) error {
	return n.transport.SetKey(node, key, value)
}
func (n *Node) deleteKeyRPC(node *models.Node, key string) error {
	return n.transport.DeleteKey(node, key)
}

func (n *Node) requestKeysRPC(
	node *models.Node, from []byte, to []byte,
) ([]*models.KV, error) {
	return n.transport.RequestKeys(node, from, to)
}

func (n *Node) deleteKeysRPC(
	node *models.Node, keys []string,
) error {
	return n.transport.DeleteKeys(node, keys)
}

/*
	RPC interface implementation
	Server端函数调用
*/

// ctx context.Context上下文
// GetSuccessor gets the successor on the node..
func (n *Node) GetSuccessor(ctx context.Context, r *models.ER) (*models.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()
	if succ == nil {
		return emptyNode, nil
	}

	return succ, nil
}

// SetSuccessor sets the successor on the node..
func (n *Node) SetSuccessor(ctx context.Context, succ *models.Node) (*models.ER, error) {
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()
	return emptyRequest, nil
}

// SetPredecessor sets the predecessor on the node..
func (n *Node) SetPredecessor(ctx context.Context, pred *models.Node) (*models.ER, error) {
	n.predMtx.Lock()
	n.predecessor = pred
	n.predMtx.Unlock()
	return emptyRequest, nil
}

func (n *Node) FindSuccessor(ctx context.Context, id *models.ID) (*models.Node, error) {
	succ, err := n.findSuccessor(id.Id)
	if err != nil {
		return nil, err
	}

	if succ == nil {
		return nil, ERR_NO_SUCCESSOR
	}

	return succ, nil

}

func (n *Node) CheckPredecessor(ctx context.Context, id *models.ID) (*models.ER, error) {
	return emptyRequest, nil
}

func (n *Node) GetPredecessor(ctx context.Context, r *models.ER) (*models.Node, error) {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()
	if pred == nil {
		return emptyNode, nil
	}
	return pred, nil
}

// Notify notifies Chord that Node(Client) thinks it is our predecessor.
func (n *Node) Notify(ctx context.Context, node *models.Node) (*models.ER, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()
	var prevPredNode *models.Node

	pred := n.predecessor
	if pred == nil || between(node.Id, pred.Id, n.Id) {
		// fmt.Println("setting predecessor", n.Id, node.Id)
		if n.predecessor != nil {
			prevPredNode = n.predecessor
		}
		n.predecessor = node

		// transfer keys from parent node
		if prevPredNode != nil {
			if between(n.predecessor.Id, prevPredNode.Id, n.Id) {
				n.transferKeys(prevPredNode, n.predecessor)
			}
		}

	}

	return emptyRequest, nil
}

// 获取key对应的数据
func (n *Node) XGet(ctx context.Context, req *models.GetRequest) (*models.GetResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Get(req.Key)
	if err != nil {
		return emptyGetResponse, err
	}
	return &models.GetResponse{Value: val}, nil
}

func (n *Node) XSet(ctx context.Context, req *models.SetRequest) (*models.SetResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	fmt.Println("setting key on ", n.Node.Addr, req.Key, req.Value)
	err := n.storage.Set(req.Key, req.Value)
	return emptySetResponse, err
}

func (n *Node) XDelete(ctx context.Context, req *models.DeleteRequest) (*models.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.Delete(req.Key)
	return emptyDeleteResponse, err
}

func (n *Node) XRequestKeys(ctx context.Context, req *models.RequestKeysRequest) (*models.RequestKeysResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Between(req.From, req.To)
	if err != nil {
		return emptyRequestKeysResponse, err
	}
	return &models.RequestKeysResponse{Values: val}, nil
}

func (n *Node) XMultiDelete(ctx context.Context, req *models.MultiDeleteRequest) (*models.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.MDelete(req.Keys...)
	return emptyDeleteResponse, err
}

// 删除本节点，将当前节点的前置节点和后继节点挂钩
func (n *Node) Stop() {
	close(n.shutdownCh)

	// Notify successor to change its predecessor pointer to our predecessor.
	// Do nothing if we are our own successor (i.e. we are the only node in the
	// ring).
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if n.Node.Addr != succ.Addr && pred != nil {
		n.moveKeysFromLocal(pred, succ)
		predErr := n.setPredecessorRPC(succ, pred)
		succErr := n.setSuccessorRPC(pred, succ)
		fmt.Println("stop errors: ", predErr, succErr)
	}

	n.transport.Stop()
}
