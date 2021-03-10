package chord

import (
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"math/big"
	"sync"
	"time"

	cm "github.com/zebra-uestc/chord/models/chord"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func DefaultConfig() *Config {
	n := &Config{
		//sha1.New的Hash function size=20
		Hash:     sha256.New,
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
	// hashsize shouldnt be less than hash func size
	// if c.HashSize < c.Hash().Size() {
	// 	return ERR_HASHSIZE
	// }
	return nil
}

//新建单个节点
func NewInode(id string, addr string) *cm.Node {
	h := sha1.New()
	if _, err := h.Write([]byte(id)); err != nil {
		return nil
	}
	val := h.Sum(nil)

	return &cm.Node{
		Id:   val,
		Addr: addr,
	}
}

//NewNode creates a new Chord node. Returns error if node alreadyexists in the chord ring
func NewNode(cnf *Config, joinNode *cm.Node) (*Node, error) {
	if err := cnf.Validate(); err != nil {
		return nil, err
	}
	node := &Node{
		Node:       new(cm.Node),
		shutdownCh: make(chan struct{}),
		cnf:        cnf,
		storage:    NewtxStorage(cnf.Hash),
	}

	var nID string
	if cnf.Id != "" {
		nID = cnf.Id
	} else {
		nID = cnf.Addr
	}
	id, err := node.hashKey([]byte(nID))
	if err != nil {
		return nil, err
	}
	aInt := (&big.Int{}).SetBytes(id)

	fmt.Printf("new node id %d, \n", aInt)

	node.Node.Id = id
	node.Node.Addr = cnf.Addr

	// Populate finger table
	node.fingerTable = newFingerTable(node.Node, cnf.HashSize)

	// Start RPC server	是否会阻塞在这儿？测试了一下不会
	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	cm.RegisterChordServer(transport.server, node)

	node.transport.Start()

	//新增节点操作

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
	*cm.Node
	*cm.UnimplementedChordServer
	cnf *Config

	predecessor *cm.Node
	predMtx     sync.RWMutex

	successor *cm.Node
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

func (n *Node) hashKey(key []byte) ([]byte, error) {
	h := n.cnf.Hash()
	if _, err := h.Write(key); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

// 已验证逻辑，并大修改
//joinnode为已在环上的父节点，根据joinNode的信息将n加入环
func (n *Node) join(joinNode *cm.Node) error {
	// First check if node already present in the circle
	// Join this node to the same chord ring as parent
	fmt.Println("join node...")
	var err error
	var succ *cm.Node
	// // Ask if our id already exists on the ring.
	if joinNode != nil {
		//根据n.Id,通过grpc，以joinnode为起点，(递归)找到n的正确successor节点
		remoteNode, err := n.findSuccessorRPC(joinNode, n.Id)
		if err != nil {
			return err
		}
		fmt.Println("findSuccessorRPC()->remoteNode: ", remoteNode.Addr)
		//如果后继节点的id与我想要加入环中的节点n.Id相等，说明该节点已经加入环
		if isEqual(remoteNode.Id, n.Id) {
			return ERR_NODE_EXISTS
		}
		succ = remoteNode
	} else {
		succ = n.Node
	}

	if err != nil {

		return err
	}
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()
	fmt.Println("successor...", succ.Addr)

	// 改为放到Notify()里面，周期调用。。。
	// 刚加入节点，找到successor后，马上转移数据
	// 直接从succ上拉取数据，而不用先找predecessor
	// if n.successor != nil {
	// 	fmt.Println("join transfer keys from...", succ.Addr)
	// 	n.transferKeys(n.successor, n.Node)
	// }

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

func (n *Node) Find(key []byte) (*cm.Node, error) {
	return n.locate(key)
}

func (n *Node) Get(key []byte) ([]byte, error) {
	return n.get(key)
}
func (n *Node) Set(key []byte, value []byte) error {
	return n.set(key, value)
}
func (n *Node) Delete(key []byte) error {
	return n.delete(key)
}

/*
	Finds the node for the key
*/
func (n *Node) locate(key []byte) (*cm.Node, error) {
	id, err := n.hashKey(key)
	if err != nil {
		return nil, err
	}
	succ, err := n.findSuccessor(id)
	return succ, err
}

func (n *Node) get(key []byte) ([]byte, error) {
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

func (n *Node) set(key []byte, value []byte) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.setKeyRPC(node, key, value)
	return err
}

func (n *Node) delete(key []byte) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.deleteKeyRPC(node, key)
	return err
}

// 已验证逻辑，并做修改
// 感觉transfer是增加节点时从succ节点转移数据用的
// 从succ把pred到n之间的数据转移到n上
//论文里面节点的退出归类为节点崩溃的一种。
func (n *Node) transferKeys(pred, succ *cm.Node) {
	fmt.Println("transferKeys()...pred, succ ", pred.Addr, succ.Addr)
	//修改为将(n.Id, pred.Id)之间的key从succ转移到n上
	keys, err := n.requestKeys(pred, n.Node)
	if len(keys) > 0 {
		for _, item := range keys {
			fmt.Println("transfering key,value: ", item.Key, item.Value, err)
		}

	}
	delKeyList := make([][]byte, 0, 10)
	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		n.storage.Set(item.Key, item.Value)
		if err != nil {
			fmt.Println("error transfering key: ", item.Key, succ.Addr)
		}
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from succ node.
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}
}

// func (n *Node) transferKeys(predpred, pred *cm.Node) {
// 	fmt.Println("transferKeys()...predpred, pred ", predpred.Addr, pred.Addr)
// 	//修改为将(n.Id, pred.Id)之间的key从succ转移到n上
// 	keys, err := n.storage.Between(predpred.Id, pred.Id)
// 	if len(keys) > 0 {
// 		for _, item := range keys {
// 			fmt.Println("transfering key,value: ", item.Key, item.Value, err)
// 		}
// 	}
// 	delKeyList := make([]string, 0, 10)
// 	// store the keys in current node
// 	for _, item := range keys {
// 		if item == nil {
// 			continue
// 		}
// 		err := n.setKeyRPC(pred, item.Key, item.Value)
// 		if err != nil {
// 			fmt.Println("error transfering key: ", item.Key, pred.Addr)
// 		}
// 		delKeyList = append(delKeyList, item.Key)
// 	}
// 	// delete the keys from the current node.
// 	if len(delKeyList) > 0 {
// 		for i := range delKeyList {
// 			n.storage.Delete(delKeyList[i])
// 		}
// 	}
// }

//已验证逻辑，并做修改
// 实现Node删除时将Node上的所有数据转移给其successor
// 将fromnode到tonode的数据存到tonode上，并存本地删除这些数据
func (n *Node) moveKeysFromLocal(fromNode, toNode *cm.Node) {
	fmt.Println("moveKeysFromLocal()...from, to ", fromNode.Addr, toNode.Addr)
	keys, err := n.storage.Between(fromNode.Id, toNode.Id)
	if len(keys) > 0 {
		for _, item := range keys {
			fmt.Println("transfering key: ", item.Key, err)
		}

	}
	delKeyList := make([][]byte, 0, 10)
	// store the keys in  toNode
	for _, item := range keys {
		if item == nil {
			continue
		}
		err := n.setKeyRPC(toNode, item.Key, item.Value)
		if err != nil {
			fmt.Println("error transfering key: ", item.Key, toNode.Addr)
		}
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from the current node.
	if len(delKeyList) > 0 {
		for i := range delKeyList {
			n.storage.Delete(delKeyList[i])
		}
	}
}

func (n *Node) deleteKeys(node *cm.Node, keys [][]byte) error {
	return n.deleteKeysRPC(node, keys)
}

// When a new node joins, it requests keys from it's successor
// 返回pred到succ之间所有键值对
func (n *Node) requestKeys(pred, succ *cm.Node) ([]*cm.KV, error) {

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

// 已验证逻辑，并做修改，边界情况欠考虑(只有一个node),感觉可能还有问题
// 本函数以当前n为起点找hash为id的数据应当存放的节点，或某节点的下一个节点
// id为fingerEntry的id，比如第i+1个fingerEntry：id为n + 2^i，找到对应的后继node，注意区分finger表的id和节点的id(本函数应该使用节点id)
// 初始化时，succ = cur
func (n *Node) findSuccessor(id []byte) (*cm.Node, error) {
	// Check if lock is needed throughout the process
	n.succMtx.RLock()
	defer n.succMtx.RUnlock()
	curr := n.Node
	succ := n.successor //succ：预期存放数据的节点

	if succ == nil {
		return curr, nil
	}

	// var err error

	if betweenRightIncl(id, curr.Id, succ.Id) {
		/* 若id在curr和curr的下一个节点之间 -> succ即为下一个节点 */
		return succ, nil
	} else {
		/*
			论文里的方法为：
			若id不在curr和curr的下一个节点之间
			-> 从finger表中查找最靠近id的hash值(n+2^i−1)所对应的节点
			-> 调用getSuccessorRPC远程重复查找过程(类似递归查找)
		*/
		pred := n.closestPrecedingNode(id)
		/*
			NOT SURE ABOUT THIS, RECHECK from paper!!!？？？
			if preceeding node and current node are the same,
			store the key on this node
		*/
		if isEqual(pred.Id, n.Id) {
			// succ, err = n.getSuccessorRPC(pred)
			succ = n.successor
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
	// return nil, nil
}

//已验证逻辑
// Fig 5 implementation for closest_preceding_node
// 返回当前节点finger表中小于键值对hash值的最大id映射的节点(该节点可能是id即将映射的节点，也可能是当前节点的finger表中距离目标节点更近的节点)
// 找到离id最接近的最大的节点，从后往前找
// 论文里是直接把id跟finger表中节点的id比(而不是跟n+2^i比。。。)
func (n *Node) closestPrecedingNode(id []byte) *cm.Node {
	n.predMtx.RLock()
	defer n.predMtx.RUnlock()

	curr := n.Node

	m := len(n.fingerTable) - 1
	for i := m; i >= 0; i-- {
		f := n.fingerTable[i]
		if f == nil || f.RemoteNode == nil {
			continue
		}
		if between(f.RemoteNode.Id, curr.Id, id) {
			return f.RemoteNode
		}
	}
	return curr
}

/*
	Periodic functions implementation
*/
// The stabilization protocol works as follows:
// Stabilize(): n asks its successor for its predecessor p and decides whether p should be n‘s successor instead (this is the case if p recently joined the system).
// Stabilize(): n查询其后继节点的前序节点P来决定P是否应该是n的后续节点，也就是说当p不是n本身时，说明p是新加入的，此时将n的后继节点设置为p。
// notify(): notifies n‘s successor of its existence, so it can change its predecessor to n.This is an implementation of the psuedocode from figure 7 of chord paper.
// Fix_fingers(): updates finger tables
/*
	伪代码：
	n.stabilize()：(加入node时)确保succ正确，并更新
		x = successor.predecessor;
		if (x ∈ (n,successor))
		successor = x;
		successor.notify(n);

*/
//已验证逻辑
func (n *Node) stabilize() {
	// fmt.Println("stabilize()... ")
	n.succMtx.RLock()
	succ := n.successor
	if succ == nil {
		n.succMtx.RUnlock()
		return
	}
	n.succMtx.RUnlock()
	//x是后继节点的前驱节点，即一般情况下应该是自己
	//如果此时新加入了节点p，则x是节点p，此时需要将p更新为自己的后继节点
	x, err := n.getPredecessorRPC(succ)
	if err != nil || x == nil {
		fmt.Println("error getting predecessor, ", err, x)
		return
	}
	//x.Id在（n.Id, succ.Id）之间
	if x.Id != nil && between(x.Id, n.Id, succ.Id) {
		n.succMtx.Lock()
		//将x更新为自己的后继节点
		n.successor = x
		n.succMtx.Unlock()
		fmt.Println("successor ", x.Addr)
	}
	//notifyRPC表示连接succ，在succ节点上执行Notify(n.Node)
	n.notifyRPC(n.successor, n.Node)
}

// checkes whether predecessor has failed. Newnode()中周期调用
func (n *Node) checkPredecessor() {
	// fmt.Println("checkPredecessor()... ")
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
func (n *Node) getSuccessorRPC(node *cm.Node) (*cm.Node, error) {
	return n.transport.GetSuccessor(node)
}

// setSuccessorRPC sets the successor of a given node.
func (n *Node) setSuccessorRPC(node *cm.Node, succ *cm.Node) error {
	return n.transport.SetSuccessor(node, succ)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *cm.Node, id []byte) (*cm.Node, error) {
	return n.transport.FindSuccessor(node, id)
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getPredecessorRPC(node *cm.Node) (*cm.Node, error) {
	return n.transport.GetPredecessor(node)
}

// setPredecessorRPC sets the predecessor of a given node.
func (n *Node) setPredecessorRPC(node *cm.Node, pred *cm.Node) error {
	return n.transport.SetPredecessor(node, pred)
}

// notifyRPC notifies a remote node that pred is its predecessor.
func (n *Node) notifyRPC(node, pred *cm.Node) error {
	return n.transport.Notify(node, pred)
}

func (n *Node) getKeyRPC(node *cm.Node, key []byte) (*cm.GetResponse, error) {
	return n.transport.GetKey(node, key)
}
func (n *Node) setKeyRPC(node *cm.Node, key []byte, value []byte) error {
	return n.transport.SetKey(node, key, value)
}
func (n *Node) deleteKeyRPC(node *cm.Node, key []byte) error {
	return n.transport.DeleteKey(node, key)
}

func (n *Node) requestKeysRPC(node *cm.Node, from []byte, to []byte) ([]*cm.KV, error) {
	return n.transport.RequestKeys(node, from, to)
}

func (n *Node) deleteKeysRPC(node *cm.Node, keys [][]byte) error {
	return n.transport.DeleteKeys(node, keys)
}

/*
	RPC interface implementation
	Server端函数调用(被动方)
*/

// ctx context.Context上下文
// GetSuccessor gets the successor on the node..
func (n *Node) GetSuccessor(ctx context.Context, r *cm.ER) (*cm.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()
	if succ == nil {
		return emptyNode, nil
	}

	return succ, nil
}

// SetSuccessor sets the successor on the node..
func (n *Node) SetSuccessor(ctx context.Context, succ *cm.Node) (*cm.ER, error) {
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()
	fmt.Println("successor......", n.successor.Addr)
	return emptyRequest, nil
}

// SetPredecessor sets the predecessor on the node..
func (n *Node) SetPredecessor(ctx context.Context, pred *cm.Node) (*cm.ER, error) {
	n.predMtx.Lock()
	n.predecessor = pred
	n.predMtx.Unlock()
	fmt.Println("predecessor......", n.predecessor.Addr)
	return emptyRequest, nil
}

func (n *Node) FindSuccessor(ctx context.Context, id *cm.ID) (*cm.Node, error) {
	succ, err := n.findSuccessor(id.Id)
	if err != nil {
		return nil, err
	}

	if succ == nil {
		return nil, ERR_NO_SUCCESSOR
	}

	return succ, nil

}

func (n *Node) CheckPredecessor(ctx context.Context, id *cm.ID) (*cm.ER, error) {
	return emptyRequest, nil
}

func (n *Node) GetPredecessor(ctx context.Context, r *cm.ER) (*cm.Node, error) {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()
	if pred == nil {
		return emptyNode, nil
	}
	return pred, nil
}

//已验证逻辑，transfer_Keys部分存疑
// Notify notifies Chord that Node(Client) thinks it is our predecessor
//Notify(n0): n0通知n它的存在，若此时n没有前序节点或，n0比n现有的前序节点更加靠近n，则n将其设置为前序节点。
func (n *Node) Notify(ctx context.Context, node *cm.Node) (*cm.ER, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()
	//prevPredNode记录的更新n.predecessor后，之前的n.predecessor
	var prevPredNode *cm.Node

	pred := n.predecessor
	//若此时n没有前序节点或
	//node.Id比n现有的前序节点pred.Id更加靠近n，则将node设置为n的前序节点
	if pred == nil || between(node.Id, pred.Id, n.Id) {
		fmt.Println("predecessor...", node.Addr)
		if n.predecessor != nil {
			prevPredNode = n.predecessor
		}
		n.predecessor = node

		// 增加节点时transfer key的工作由移出数据的节点做
		// transfer keys from current node to node's predecessor
		if prevPredNode != nil {
			if between(n.predecessor.Id, prevPredNode.Id, n.Id) {
				fmt.Println("transferKeys() to", n.predecessor.Addr)
				n.moveKeysFromLocal(prevPredNode, n.predecessor)
			}
		}
	}

	return emptyRequest, nil
}

// 获取key对应的数据
func (n *Node) XGet(ctx context.Context, req *cm.GetRequest) (*cm.GetResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Get(req.Key)
	if err != nil {
		return emptyGetResponse, err
	}
	return &cm.GetResponse{Value: val}, nil
}

func (n *Node) XSet(ctx context.Context, req *cm.SetRequest) (*cm.SetResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	// fmt.Println("setting key on ", n.Node.Addr, req.Key, req.Value)
	// fmt.Println("setting key on ", n.Node.Addr, " key:", req.Key)

	err := n.storage.Set(req.Key, req.Value)
	return emptySetResponse, err
}

func (n *Node) XDelete(ctx context.Context, req *cm.DeleteRequest) (*cm.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.Delete(req.Key)
	return emptyDeleteResponse, err
}

func (n *Node) XRequestKeys(ctx context.Context, req *cm.RequestKeysRequest) (*cm.RequestKeysResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Between(req.From, req.To)
	if err != nil {
		return emptyRequestKeysResponse, err
	}
	return &cm.RequestKeysResponse{Values: val}, nil
}

func (n *Node) XMultiDelete(ctx context.Context, req *cm.MultiDeleteRequest) (*cm.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.MDelete(req.Keys...)
	return emptyDeleteResponse, err
}

// 删除本节点，将当前节点的前置节点和后继节点挂钩
func (n *Node) Stop() {
	close(n.shutdownCh)

	// notify successor to change its predecessor pointer to our predecessor.
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

func (n *Node) GetConfig() *Config {
	return n.cnf
}

func (n *Node) GetStorage() Storage {
	return n.storage
}

func (n *Node) GetShutdownCh() chan struct{} {
	return n.shutdownCh
}
