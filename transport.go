package chord

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	cm "github.com/zebra-uestc/chord/models/chord"
)

var (
	emptyNode                = &cm.Node{}
	emptyRequest             = &cm.ER{}
	emptyGetResponse         = &cm.GetResponse{}
	emptySetResponse         = &cm.SetResponse{}
	emptyDeleteResponse      = &cm.DeleteResponse{}
	emptyRequestKeysResponse = &cm.RequestKeysResponse{}
)

func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, opts...)
}

/*
	要实现一个 RPC 框架，只需要把以下三点实现了就基本完成了：

	Call ID 映射：可以直接使用函数字符串，也可以使用整数 ID。映射表一般就是一个哈希表。
	序列化反序列化：可以自己写，也可以使用 Protobuf 或者 FlatBuffers 之类的。
	网络传输库：可以自己写 Socket，或者用 Asio，ZeroMQ，Netty 之类。
*/
/*
	Transport enables a node to talk to the other nodes in
	the ring
*/
type Transport interface {
	Start() error
	Stop() error

	//RPC
	GetSuccessor(*cm.Node) (*cm.Node, error)
	FindSuccessor(*cm.Node, []byte) (*cm.Node, error)
	GetPredecessor(*cm.Node) (*cm.Node, error)
	Notify(*cm.Node, *cm.Node) error
	CheckPredecessor(*cm.Node) error
	SetPredecessor(*cm.Node, *cm.Node) error
	SetSuccessor(*cm.Node, *cm.Node) error

	//Storage
	GetKey(*cm.Node, []byte) (*cm.GetResponse, error)
	SetKey(*cm.Node, []byte, []byte) error
	DeleteKey(*cm.Node, []byte) error
	RequestKeys(*cm.Node, []byte, []byte) ([]*cm.KV, error)
	DeleteKeys(*cm.Node, [][]byte) error
}

type GrpcTransport struct {
	config *Config
	*cm.UnimplementedChordServer
	timeout time.Duration
	maxIdle time.Duration

	sock *net.TCPListener

	pool    map[string]*grpcConn
	poolMtx sync.RWMutex

	server *grpc.Server

	shutdown int32
}

// func NewGrpcTransport(config *Config) (cm.ChordClient, error) {
func NewGrpcTransport(config *Config) (*GrpcTransport, error) {

	addr := config.Addr
	// Try to start the listener
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	pool := make(map[string]*grpcConn)

	// Setup the transport
	grp := &GrpcTransport{
		sock:    listener.(*net.TCPListener),
		timeout: config.Timeout,
		maxIdle: config.MaxIdle,
		pool:    pool,
		config:  config,
	}

	grp.server = grpc.NewServer(config.ServerOpts...)

	// Done
	return grp, nil
}

type grpcConn struct {
	addr       string
	client     cm.ChordClient
	conn       *grpc.ClientConn
	lastActive time.Time
}

func (g *grpcConn) Close() {
	g.conn.Close()
}

// Registry(服务发现)：借助 JNDI 发布并调用了 RMI 服务。实际上，JNDI 就是一个注册表，服务端将服务对象放入到注册表中，客户端从注册表中获取服务对象。
func (g *GrpcTransport) registerNode(node *Node) {
	cm.RegisterChordServer(g.server, node)
}

func (g *GrpcTransport) GetServer() *grpc.Server {
	return g.server
}

// Gets an outbound connection to a host
func (g *GrpcTransport) getConn(
	addr string,
) (cm.ChordClient, error) {

	g.poolMtx.RLock()

	if atomic.LoadInt32(&g.shutdown) == 1 {
		g.poolMtx.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}

	cc, ok := g.pool[addr]
	g.poolMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	var conn *grpc.ClientConn
	var err error
	conn, err = Dial(addr, g.config.DialOpts...)
	if err != nil {
		return nil, err
	}

	client := cm.NewChordClient(conn)
	cc = &grpcConn{addr, client, conn, time.Now()}
	g.poolMtx.Lock()
	if g.pool == nil {
		g.poolMtx.Unlock()
		return nil, errors.New("must instantiate node before using")
	}
	g.pool[addr] = cc
	g.poolMtx.Unlock()

	return client, nil
}

func (g *GrpcTransport) Start() error {
	// Start RPC server
	go g.listen()

	// Reap old connections
	go g.reapOld()

	return nil

}

// Returns an outbound TCP connection to the pool
func (g *GrpcTransport) returnConn(o *grpcConn) {
	// Update the last asctive time
	o.lastActive = time.Now()

	// Push back into the pool
	g.poolMtx.Lock()
	defer g.poolMtx.Unlock()
	if atomic.LoadInt32(&g.shutdown) == 1 {
		o.conn.Close()
		return
	}
	g.pool[o.addr] = o
}

// Shutdown the TCP transport
func (g *GrpcTransport) Stop() error {
	atomic.StoreInt32(&g.shutdown, 1)

	// Close all the connections
	g.poolMtx.Lock()

	g.server.Stop()
	for _, conn := range g.pool {
		conn.Close()
	}
	g.pool = nil

	g.poolMtx.Unlock()

	return nil
}

// Closes old outbound connections
func (g *GrpcTransport) reapOld() {
	ticker := time.NewTicker(60 * time.Second)

	for {
		if atomic.LoadInt32(&g.shutdown) == 1 {
			return
		}
		select {
		case <-ticker.C:
			g.reap()
		}

	}
}

func (g *GrpcTransport) reap() {
	g.poolMtx.Lock()
	defer g.poolMtx.Unlock()
	for host, conn := range g.pool {
		if time.Since(conn.lastActive) > g.maxIdle {
			conn.Close()
			delete(g.pool, host)
		}
	}
}

// Listens for inbound connections
func (g *GrpcTransport) listen() {
	g.server.Serve(g.sock)
}

// GetSuccessor the successor ID of a remote node.
func (g *GrpcTransport) GetSuccessor(node *cm.Node) (*cm.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.GetSuccessor(ctx, emptyRequest)
}

// FindSuccessor the successor ID of a remote node.
func (g *GrpcTransport) FindSuccessor(node *cm.Node, id []byte) (*cm.Node, error) {
	// fmt.Println("yo", node.Id, id)
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.FindSuccessor(ctx, &cm.ID{Id: id})
}

// GetPredecessor the successor ID of a remote node.
func (g *GrpcTransport) GetPredecessor(node *cm.Node) (*cm.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.GetPredecessor(ctx, emptyRequest)
}

func (g *GrpcTransport) SetPredecessor(node *cm.Node, pred *cm.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.SetPredecessor(ctx, pred)
	return err
}

func (g *GrpcTransport) SetSuccessor(node *cm.Node, succ *cm.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.SetSuccessor(ctx, succ)
	return err
}

func (g *GrpcTransport) Notify(node, pred *cm.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.Notify(ctx, pred)
	return err

}

func (g *GrpcTransport) CheckPredecessor(node *cm.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.CheckPredecessor(ctx, &cm.ID{Id: node.Id})
	return err
}

func (g *GrpcTransport) GetKey(node *cm.Node, key []byte) (*cm.GetResponse, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.XGet(ctx, &cm.GetRequest{Key: key})
}

func (g *GrpcTransport) SetKey(node *cm.Node, key, value []byte) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XSet(ctx, &cm.SetRequest{Key: key, Value: value})
	return err
}

func (g *GrpcTransport) DeleteKey(node *cm.Node, key []byte) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XDelete(ctx, &cm.DeleteRequest{Key: key})
	return err
}

func (g *GrpcTransport) RequestKeys(node *cm.Node, from, to []byte) ([]*cm.KV, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	val, err := client.XRequestKeys(
		ctx, &cm.RequestKeysRequest{From: from, To: to},
	)
	if err != nil {
		return nil, err
	}
	return val.Values, nil
}

func (g *GrpcTransport) DeleteKeys(node *cm.Node, keys [][]byte) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XMultiDelete(
		ctx, &cm.MultiDeleteRequest{Keys: keys},
	)
	return err
}
