package chord

import (
	"errors"
	"fmt"
	"github.com/arriqaaq/chord/internal"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	emptyNode                = &internal.Node{}
	emptyRequest             = &internal.ER{}
	emptyGetResponse         = &internal.GetResponse{}
	emptySetResponse         = &internal.SetResponse{}
	emptyDeleteResponse      = &internal.DeleteResponse{}
	emptyRequestKeysResponse = &internal.RequestKeysResponse{}
)

func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, opts...)
}

/*
	Transport enables a node to talk to the other nodes in
	the ring
*/
type Transport interface {
	Start()
	GetSuccessor(*internal.Node) (*internal.Node, error)
	FindSuccessor(*internal.Node, []byte) (*internal.Node, error)
	GetPredecessor(*internal.Node) (*internal.Node, error)
	Notify(*internal.Node, *internal.Node) error
	CheckPredecessor(*internal.Node) error

	//Storage
	GetKey(*internal.Node, string) (*internal.GetResponse, error)
	SetKey(*internal.Node, string, string) error
	DeleteKey(*internal.Node, string) error
	RequestKeys(*internal.Node, []byte, []byte) ([]*internal.KV, error)
	DeleteKeys(*internal.Node, []string) error
}

type GrpcTransport struct {
	config *Config

	timeout time.Duration
	maxIdle time.Duration

	sock *net.TCPListener

	pool    map[string]*grpcConn
	poolMtx sync.RWMutex

	server *grpc.Server

	shutdown int32
}

// func NewGrpcTransport(config *Config) (internal.ChordClient, error) {
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
	client     internal.ChordClient
	conn       *grpc.ClientConn
	lastActive time.Time
}

func (g *grpcConn) Close() {
	g.conn.Close()
}

func (g *GrpcTransport) registerNode(node *Node) {
	internal.RegisterChordServer(g.server, node)
}

func (g *GrpcTransport) GetServer() *grpc.Server {
	return g.server
}

// Gets an outbound connection to a host
func (g *GrpcTransport) getConn(
	addr string,
) (internal.ChordClient, error) {

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

	client := internal.NewChordClient(conn)
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

func (g *GrpcTransport) Start() {
	// Start RPC server
	go g.listen()

	// Reap old connections
	go g.reapOld()

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
func (g *GrpcTransport) Shutdown() {
	atomic.StoreInt32(&g.shutdown, 1)

	// Close all the connections
	g.poolMtx.Lock()

	g.server.Stop()
	for _, conn := range g.pool {
		conn.Close()
	}
	g.pool = nil

	g.poolMtx.Unlock()
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
			// case <-node.shutdownCh:
			// 	ticker.Stop()
			// 	return
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
func (g *GrpcTransport) GetSuccessor(node *internal.Node) (*internal.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.GetSuccessor(ctx, emptyRequest)
}

// FindSuccessor the successor ID of a remote node.
func (g *GrpcTransport) FindSuccessor(node *internal.Node, id []byte) (*internal.Node, error) {
	// fmt.Println("yo", node.Id, id)
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.FindSuccessor(ctx, &internal.ID{Id: id})
}

// GetPredecessor the successor ID of a remote node.
func (g *GrpcTransport) GetPredecessor(node *internal.Node) (*internal.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.GetPredecessor(ctx, emptyRequest)
}

func (g *GrpcTransport) Notify(node, pred *internal.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.Notify(ctx, pred)
	return err

}

func (g *GrpcTransport) CheckPredecessor(node *internal.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.CheckPredecessor(ctx, &internal.ID{Id: node.Id})
	return err
}

func (g *GrpcTransport) GetKey(node *internal.Node, key string) (*internal.GetResponse, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.XGet(ctx, &internal.GetRequest{Key: key})
}

func (g *GrpcTransport) SetKey(node *internal.Node, key, value string) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XSet(ctx, &internal.SetRequest{Key: key, Value: value})
	return err
}

func (g *GrpcTransport) DeleteKey(node *internal.Node, key string) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XDelete(ctx, &internal.DeleteRequest{Key: key})
	return err
}

func (g *GrpcTransport) RequestKeys(node *internal.Node, from, to []byte) ([]*internal.KV, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	val, err := client.XRequestKeys(
		ctx, &internal.RequestKeysRequest{From: from, To: to},
	)
	if err != nil {
		return nil, err
	}
	return val.Values, nil
}

func (g *GrpcTransport) DeleteKeys(node *internal.Node, keys []string) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XMultiDelete(
		ctx, &internal.MultiDeleteRequest{Keys: keys},
	)
	return err
}
