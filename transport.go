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
	er = &internal.ER{}
)

// Dial wraps grpc's dial function with settings that facilitate the
// functionality of gmaj.
func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, append(append(opts,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
		grpc.FailOnNonTempDialError(true)),
		grpc.WithInsecure(),
	)...)
}

// Implements the methods needed for a Chord ring
type Transport interface {
	// Find a successor
	GetSuccessor(*internal.Node) (*internal.Node, error)
	FindSuccessor(*internal.Node) (*internal.Node, error)
	Start()
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

type grpcConn struct {
	addr       string
	client     internal.ChordClient
	conn       *grpc.ClientConn
	lastActive time.Time
}

func (g *grpcConn) Close() {
	g.conn.Close()
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

	// conn, err := Dial(addr, g.config.DialOpts...)
	conn, err := Dial(addr)
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
	}

	// grp.server = grpc.NewServer(config.ServerOpts...)
	grp.server = grpc.NewServer()

	// Done
	return grp, nil
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
	for {
		if atomic.LoadInt32(&g.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		g.reapOnce()
	}
}

func (g *GrpcTransport) reapOnce() {
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

	return client.GetSuccessor(context.Background(), er)
}

// FindSuccessor the successor ID of a remote node.
func (g *GrpcTransport) FindSuccessor(node *internal.Node) (*internal.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}
	return client.FindSuccessor(context.Background(), &internal.ID{Id: node.Id})
}
