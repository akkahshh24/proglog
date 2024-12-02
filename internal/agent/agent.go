package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	api "github.com/akkahshh24/proglog/api/v1"
	"github.com/akkahshh24/proglog/internal/auth"
	"github.com/akkahshh24/proglog/internal/discovery"
	"github.com/akkahshh24/proglog/internal/log"
	"github.com/akkahshh24/proglog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Manages the different components and processes.
// It references each component it manages.
// (Log, server, membership, replicator).
type agent struct {
	config Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

// Comprises of the components' parameters it manages
// to pass them through.
type Config struct {
	dataDir         string // used to store log (store and index) files
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
	bindAddr        string // the address and port the server will listen on
	rpcPort         int    // ? why is this needed?
	nodeName        string
	startJoinAddrs  []string
	aclModelFile    string // used to configure authorizer
	aclPolicyFile   string // used to configure authorizer
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.bindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.rpcPort), nil
}

// Creates an agent and runs a set of methods to set up
// and run the agent's components.
func New(config Config) (*agent, error) {
	a := &agent{
		config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.config.dataDir,
		log.Config{},
	)
	return err
}

func (a *agent) setupServer() error {
	// To setup a new gRPC server, we need a commit log
	// and an authorizer. We also need to build our server options
	// with the TLS credentials.
	authorizer := auth.New(
		a.config.aclModelFile,
		a.config.aclPolicyFile,
	)

	serverConfig := &server.Config{
		Commitlog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if a.config.serverTLSConfig != nil {
		creds := credentials.NewTLS(a.config.serverTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	rpcAddr, err := a.config.RPCAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

// Sets up a Replicator with the given gRPC dial options
func (a *agent) setupMembership() error {
	// To create a new membership we need a handler, which is
	// replicator in our case and configuration.
	// For a replicator, we need client dial options and
	// a grpc client to consume from other discovered servers
	// the cluster and produce a copy in the local server.
	// For a gRPC client, we need the target address.

	var opts []grpc.DialOption
	if a.config.peerTLSConfig != nil {
		creds := credentials.NewTLS(a.config.peerTLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	rpcAddr, err := a.config.RPCAddr()
	if err != nil {
		return err
	}

	cc, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(cc)

	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}

	a.membership, err = discovery.New(
		a.replicator,
		discovery.Config{
			NodeName: a.config.nodeName,
			BindAddr: a.config.bindAddr,
			Tags: map[string]string{
				"rpc_addr": rpcAddr,
			},
			StartJoinAddrs: a.config.startJoinAddrs,
		})
	return err
}

// Shuts down the agent and it's components by:
// 1. Leaving the membership to signal other servers and stop receiving serf events
// 2. Closing the replicator and stop replicating
// 3. Gracefully stopping the server
// 4. Closing the log
func (a *agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
