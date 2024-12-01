package log

import (
	"context"
	"sync"

	api "github.com/akkahshh24/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// * Exported: used by agent
// Replicator connects to other servers with the gRPC client
// and consumes from it and produces to the local server.
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool // to check the replicator's status (open/closed)
	close       chan struct{}
}

// Adds the given server address to the list of servers
// to replicate from and starts a goroutine to run the
// replication logic.
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	// if the given name already exists in the servers map
	// then we are already replicating from the server
	if _, ok := r.servers[name]; ok {
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}

// Creates a client and opens up a stream to consume all logs
// from the address of the given server.
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	// receive the records and put them into the records channel
	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	// consume logs from the discovered server and produce to
	// the local server until that server fails/leaves.
	for {
		select {
		case <-r.close:
			return

		// stop replicating when the server leaves the cluster
		// this channel is closed in the 'leave' method
		case <-leave:
			return

		// produce to the local server when we receive a record
		// in the records channel
		case record := <-records:
			_, err = r.LocalServer.Produce(
				ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Removes the server from the list of servers to replicate from
// and closes the server's associated channel to signal the replicate
// goroutine to stop replicating from that server.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	// if the server which is leaving does not exist
	// in our servers map, we don't need to do anything
	if _, ok := r.servers[name]; !ok {
		return nil
	}

	// close the leave channel
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// Lazily initializes the logger, servers map and close channel.
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}

	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}

	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(msg, zap.String("addr", addr), zap.Error(err))
}
