package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/akkahshh24/proglog/api/v1"
	"github.com/akkahshh24/proglog/internal/config"
	"github.com/akkahshh24/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a msg to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                testProduceConsumeStream,
		"consume past offset range fails":                testConsumePastRange,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

// TODO: what is the use of fn?
func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	// to tell the compiler to skip the function in the call stack when reporting errors
	t.Helper()

	// create a listener on any free port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// make an insecure connection to the listener
	// clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// * Code updated to setup client TLS
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile: config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)
	// cc, err := grpc.NewClient(l.Addr().String(), clientOptions...)

	// * Code updated to setup client TLS
	cc, err := grpc.NewClient(
		l.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
	)
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	// Now we need to create a server and start serving our requests
	// For that we need config
	// For config, we need a commit log
	// For a commit log, we need a directory
	// * Code updated to setup server TLS
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server_test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// named return
	cfg = &Config{
		commitlog: clog,
	}

	if fn != nil {
		fn(cfg)
	}

	// server, err := NewGRPCServer(cfg)
	// * Code updated to setup TLS
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// blocking call, therefore goroutine
	go func() {
		server.Serve(l)
	}()

	return client, cfg, func() {
		// teardown function
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove() // TODO: re-check if needed
	}
}

func testProduceConsume(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("Mystery Rooms"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	// sample records
	records := []*api.Record{{
		Value:  []byte("Cabin in the woods"),
		Offset: 0,
	}, {
		Value:  []byte("Kohinoor Adventure"),
		Offset: 1,
	}}

	// produce stream
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		// iterate over the records and send one by one
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			// validate received offset
			recv, err := stream.Recv()
			require.NoError(t, err)

			if recv.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					recv.Offset,
					offset,
				)
			}
		}
	}

	// consume stream
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for _, record := range records {
			recv, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, recv.Record.Value, record.Value)
			require.Equal(t, recv.Record.Offset, record.Offset)
		}
	}
}

func testConsumePastRange(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	record := &api.Record{Value: []byte("India's Got Latent")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: record})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}
