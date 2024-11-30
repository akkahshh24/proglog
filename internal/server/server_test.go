package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	api "github.com/akkahshh24/proglog/api/v1"
	"github.com/akkahshh24/proglog/internal/auth"
	"github.com/akkahshh24/proglog/internal/config"
	"github.com/akkahshh24/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// The -debug flag can be passed to enable debugging
var debug = flag.Bool("debug", false, "Enable observability for debugging.")

// A special function which runs before any tests
// to setup global configs or run setup/teardown logic.
func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	// run all package test cases and exit
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		// 2. client api.LogClient,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a msg to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                testProduceConsumeStream,
		"unauthorized client fails":                      testUnauthorized,
		"consume past offset range fails":                testConsumePastRange,
	} {
		t.Run(scenario, func(t *testing.T) {
			// 2. client, config, teardown := setupTest(t, nil)
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			// 2. fn(t, client, config)
			fn(t, rootClient, nobodyClient, config) // ? why is config needed?
		})
	}
}

// ? what is the use of fn?
func setupTest(t *testing.T, fn func(*Config)) (
	// 2. client api.LogClient,
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	// to tell the compiler to skip the function in the call stack when reporting errors
	t.Helper()

	// create a listener on any free port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// 1. make an insecure connection to the listener
	// 1. clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// * 2. Code updated to setup client TLS
	// 2. clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
	// 	CertFile: config.ClientCertFile,
	// 	KeyFile:  config.ClientKeyFile,
	// 	CAFile:   config.CAFile,
	// })
	// require.NoError(t, err)

	// 1. cc, err := grpc.NewClient(l.Addr().String(), clientOptions...)
	// 2. clientCreds := credentials.NewTLS(clientTLSConfig)

	// * 2. Code updated to setup client TLS
	// 2. cc, err := grpc.NewClient(
	// 	l.Addr().String(),
	// 	grpc.WithTransportCredentials(clientCreds),
	// )
	// require.NoError(t, err)

	// 2. client = api.NewLogClient(cc)

	// * 3. Build two clients for testing authorization
	// 3. root client to produce and consume
	// 3. nobody client with no permissions
	newClient := func(certPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: certPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)

		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.NewClient(l.Addr().String(), opts...)
		require.NoError(t, err)

		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

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
		Server:        true, // for mutual TLS
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server_test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	// Setup and start the telemetry exporter to write to two files.
	// By declaring outside the block, we ensure function wide access.
	// Otherwise, using := short declaration operator would limit it's
	// scope to only the if block.
	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)

		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	// named return
	cfg = &Config{
		commitlog:  clog,
		authorizer: authorizer,
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

	// 2. return client, cfg, func() {
	return rootClient, nobodyClient, cfg, func() {
		// teardown function
		server.Stop()
		// 2. cc.Close()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove() // TODO: re-check if needed
		if telemetryExporter != nil {
			// sleep for 1.5s to flush data to disk
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

func testProduceConsume(
	t *testing.T,
	client,
	_ api.LogClient,
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
	client,
	_ api.LogClient,
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

func testUnauthorized(
	t *testing.T,
	_,
	client api.LogClient, // use nobody client
	config *Config,
) {
	ctx := context.Background()

	// produce using unauthorized client and check response and error code
	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("scalability"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}

	// consume using unauthorized client and check response and error code
	consume, err := client.Consume(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}

	gotCode = status.Code(err)
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}

func testConsumePastRange(
	t *testing.T,
	client,
	_ api.LogClient,
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
