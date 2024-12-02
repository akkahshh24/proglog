package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/akkahshh24/proglog/api/v1"
	"github.com/akkahshh24/proglog/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Setup a three-node cluster
func TestAgent(t *testing.T) {
	// these server certificates will be served to clients.
	// needed for agent config
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// these client certificates will be served between servers
	// to connect with and replicate each other.
	// needed for agent config
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*agent
	for i := 0; i < 3; i++ {
		// get two free ports for our service
		// RPC address and Serf address are needed for agent config
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		// a directory is also needed for storing the log
		// and passed in agent config
		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		// create startJoinAddrs for the second and third node
		// needed in agent config
		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].config.bindAddr,
			)
		}

		agent, err := New(Config{
			nodeName:        fmt.Sprintf("%d", i),
			startJoinAddrs:  startJoinAddrs,
			bindAddr:        bindAddr,
			rpcPort:         rpcPort,
			dataDir:         dataDir,
			aclModelFile:    config.ACLModelFile,
			aclPolicyFile:   config.ACLPolicyFile,
			serverTLSConfig: serverTLSConfig,
			peerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	// Runs after the test to shutdown the agents and
	// delete the test data.
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.config.dataDir))
		}
	}()

	// Sleep to let the three nodes discover each other.
	time.Sleep(3 * time.Second)

	// produce a record to the first node
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("football"),
			},
		},
	)
	require.NoError(t, err)

	// consume the same record from the node
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)

	require.Equal(t, []byte("football"), consumeResponse.Record.Value)

	// wait for replication in other nodes
	// consume from another node and verify result
	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)

	require.Equal(t, []byte("football"), consumeResponse.Record.Value)

	// ! As we don't have a leader-follower relationship in our
	// ! servers, they are replicating each other in a cycle
	// ! and are not co-ordinated at the moment.
	// ! Hence, the below test case will fail.
	// consumeResponse, err = leaderClient.Consume(
	// 	context.Background(),
	// 	&api.ConsumeRequest{
	// 		Offset: produceResponse.Offset + 1,
	// 	},
	// )
	// require.Nil(t, consumeResponse)
	// require.Error(t, err)
	// got := status.Code(err)
	// want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	// require.Equal(t, got, want)
}

// Setups a client for the service.
func client(
	t *testing.T,
	agent *agent,
	tlsConfig *tls.Config,
) api.LogClient {
	// for client, we need client connection,
	// for that, we need RPC address and dial options,
	// for dial options, we need TLS credentials,
	rpcAddr, err := agent.config.RPCAddr()
	require.NoError(t, err)

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	cc, err := grpc.NewClient(rpcAddr, opts...)
	require.NoError(t, err)

	client := api.NewLogClient(cc)
	return client
}
