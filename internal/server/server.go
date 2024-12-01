package server

import (
	"context"
	"time"

	api "github.com/akkahshh24/proglog/api/v1"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type commitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type authorizer interface {
	Authorize(subject, object, action string) error
}

// * Exported: used by agent
type Config struct {
	// we can pass a log implementation based on our needs (dev/prod)
	// by having our service depend on a log interface rather than a concrete type
	Commitlog  commitLog
	Authorizer authorizer
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGRPCServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}

	return srv, nil
}

// func NewGRPCServer(config *Config) (*grpc.Server, error) {
// * Code updated to setup TLS
func NewGRPCServer(config *Config, grpcOpts ...grpc.ServerOption) (
	*grpc.Server,
	error,
) {
	// configure Zap logger
	logger := zap.L().Named("server") // specify logger's name
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			// log the duration of each request in nanoseconds
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	// configure OpenCensus to trace all the requests
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			// chain multiple interceptors in a sequence
			grpc_middleware.ChainStreamServer(
				// add request metadata to the context
				// (eg. Client IP, method name, etc.)
				grpc_ctxtags.StreamServerInterceptor(),
				// log request metadata
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				// set up authentication interceptor for gRPC streaming calls
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		grpc.UnaryInterceptor(
			// chain multiple interceptors in a sequence
			grpc_middleware.ChainUnaryServer(
				// add request metadata to the context
				// (eg. Client IP, method name, etc.)
				grpc_ctxtags.UnaryServerInterceptor(),
				// log request metadata
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
				// set up authentication interceptor for gRPC unary calls
				grpc_auth.UnaryServerInterceptor(authenticate),
			),
		),
		// let OpenCensus capture server's stats
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	// gsrv := grpc.NewServer()
	// * Code updated to setup TLS
	gsrv := grpc.NewServer(grpcOpts...)
	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// Writes the given record to the log
func (s *grpcServer) Produce(
	ctx context.Context,
	req *api.ProduceRequest,
) (*api.ProduceResponse, error) {
	// check if the client is authorized to produce
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.Commitlog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

// Reads the record with the given offset
func (s *grpcServer) Consume(
	ctx context.Context,
	req *api.ConsumeRequest,
) (*api.ConsumeResponse, error) {
	// check if the client is authorized to consume
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.Commitlog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: record}, nil
}

// bi-directional streaming API
func (s *grpcServer) ProduceStream(
	stream api.Log_ProduceStreamServer,
) error {
	for {
		// receive the produce request
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// produce the record
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		// send the produce response
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// server-side streaming API
func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			// read the record with the given offset
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			// send the consume response
			if err = stream.Send(res); err != nil {
				return err
			}

			// this is server-side streaming API
			// it will return all records with offset greater than given offset
			req.Offset++
		}
	}
}

// It is an interceptor that reads the subject of the client's cert
// and writes it to the context
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

type subjectContextKey struct{}

// Returns a client's cert's subject to identify a client
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}
