package server

import (
	"context"

	api "github.com/akkahshh24/proglog/api/v1"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
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

type Config struct {
	// we can pass a log implementation based on our needs (dev/prod)
	// by having our service depend on a log interface rather than a concrete type
	commitlog  commitLog
	authorizer authorizer
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
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (
	*grpc.Server,
	error,
) {
	opts = append(opts,
		// set up authentication interceptor for gRPC streaming calls
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		// set up authentication interceptor for gRPC unary calls
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_auth.UnaryServerInterceptor(authenticate),
			),
		),
	)

	// gsrv := grpc.NewServer()
	// * Code updated to setup TLS
	gsrv := grpc.NewServer(opts...)
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
	if err := s.authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.commitlog.Append(req.Record)
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
	if err := s.authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.commitlog.Read(req.Offset)
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
