package server

import (
	"context"

	api "github.com/akkahshh24/proglog/api/v1"
	"google.golang.org/grpc"
)

type commitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	// we can pass a log implementation based on our needs (dev/prod)
	// by having our service depend on a log interface rather than a concrete type
	commitlog commitLog
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
