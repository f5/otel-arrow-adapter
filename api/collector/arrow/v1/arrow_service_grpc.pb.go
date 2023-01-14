// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
// source: opentelemetry/proto/collector/arrow/v1/arrow_service.proto

package v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ArrowStreamServiceClient is the client API for ArrowStreamService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ArrowStreamServiceClient interface {
	// The ArrowStream endpoint is a bi-directional stream used to send batch of `BatchArrowRecords` from the exporter
	// to the collector. The collector returns `BatchStatus` messages to acknowledge the `BatchArrowRecords` messages received.
	ArrowStream(ctx context.Context, opts ...grpc.CallOption) (ArrowStreamService_ArrowStreamClient, error)
}

type arrowStreamServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewArrowStreamServiceClient(cc grpc.ClientConnInterface) ArrowStreamServiceClient {
	return &arrowStreamServiceClient{cc}
}

func (c *arrowStreamServiceClient) ArrowStream(ctx context.Context, opts ...grpc.CallOption) (ArrowStreamService_ArrowStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &ArrowStreamService_ServiceDesc.Streams[0], "/opentelemetry.proto.collector.arrow.v1.ArrowStreamService/ArrowStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &arrowStreamServiceArrowStreamClient{stream}
	return x, nil
}

type ArrowStreamService_ArrowStreamClient interface {
	Send(*BatchArrowRecords) error
	Recv() (*BatchStatus, error)
	grpc.ClientStream
}

type arrowStreamServiceArrowStreamClient struct {
	grpc.ClientStream
}

func (x *arrowStreamServiceArrowStreamClient) Send(m *BatchArrowRecords) error {
	return x.ClientStream.SendMsg(m)
}

func (x *arrowStreamServiceArrowStreamClient) Recv() (*BatchStatus, error) {
	m := new(BatchStatus)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ArrowStreamServiceServer is the server API for ArrowStreamService service.
// All implementations must embed UnimplementedArrowStreamServiceServer
// for forward compatibility
type ArrowStreamServiceServer interface {
	// The ArrowStream endpoint is a bi-directional stream used to send batch of `BatchArrowRecords` from the exporter
	// to the collector. The collector returns `BatchStatus` messages to acknowledge the `BatchArrowRecords` messages received.
	ArrowStream(ArrowStreamService_ArrowStreamServer) error
	mustEmbedUnimplementedArrowStreamServiceServer()
}

// UnimplementedArrowStreamServiceServer must be embedded to have forward compatible implementations.
type UnimplementedArrowStreamServiceServer struct {
}

func (UnimplementedArrowStreamServiceServer) ArrowStream(ArrowStreamService_ArrowStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method ArrowStream not implemented")
}
func (UnimplementedArrowStreamServiceServer) mustEmbedUnimplementedArrowStreamServiceServer() {}

// UnsafeArrowStreamServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ArrowStreamServiceServer will
// result in compilation errors.
type UnsafeArrowStreamServiceServer interface {
	mustEmbedUnimplementedArrowStreamServiceServer()
}

func RegisterArrowStreamServiceServer(s grpc.ServiceRegistrar, srv ArrowStreamServiceServer) {
	s.RegisterService(&ArrowStreamService_ServiceDesc, srv)
}

func _ArrowStreamService_ArrowStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ArrowStreamServiceServer).ArrowStream(&arrowStreamServiceArrowStreamServer{stream})
}

type ArrowStreamService_ArrowStreamServer interface {
	Send(*BatchStatus) error
	Recv() (*BatchArrowRecords, error)
	grpc.ServerStream
}

type arrowStreamServiceArrowStreamServer struct {
	grpc.ServerStream
}

func (x *arrowStreamServiceArrowStreamServer) Send(m *BatchStatus) error {
	return x.ServerStream.SendMsg(m)
}

func (x *arrowStreamServiceArrowStreamServer) Recv() (*BatchArrowRecords, error) {
	m := new(BatchArrowRecords)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ArrowStreamService_ServiceDesc is the grpc.ServiceDesc for ArrowStreamService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ArrowStreamService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "opentelemetry.proto.collector.arrow.v1.ArrowStreamService",
	HandlerType: (*ArrowStreamServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ArrowStream",
			Handler:       _ArrowStreamService_ArrowStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "opentelemetry/proto/collector/arrow/v1/arrow_service.proto",
}
