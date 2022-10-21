// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.5
// source: opentelemetry/proto/collector/arrow/v1/arrow_service.proto

package v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Enumeration of all the OTLP Arrow payload types currently supported by the OTLP Arrow protocol.
type OtlpArrowPayloadType int32

const (
	// A payload representing a collection of metrics.
	OtlpArrowPayloadType_METRICS OtlpArrowPayloadType = 0
	// A payload representing a collection of logs.
	OtlpArrowPayloadType_LOGS OtlpArrowPayloadType = 1
	// A payload representing a collection of traces.
	OtlpArrowPayloadType_SPANS OtlpArrowPayloadType = 2
)

// Enum value maps for OtlpArrowPayloadType.
var (
	OtlpArrowPayloadType_name = map[int32]string{
		0: "METRICS",
		1: "LOGS",
		2: "SPANS",
	}
	OtlpArrowPayloadType_value = map[string]int32{
		"METRICS": 0,
		"LOGS":    1,
		"SPANS":   2,
	}
)

func (x OtlpArrowPayloadType) Enum() *OtlpArrowPayloadType {
	p := new(OtlpArrowPayloadType)
	*p = x
	return p
}

func (x OtlpArrowPayloadType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (OtlpArrowPayloadType) Descriptor() protoreflect.EnumDescriptor {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[0].Descriptor()
}

func (OtlpArrowPayloadType) Type() protoreflect.EnumType {
	return &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[0]
}

func (x OtlpArrowPayloadType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use OtlpArrowPayloadType.Descriptor instead.
func (OtlpArrowPayloadType) EnumDescriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{0}
}

// The delivery mode used to process the message.
// The collector must comply with this parameter.
type DeliveryType int32

const (
	DeliveryType_BEST_EFFORT DeliveryType = 0 // Future extension -> AT_LEAST_ONCE = 1;
)

// Enum value maps for DeliveryType.
var (
	DeliveryType_name = map[int32]string{
		0: "BEST_EFFORT",
	}
	DeliveryType_value = map[string]int32{
		"BEST_EFFORT": 0,
	}
)

func (x DeliveryType) Enum() *DeliveryType {
	p := new(DeliveryType)
	*p = x
	return p
}

func (x DeliveryType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (DeliveryType) Descriptor() protoreflect.EnumDescriptor {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[1].Descriptor()
}

func (DeliveryType) Type() protoreflect.EnumType {
	return &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[1]
}

func (x DeliveryType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DeliveryType.Descriptor instead.
func (DeliveryType) EnumDescriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{1}
}

// The compression method used to compress the different bytes buffer.
type CompressionMethod int32

const (
	CompressionMethod_NO_COMPRESSION CompressionMethod = 0
	CompressionMethod_ZSTD           CompressionMethod = 1
)

// Enum value maps for CompressionMethod.
var (
	CompressionMethod_name = map[int32]string{
		0: "NO_COMPRESSION",
		1: "ZSTD",
	}
	CompressionMethod_value = map[string]int32{
		"NO_COMPRESSION": 0,
		"ZSTD":           1,
	}
)

func (x CompressionMethod) Enum() *CompressionMethod {
	p := new(CompressionMethod)
	*p = x
	return p
}

func (x CompressionMethod) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (CompressionMethod) Descriptor() protoreflect.EnumDescriptor {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[2].Descriptor()
}

func (CompressionMethod) Type() protoreflect.EnumType {
	return &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[2]
}

func (x CompressionMethod) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use CompressionMethod.Descriptor instead.
func (CompressionMethod) EnumDescriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{2}
}

type StatusCode int32

const (
	StatusCode_OK    StatusCode = 0
	StatusCode_ERROR StatusCode = 1
)

// Enum value maps for StatusCode.
var (
	StatusCode_name = map[int32]string{
		0: "OK",
		1: "ERROR",
	}
	StatusCode_value = map[string]int32{
		"OK":    0,
		"ERROR": 1,
	}
)

func (x StatusCode) Enum() *StatusCode {
	p := new(StatusCode)
	*p = x
	return p
}

func (x StatusCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (StatusCode) Descriptor() protoreflect.EnumDescriptor {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[3].Descriptor()
}

func (StatusCode) Type() protoreflect.EnumType {
	return &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[3]
}

func (x StatusCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use StatusCode.Descriptor instead.
func (StatusCode) EnumDescriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{3}
}

type ErrorCode int32

const (
	ErrorCode_UNAVAILABLE      ErrorCode = 0
	ErrorCode_INVALID_ARGUMENT ErrorCode = 1
)

// Enum value maps for ErrorCode.
var (
	ErrorCode_name = map[int32]string{
		0: "UNAVAILABLE",
		1: "INVALID_ARGUMENT",
	}
	ErrorCode_value = map[string]int32{
		"UNAVAILABLE":      0,
		"INVALID_ARGUMENT": 1,
	}
)

func (x ErrorCode) Enum() *ErrorCode {
	p := new(ErrorCode)
	*p = x
	return p
}

func (x ErrorCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrorCode) Descriptor() protoreflect.EnumDescriptor {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[4].Descriptor()
}

func (ErrorCode) Type() protoreflect.EnumType {
	return &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes[4]
}

func (x ErrorCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ErrorCode.Descriptor instead.
func (ErrorCode) EnumDescriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{4}
}

// A message sent by an exporter to a collector containing a batch of Arrow records.
type BatchArrowRecords struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// [mandatory] Batch ID. Must be unique in the context of the stream.
	BatchId string `protobuf:"bytes,1,opt,name=batch_id,json=batchId,proto3" json:"batch_id,omitempty"`
	// [mandatory] A collection of payloads containing the data of the batch.
	OtlpArrowPayloads []*OtlpArrowPayload `protobuf:"bytes,2,rep,name=otlp_arrow_payloads,json=otlpArrowPayloads,proto3" json:"otlp_arrow_payloads,omitempty"`
	// [optional] Delivery type (BEST_EFFORT by default).
	DeliveryType DeliveryType `protobuf:"varint,3,opt,name=delivery_type,json=deliveryType,proto3,enum=opentelemetry.proto.collector.arrow.v1.DeliveryType" json:"delivery_type,omitempty"`
}

func (x *BatchArrowRecords) Reset() {
	*x = BatchArrowRecords{}
	if protoimpl.UnsafeEnabled {
		mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchArrowRecords) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchArrowRecords) ProtoMessage() {}

func (x *BatchArrowRecords) ProtoReflect() protoreflect.Message {
	mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchArrowRecords.ProtoReflect.Descriptor instead.
func (*BatchArrowRecords) Descriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{0}
}

func (x *BatchArrowRecords) GetBatchId() string {
	if x != nil {
		return x.BatchId
	}
	return ""
}

func (x *BatchArrowRecords) GetOtlpArrowPayloads() []*OtlpArrowPayload {
	if x != nil {
		return x.OtlpArrowPayloads
	}
	return nil
}

func (x *BatchArrowRecords) GetDeliveryType() DeliveryType {
	if x != nil {
		return x.DeliveryType
	}
	return DeliveryType_BEST_EFFORT
}

// Represents a batch of OTLP Arrow entities.
type OtlpArrowPayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// [mandatory] A unique id assigned to a sub-stream of the batch sharing the same schema, and dictionaries.
	SubStreamId string `protobuf:"bytes,1,opt,name=sub_stream_id,json=subStreamId,proto3" json:"sub_stream_id,omitempty"`
	// [mandatory] Type of the OTLP Arrow payload.
	Type OtlpArrowPayloadType `protobuf:"varint,2,opt,name=type,proto3,enum=opentelemetry.proto.collector.arrow.v1.OtlpArrowPayloadType" json:"type,omitempty"`
	// [optional] Serialized Arrow dictionaries
	Dictionaries []*EncodedData `protobuf:"bytes,3,rep,name=dictionaries,proto3" json:"dictionaries,omitempty"`
	// [mandatory] Serialized Arrow Record Batch
	// For a description of the Arrow IPC format see: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
	Record []byte `protobuf:"bytes,4,opt,name=record,proto3" json:"record,omitempty"`
	// [mandatory]
	Compression CompressionMethod `protobuf:"varint,5,opt,name=compression,proto3,enum=opentelemetry.proto.collector.arrow.v1.CompressionMethod" json:"compression,omitempty"`
}

func (x *OtlpArrowPayload) Reset() {
	*x = OtlpArrowPayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OtlpArrowPayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OtlpArrowPayload) ProtoMessage() {}

func (x *OtlpArrowPayload) ProtoReflect() protoreflect.Message {
	mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OtlpArrowPayload.ProtoReflect.Descriptor instead.
func (*OtlpArrowPayload) Descriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{1}
}

func (x *OtlpArrowPayload) GetSubStreamId() string {
	if x != nil {
		return x.SubStreamId
	}
	return ""
}

func (x *OtlpArrowPayload) GetType() OtlpArrowPayloadType {
	if x != nil {
		return x.Type
	}
	return OtlpArrowPayloadType_METRICS
}

func (x *OtlpArrowPayload) GetDictionaries() []*EncodedData {
	if x != nil {
		return x.Dictionaries
	}
	return nil
}

func (x *OtlpArrowPayload) GetRecord() []byte {
	if x != nil {
		return x.Record
	}
	return nil
}

func (x *OtlpArrowPayload) GetCompression() CompressionMethod {
	if x != nil {
		return x.Compression
	}
	return CompressionMethod_NO_COMPRESSION
}

// Arrow IPC message
// see: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
type EncodedData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Serialized Arrow encoded IPC message
	IpcMessage []byte `protobuf:"bytes,1,opt,name=ipc_message,json=ipcMessage,proto3" json:"ipc_message,omitempty"`
	// Serialized Arrow buffer
	ArrowData []byte `protobuf:"bytes,2,opt,name=arrow_data,json=arrowData,proto3" json:"arrow_data,omitempty"`
}

func (x *EncodedData) Reset() {
	*x = EncodedData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EncodedData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EncodedData) ProtoMessage() {}

func (x *EncodedData) ProtoReflect() protoreflect.Message {
	mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EncodedData.ProtoReflect.Descriptor instead.
func (*EncodedData) Descriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{2}
}

func (x *EncodedData) GetIpcMessage() []byte {
	if x != nil {
		return x.IpcMessage
	}
	return nil
}

func (x *EncodedData) GetArrowData() []byte {
	if x != nil {
		return x.ArrowData
	}
	return nil
}

// A message sent by a Collector to the exporter that opened the Jodata stream.
type BatchStatus struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Statuses []*StatusMessage `protobuf:"bytes,1,rep,name=statuses,proto3" json:"statuses,omitempty"`
}

func (x *BatchStatus) Reset() {
	*x = BatchStatus{}
	if protoimpl.UnsafeEnabled {
		mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchStatus) ProtoMessage() {}

func (x *BatchStatus) ProtoReflect() protoreflect.Message {
	mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchStatus.ProtoReflect.Descriptor instead.
func (*BatchStatus) Descriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{3}
}

func (x *BatchStatus) GetStatuses() []*StatusMessage {
	if x != nil {
		return x.Statuses
	}
	return nil
}

type StatusMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BatchId      string     `protobuf:"bytes,1,opt,name=batch_id,json=batchId,proto3" json:"batch_id,omitempty"`
	StatusCode   StatusCode `protobuf:"varint,2,opt,name=status_code,json=statusCode,proto3,enum=opentelemetry.proto.collector.arrow.v1.StatusCode" json:"status_code,omitempty"`
	ErrorCode    ErrorCode  `protobuf:"varint,3,opt,name=error_code,json=errorCode,proto3,enum=opentelemetry.proto.collector.arrow.v1.ErrorCode" json:"error_code,omitempty"`
	ErrorMessage string     `protobuf:"bytes,4,opt,name=error_message,json=errorMessage,proto3" json:"error_message,omitempty"`
	RetryInfo    *RetryInfo `protobuf:"bytes,5,opt,name=retry_info,json=retryInfo,proto3" json:"retry_info,omitempty"`
}

func (x *StatusMessage) Reset() {
	*x = StatusMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StatusMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatusMessage) ProtoMessage() {}

func (x *StatusMessage) ProtoReflect() protoreflect.Message {
	mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StatusMessage.ProtoReflect.Descriptor instead.
func (*StatusMessage) Descriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{4}
}

func (x *StatusMessage) GetBatchId() string {
	if x != nil {
		return x.BatchId
	}
	return ""
}

func (x *StatusMessage) GetStatusCode() StatusCode {
	if x != nil {
		return x.StatusCode
	}
	return StatusCode_OK
}

func (x *StatusMessage) GetErrorCode() ErrorCode {
	if x != nil {
		return x.ErrorCode
	}
	return ErrorCode_UNAVAILABLE
}

func (x *StatusMessage) GetErrorMessage() string {
	if x != nil {
		return x.ErrorMessage
	}
	return ""
}

func (x *StatusMessage) GetRetryInfo() *RetryInfo {
	if x != nil {
		return x.RetryInfo
	}
	return nil
}

type RetryInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	RetryDelay int64 `protobuf:"varint,1,opt,name=retry_delay,json=retryDelay,proto3" json:"retry_delay,omitempty"`
}

func (x *RetryInfo) Reset() {
	*x = RetryInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RetryInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RetryInfo) ProtoMessage() {}

func (x *RetryInfo) ProtoReflect() protoreflect.Message {
	mi := &file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RetryInfo.ProtoReflect.Descriptor instead.
func (*RetryInfo) Descriptor() ([]byte, []int) {
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP(), []int{5}
}

func (x *RetryInfo) GetRetryDelay() int64 {
	if x != nil {
		return x.RetryDelay
	}
	return 0
}

var File_opentelemetry_proto_collector_arrow_v1_arrow_service_proto protoreflect.FileDescriptor

var file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDesc = []byte{
	0x0a, 0x3a, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2f,
	0x61, 0x72, 0x72, 0x6f, 0x77, 0x2f, 0x76, 0x31, 0x2f, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x5f, 0x73,
	0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x26, 0x6f, 0x70,
	0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f,
	0x77, 0x2e, 0x76, 0x31, 0x22, 0xf3, 0x01, 0x0a, 0x11, 0x42, 0x61, 0x74, 0x63, 0x68, 0x41, 0x72,
	0x72, 0x6f, 0x77, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x12, 0x19, 0x0a, 0x08, 0x62, 0x61,
	0x74, 0x63, 0x68, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x62, 0x61,
	0x74, 0x63, 0x68, 0x49, 0x64, 0x12, 0x68, 0x0a, 0x13, 0x6f, 0x74, 0x6c, 0x70, 0x5f, 0x61, 0x72,
	0x72, 0x6f, 0x77, 0x5f, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x38, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74,
	0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74,
	0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x2e, 0x4f, 0x74, 0x6c, 0x70,
	0x41, 0x72, 0x72, 0x6f, 0x77, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x52, 0x11, 0x6f, 0x74,
	0x6c, 0x70, 0x41, 0x72, 0x72, 0x6f, 0x77, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x73, 0x12,
	0x59, 0x0a, 0x0d, 0x64, 0x65, 0x6c, 0x69, 0x76, 0x65, 0x72, 0x79, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x34, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c,
	0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c,
	0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x2e,
	0x44, 0x65, 0x6c, 0x69, 0x76, 0x65, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0c, 0x64, 0x65,
	0x6c, 0x69, 0x76, 0x65, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65, 0x22, 0xd6, 0x02, 0x0a, 0x10, 0x4f,
	0x74, 0x6c, 0x70, 0x41, 0x72, 0x72, 0x6f, 0x77, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12,
	0x22, 0x0a, 0x0d, 0x73, 0x75, 0x62, 0x5f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x73, 0x75, 0x62, 0x53, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x49, 0x64, 0x12, 0x50, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x3c, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x2e, 0x4f, 0x74, 0x6c, 0x70, 0x41,
	0x72, 0x72, 0x6f, 0x77, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x54, 0x79, 0x70, 0x65, 0x52,
	0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x57, 0x0a, 0x0c, 0x64, 0x69, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x61, 0x72, 0x69, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x33, 0x2e, 0x6f, 0x70,
	0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f,
	0x77, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x64, 0x44, 0x61, 0x74, 0x61,
	0x52, 0x0c, 0x64, 0x69, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x72, 0x69, 0x65, 0x73, 0x12, 0x16,
	0x0a, 0x06, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06,
	0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x12, 0x5b, 0x0a, 0x0b, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65,
	0x73, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x39, 0x2e, 0x6f, 0x70,
	0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f,
	0x77, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e,
	0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x52, 0x0b, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73,
	0x69, 0x6f, 0x6e, 0x22, 0x4d, 0x0a, 0x0b, 0x45, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x64, 0x44, 0x61,
	0x74, 0x61, 0x12, 0x1f, 0x0a, 0x0b, 0x69, 0x70, 0x63, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0a, 0x69, 0x70, 0x63, 0x4d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x5f, 0x64, 0x61, 0x74,
	0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x44, 0x61,
	0x74, 0x61, 0x22, 0x60, 0x0a, 0x0b, 0x42, 0x61, 0x74, 0x63, 0x68, 0x53, 0x74, 0x61, 0x74, 0x75,
	0x73, 0x12, 0x51, 0x0a, 0x08, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x65, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x35, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65,
	0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63,
	0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x52, 0x08, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x65, 0x73, 0x22, 0xc8, 0x02, 0x0a, 0x0d, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x4d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x62, 0x61, 0x74, 0x63, 0x68, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x62, 0x61, 0x74, 0x63, 0x68, 0x49,
	0x64, 0x12, 0x53, 0x0a, 0x0b, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x5f, 0x63, 0x6f, 0x64, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x32, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c,
	0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c,
	0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x2e,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x0a, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x43, 0x6f, 0x64, 0x65, 0x12, 0x50, 0x0a, 0x0a, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x5f,
	0x63, 0x6f, 0x64, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x31, 0x2e, 0x6f, 0x70, 0x65,
	0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77,
	0x2e, 0x76, 0x31, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x09, 0x65,
	0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x65, 0x72, 0x72, 0x6f,
	0x72, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0c, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x50, 0x0a,
	0x0a, 0x72, 0x65, 0x74, 0x72, 0x79, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x31, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x74, 0x72, 0x79,
	0x49, 0x6e, 0x66, 0x6f, 0x52, 0x09, 0x72, 0x65, 0x74, 0x72, 0x79, 0x49, 0x6e, 0x66, 0x6f, 0x22,
	0x2c, 0x0a, 0x09, 0x52, 0x65, 0x74, 0x72, 0x79, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x1f, 0x0a, 0x0b,
	0x72, 0x65, 0x74, 0x72, 0x79, 0x5f, 0x64, 0x65, 0x6c, 0x61, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x0a, 0x72, 0x65, 0x74, 0x72, 0x79, 0x44, 0x65, 0x6c, 0x61, 0x79, 0x2a, 0x38, 0x0a,
	0x14, 0x4f, 0x74, 0x6c, 0x70, 0x41, 0x72, 0x72, 0x6f, 0x77, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61,
	0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0b, 0x0a, 0x07, 0x4d, 0x45, 0x54, 0x52, 0x49, 0x43, 0x53,
	0x10, 0x00, 0x12, 0x08, 0x0a, 0x04, 0x4c, 0x4f, 0x47, 0x53, 0x10, 0x01, 0x12, 0x09, 0x0a, 0x05,
	0x53, 0x50, 0x41, 0x4e, 0x53, 0x10, 0x02, 0x2a, 0x1f, 0x0a, 0x0c, 0x44, 0x65, 0x6c, 0x69, 0x76,
	0x65, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0f, 0x0a, 0x0b, 0x42, 0x45, 0x53, 0x54, 0x5f,
	0x45, 0x46, 0x46, 0x4f, 0x52, 0x54, 0x10, 0x00, 0x2a, 0x31, 0x0a, 0x11, 0x43, 0x6f, 0x6d, 0x70,
	0x72, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x12, 0x12, 0x0a,
	0x0e, 0x4e, 0x4f, 0x5f, 0x43, 0x4f, 0x4d, 0x50, 0x52, 0x45, 0x53, 0x53, 0x49, 0x4f, 0x4e, 0x10,
	0x00, 0x12, 0x08, 0x0a, 0x04, 0x5a, 0x53, 0x54, 0x44, 0x10, 0x01, 0x2a, 0x1f, 0x0a, 0x0a, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x43, 0x6f, 0x64, 0x65, 0x12, 0x06, 0x0a, 0x02, 0x4f, 0x4b, 0x10,
	0x00, 0x12, 0x09, 0x0a, 0x05, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x01, 0x2a, 0x32, 0x0a, 0x09,
	0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x12, 0x0f, 0x0a, 0x0b, 0x55, 0x4e, 0x41,
	0x56, 0x41, 0x49, 0x4c, 0x41, 0x42, 0x4c, 0x45, 0x10, 0x00, 0x12, 0x14, 0x0a, 0x10, 0x49, 0x4e,
	0x56, 0x41, 0x4c, 0x49, 0x44, 0x5f, 0x41, 0x52, 0x47, 0x55, 0x4d, 0x45, 0x4e, 0x54, 0x10, 0x01,
	0x32, 0x9a, 0x01, 0x0a, 0x12, 0x41, 0x72, 0x72, 0x6f, 0x77, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x83, 0x01, 0x0a, 0x0b, 0x41, 0x72, 0x72, 0x6f,
	0x77, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x39, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65,
	0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f,
	0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31,
	0x2e, 0x42, 0x61, 0x74, 0x63, 0x68, 0x41, 0x72, 0x72, 0x6f, 0x77, 0x52, 0x65, 0x63, 0x6f, 0x72,
	0x64, 0x73, 0x1a, 0x33, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74,
	0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74,
	0x6f, 0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x2e, 0x42, 0x61, 0x74, 0x63,
	0x68, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0x00, 0x28, 0x01, 0x30, 0x01, 0x42, 0x7e, 0x0a,
	0x29, 0x69, 0x6f, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x74, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x74, 0x72,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x2e, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2e, 0x76, 0x31, 0x42, 0x11, 0x41, 0x72, 0x72, 0x6f,
	0x77, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a,
	0x3c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6c, 0x71, 0x75, 0x65,
	0x72, 0x65, 0x6c, 0x2f, 0x6f, 0x74, 0x65, 0x6c, 0x2d, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2d, 0x61,
	0x64, 0x61, 0x70, 0x74, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6c, 0x6c, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x2f, 0x61, 0x72, 0x72, 0x6f, 0x77, 0x2f, 0x76, 0x31, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescOnce sync.Once
	file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescData = file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDesc
)

func file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescGZIP() []byte {
	file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescOnce.Do(func() {
		file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescData = protoimpl.X.CompressGZIP(file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescData)
	})
	return file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDescData
}

var file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes = make([]protoimpl.EnumInfo, 5)
var file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_goTypes = []interface{}{
	(OtlpArrowPayloadType)(0), // 0: opentelemetry.proto.collector.arrow.v1.OtlpArrowPayloadType
	(DeliveryType)(0),         // 1: opentelemetry.proto.collector.arrow.v1.DeliveryType
	(CompressionMethod)(0),    // 2: opentelemetry.proto.collector.arrow.v1.CompressionMethod
	(StatusCode)(0),           // 3: opentelemetry.proto.collector.arrow.v1.StatusCode
	(ErrorCode)(0),            // 4: opentelemetry.proto.collector.arrow.v1.ErrorCode
	(*BatchArrowRecords)(nil), // 5: opentelemetry.proto.collector.arrow.v1.BatchArrowRecords
	(*OtlpArrowPayload)(nil),  // 6: opentelemetry.proto.collector.arrow.v1.OtlpArrowPayload
	(*EncodedData)(nil),       // 7: opentelemetry.proto.collector.arrow.v1.EncodedData
	(*BatchStatus)(nil),       // 8: opentelemetry.proto.collector.arrow.v1.BatchStatus
	(*StatusMessage)(nil),     // 9: opentelemetry.proto.collector.arrow.v1.StatusMessage
	(*RetryInfo)(nil),         // 10: opentelemetry.proto.collector.arrow.v1.RetryInfo
}
var file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_depIdxs = []int32{
	6,  // 0: opentelemetry.proto.collector.arrow.v1.BatchArrowRecords.otlp_arrow_payloads:type_name -> opentelemetry.proto.collector.arrow.v1.OtlpArrowPayload
	1,  // 1: opentelemetry.proto.collector.arrow.v1.BatchArrowRecords.delivery_type:type_name -> opentelemetry.proto.collector.arrow.v1.DeliveryType
	0,  // 2: opentelemetry.proto.collector.arrow.v1.OtlpArrowPayload.type:type_name -> opentelemetry.proto.collector.arrow.v1.OtlpArrowPayloadType
	7,  // 3: opentelemetry.proto.collector.arrow.v1.OtlpArrowPayload.dictionaries:type_name -> opentelemetry.proto.collector.arrow.v1.EncodedData
	2,  // 4: opentelemetry.proto.collector.arrow.v1.OtlpArrowPayload.compression:type_name -> opentelemetry.proto.collector.arrow.v1.CompressionMethod
	9,  // 5: opentelemetry.proto.collector.arrow.v1.BatchStatus.statuses:type_name -> opentelemetry.proto.collector.arrow.v1.StatusMessage
	3,  // 6: opentelemetry.proto.collector.arrow.v1.StatusMessage.status_code:type_name -> opentelemetry.proto.collector.arrow.v1.StatusCode
	4,  // 7: opentelemetry.proto.collector.arrow.v1.StatusMessage.error_code:type_name -> opentelemetry.proto.collector.arrow.v1.ErrorCode
	10, // 8: opentelemetry.proto.collector.arrow.v1.StatusMessage.retry_info:type_name -> opentelemetry.proto.collector.arrow.v1.RetryInfo
	5,  // 9: opentelemetry.proto.collector.arrow.v1.ArrowStreamService.ArrowStream:input_type -> opentelemetry.proto.collector.arrow.v1.BatchArrowRecords
	8,  // 10: opentelemetry.proto.collector.arrow.v1.ArrowStreamService.ArrowStream:output_type -> opentelemetry.proto.collector.arrow.v1.BatchStatus
	10, // [10:11] is the sub-list for method output_type
	9,  // [9:10] is the sub-list for method input_type
	9,  // [9:9] is the sub-list for extension type_name
	9,  // [9:9] is the sub-list for extension extendee
	0,  // [0:9] is the sub-list for field type_name
}

func init() { file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_init() }
func file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_init() {
	if File_opentelemetry_proto_collector_arrow_v1_arrow_service_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchArrowRecords); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OtlpArrowPayload); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EncodedData); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchStatus); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StatusMessage); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RetryInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDesc,
			NumEnums:      5,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_goTypes,
		DependencyIndexes: file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_depIdxs,
		EnumInfos:         file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_enumTypes,
		MessageInfos:      file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_msgTypes,
	}.Build()
	File_opentelemetry_proto_collector_arrow_v1_arrow_service_proto = out.File
	file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_rawDesc = nil
	file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_goTypes = nil
	file_opentelemetry_proto_collector_arrow_v1_arrow_service_proto_depIdxs = nil
}
