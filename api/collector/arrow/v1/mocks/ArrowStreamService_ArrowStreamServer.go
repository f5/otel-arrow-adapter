// Code generated by mockery v2.14.1. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
	metadata "google.golang.org/grpc/metadata"

	v1 "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
)

// ArrowStreamService_ArrowStreamServer is an autogenerated mock type for the ArrowStreamService_ArrowStreamServer type
type ArrowStreamService_ArrowStreamServer struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *ArrowStreamService_ArrowStreamServer) Context() context.Context {
	ret := _m.Called()

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// Recv provides a mock function with given fields:
func (_m *ArrowStreamService_ArrowStreamServer) Recv() (*v1.BatchArrowRecords, error) {
	ret := _m.Called()

	var r0 *v1.BatchArrowRecords
	if rf, ok := ret.Get(0).(func() *v1.BatchArrowRecords); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*v1.BatchArrowRecords)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RecvMsg provides a mock function with given fields: m
func (_m *ArrowStreamService_ArrowStreamServer) RecvMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: _a0
func (_m *ArrowStreamService_ArrowStreamServer) Send(_a0 *v1.BatchStatus) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*v1.BatchStatus) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendHeader provides a mock function with given fields: _a0
func (_m *ArrowStreamService_ArrowStreamServer) SendHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *ArrowStreamService_ArrowStreamServer) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetHeader provides a mock function with given fields: _a0
func (_m *ArrowStreamService_ArrowStreamServer) SetHeader(_a0 metadata.MD) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(metadata.MD) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetTrailer provides a mock function with given fields: _a0
func (_m *ArrowStreamService_ArrowStreamServer) SetTrailer(_a0 metadata.MD) {
	_m.Called(_a0)
}

type mockConstructorTestingTNewArrowStreamService_ArrowStreamServer interface {
	mock.TestingT
	Cleanup(func())
}

// NewArrowStreamService_ArrowStreamServer creates a new instance of ArrowStreamService_ArrowStreamServer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewArrowStreamService_ArrowStreamServer(t mockConstructorTestingTNewArrowStreamService_ArrowStreamServer) *ArrowStreamService_ArrowStreamServer {
	mock := &ArrowStreamService_ArrowStreamServer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
