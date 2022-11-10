// Code generated by mockery v2.14.1. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
	metadata "google.golang.org/grpc/metadata"

	v1 "github.com/f5/otel-arrow-adapter/api/collector/arrow/v1"
)

// ArrowStreamService_ArrowStreamClient is an autogenerated mock type for the ArrowStreamService_ArrowStreamClient type
type ArrowStreamService_ArrowStreamClient struct {
	mock.Mock
}

// CloseSend provides a mock function with given fields:
func (_m *ArrowStreamService_ArrowStreamClient) CloseSend() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Context provides a mock function with given fields:
func (_m *ArrowStreamService_ArrowStreamClient) Context() context.Context {
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

// Header provides a mock function with given fields:
func (_m *ArrowStreamService_ArrowStreamClient) Header() (metadata.MD, error) {
	ret := _m.Called()

	var r0 metadata.MD
	if rf, ok := ret.Get(0).(func() metadata.MD); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.MD)
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

// Recv provides a mock function with given fields:
func (_m *ArrowStreamService_ArrowStreamClient) Recv() (*v1.BatchStatus, error) {
	ret := _m.Called()

	var r0 *v1.BatchStatus
	if rf, ok := ret.Get(0).(func() *v1.BatchStatus); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*v1.BatchStatus)
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
func (_m *ArrowStreamService_ArrowStreamClient) RecvMsg(m interface{}) error {
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
func (_m *ArrowStreamService_ArrowStreamClient) Send(_a0 *v1.BatchArrowRecords) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(*v1.BatchArrowRecords) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMsg provides a mock function with given fields: m
func (_m *ArrowStreamService_ArrowStreamClient) SendMsg(m interface{}) error {
	ret := _m.Called(m)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Trailer provides a mock function with given fields:
func (_m *ArrowStreamService_ArrowStreamClient) Trailer() metadata.MD {
	ret := _m.Called()

	var r0 metadata.MD
	if rf, ok := ret.Get(0).(func() metadata.MD); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(metadata.MD)
		}
	}

	return r0
}

type mockConstructorTestingTNewArrowStreamService_ArrowStreamClient interface {
	mock.TestingT
	Cleanup(func())
}

// NewArrowStreamService_ArrowStreamClient creates a new instance of ArrowStreamService_ArrowStreamClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewArrowStreamService_ArrowStreamClient(t mockConstructorTestingTNewArrowStreamService_ArrowStreamClient) *ArrowStreamService_ArrowStreamClient {
	mock := &ArrowStreamService_ArrowStreamClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
