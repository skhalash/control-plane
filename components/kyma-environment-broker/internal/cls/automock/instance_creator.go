// Code generated by mockery v2.6.0. DO NOT EDIT.

package automock

import (
	cls "github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/cls"
	mock "github.com/stretchr/testify/mock"

	servicemanager "github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/servicemanager"
)

// InstanceCreator is an autogenerated mock type for the InstanceCreator type
type InstanceCreator struct {
	mock.Mock
}

// CreateInstance provides a mock function with given fields: smClient, request
func (_m *InstanceCreator) CreateInstance(smClient servicemanager.Client, request *cls.CreateInstanceRequest) (string, error) {
	ret := _m.Called(smClient, request)

	var r0 string
	if rf, ok := ret.Get(0).(func(servicemanager.Client, *cls.CreateInstanceRequest) string); ok {
		r0 = rf(smClient, request)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(servicemanager.Client, *cls.CreateInstanceRequest) error); ok {
		r1 = rf(smClient, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
