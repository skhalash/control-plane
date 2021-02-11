package cls

import (
	"errors"
	"testing"

	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/cls/automock"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/logger"
	smautomock "github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/servicemanager/automock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProvisionCreatesNewInstanceIfNoneFoundInDB(t *testing.T) {
	const (
		fakeGlobalAccountID = "fake-global-account-id"
		fakeSKRInstanceID   = "fake-skr-instance-id"
		fakeBrokerID        = "fake-broker-id"
		fakeServiceID       = "fake-service-id"
		fakePlanID          = "fake-plan-id"
	)

	storageMock := &automock.InstanceStorage{}
	storageMock.On("FindInstance", fakeGlobalAccountID).Return(nil, false, nil)
	storageMock.On("InsertInstance", mock.Anything).Return(nil)

	smClientMock := &smautomock.Client{}
	creatorMock := &automock.InstanceCreator{}
	creatorMock.On("CreateInstance", smClientMock, fakeBrokerID, fakeServiceID, fakePlanID, mock.MatchedBy(func(instanceID string) bool {
		return isValidUUID(instanceID)
	})).Return(nil)

	sut := NewProvisioner(storageMock, creatorMock, logger.NewLogDummy())
	result, err := sut.ProvisionIfNoneExists(smClientMock, &ProvisionRequest{
		GlobalAccountID: fakeGlobalAccountID,
		SKRInstanceID:   fakeSKRInstanceID,
		BrokerID:        fakeBrokerID,
		ServiceID:       fakeServiceID,
		PlanID:          fakePlanID,
	})
	require.NotNil(t, result)
	require.NoError(t, err)

	creatorMock.AssertNumberOfCalls(t, "CreateInstance", 1)
}

func TestProvisionDoesNotCreateNewInstanceIfFindQueryFails(t *testing.T) {
	const (
		fakeGlobalAccountID = "fake-global-account-id"
		fakeSKRInstanceID   = "fake-skr-instance-id"
		fakeBrokerID        = "fake-broker-id"
		fakeServiceID       = "fake-service-id"
		fakePlanID          = "fake-plan-id"
		fakeInstanceID      = "fake-instance-id"
	)

	storageMock := &automock.InstanceStorage{}
	storageMock.On("FindInstance", fakeGlobalAccountID).Return(nil, false, errors.New("unable to connect"))

	smClientMock := &smautomock.Client{}
	creatorMock := &automock.InstanceCreator{}

	sut := NewProvisioner(storageMock, creatorMock, logger.NewLogDummy())
	result, err := sut.ProvisionIfNoneExists(smClientMock, &ProvisionRequest{
		GlobalAccountID: fakeGlobalAccountID,
		SKRInstanceID:   fakeSKRInstanceID,
		BrokerID:        fakeBrokerID,
		ServiceID:       fakeServiceID,
		PlanID:          fakePlanID,
	})
	require.Nil(t, result)
	require.Error(t, err)

	creatorMock.AssertNumberOfCalls(t, "CreateInstance", 0)
}

func TestProvisionDoesNotCreateNewInstanceIfInsertQueryFails(t *testing.T) {
	const (
		fakeGlobalAccountID = "fake-global-account-id"
		fakeSKRInstanceID   = "fake-skr-instance-id"
		fakeBrokerID        = "fake-broker-id"
		fakeServiceID       = "fake-service-id"
		fakePlanID          = "fake-plan-id"
		fakeInstanceID      = "fake-instance-id"
	)

	storageMock := &automock.InstanceStorage{}
	storageMock.On("FindInstance", fakeGlobalAccountID).Return(nil, false, nil)
	storageMock.On("InsertInstance", mock.Anything).Return(errors.New("unable to connect"))

	smClientMock := &smautomock.Client{}
	creatorMock := &automock.InstanceCreator{}

	sut := NewProvisioner(storageMock, creatorMock, logger.NewLogDummy())
	result, err := sut.ProvisionIfNoneExists(smClientMock, &ProvisionRequest{
		GlobalAccountID: fakeGlobalAccountID,
		SKRInstanceID:   fakeSKRInstanceID,
		BrokerID:        fakeBrokerID,
		ServiceID:       fakeServiceID,
		PlanID:          fakePlanID,
	})
	require.Nil(t, result)
	require.Error(t, err)

	creatorMock.AssertNumberOfCalls(t, "CreateInstance", 0)
}

func TestProvisionSavesNewInstanceToDB(t *testing.T) {
	const (
		fakeGlobalAccountID = "fake-global-account-id"
		fakeSKRInstanceID   = "fake-skr-instance-id"
		fakeBrokerID        = "fake-broker-id"
		fakeServiceID       = "fake-service-id"
		fakePlanID          = "fake-plan-id"
	)

	storageMock := &automock.InstanceStorage{}
	storageMock.On("FindInstance", fakeGlobalAccountID).Return(nil, false, nil)
	storageMock.On("InsertInstance", mock.MatchedBy(func(instance internal.CLSInstance) bool {
		return assert.Equal(t, fakeGlobalAccountID, instance.GlobalAccountID) &&
			assert.NotEmpty(t, instance.ID) &&
			assert.Len(t, instance.SKRReferences, 1) &&
			assert.Equal(t, fakeSKRInstanceID, instance.SKRReferences[0])
	})).Return(nil).Once()

	smClientMock := &smautomock.Client{}
	creatorMock := &automock.InstanceCreator{}
	creatorMock.On("CreateInstance", smClientMock, fakeBrokerID, fakeServiceID, fakePlanID, mock.Anything).Return(nil)

	sut := NewProvisioner(storageMock, creatorMock, logger.NewLogDummy())
	sut.ProvisionIfNoneExists(smClientMock, &ProvisionRequest{
		GlobalAccountID: fakeGlobalAccountID,
		SKRInstanceID:   fakeSKRInstanceID,
		BrokerID:        fakeBrokerID,
		ServiceID:       fakeServiceID,
		PlanID:          fakePlanID,
	})

	storageMock.AssertNumberOfCalls(t, "InsertInstance", 1)
}

func TestProvisionAddsReferenceIfFoundInDB(t *testing.T) {
	const (
		fakeGlobalAccountID = "fake-global-account-id"
		fakeSKRInstanceID   = "fake-skr-instance-id"
		fakeBrokerID        = "fake-broker-id"
		fakeServiceID       = "fake-service-id"
		fakePlanID          = "fake-plan-id"
		fakeInstanceID      = "fake-instance-id"
	)

	storageMock := &automock.InstanceStorage{}
	storageMock.On("FindInstance", fakeGlobalAccountID).Return(&internal.CLSInstance{
		GlobalAccountID: fakeGlobalAccountID,
		ID:              fakeInstanceID,
	}, true, nil)
	storageMock.On("AddReference", fakeGlobalAccountID, fakeSKRInstanceID).Return(nil)

	smClientMock := &smautomock.Client{}
	creatorMock := &automock.InstanceCreator{}

	sut := NewProvisioner(storageMock, creatorMock, logger.NewLogDummy())
	result, err := sut.ProvisionIfNoneExists(smClientMock, &ProvisionRequest{
		GlobalAccountID: fakeGlobalAccountID,
		SKRInstanceID:   fakeSKRInstanceID,
		BrokerID:        fakeBrokerID,
		ServiceID:       fakeServiceID,
		PlanID:          fakePlanID,
	})
	require.NotNil(t, result)
	require.NoError(t, err)

	storageMock.AssertNumberOfCalls(t, "AddReference", 1)
	storageMock.AssertNumberOfCalls(t, "InsertInstance", 0)
	creatorMock.AssertNumberOfCalls(t, "CreateInstance", 0)
}
