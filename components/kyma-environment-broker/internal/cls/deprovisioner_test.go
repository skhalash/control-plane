package cls

import (
	"testing"

	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/cls/automock"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/logger"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/servicemanager"
	smautomock "github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/servicemanager/automock"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/storage"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDeprovisionFailsIfFindQueryFails(t *testing.T) {
	// given
	fakeSKRInstanceID := "fake-skr-instance-id"
	fakeInstance := servicemanager.InstanceKey{
		BrokerID:   "fake-broker-id",
		ServiceID:  "fake-service-id",
		PlanID:     "fake-plan-id",
		InstanceID: "fake-instance-id",
	}

	storageMock := &automock.DeprovisionerStorage{}
	storageMock.On("FindByID", fakeInstance.InstanceID).Return(nil, false, errors.New("unable to connect"))

	deprovisioner := &Deprovisioner{
		storage: storageMock,
	}

	smClientMock := &smautomock.Client{}

	// when
	_, err := deprovisioner.Deprovision(smClientMock, &DeprovisionRequest{
		SKRInstanceID: fakeSKRInstanceID,
		Instance:      fakeInstance,
	}, logger.NewLogDummy())

	// then
	require.Error(t, err)
}

func TestDeprovisionRemovesInstanceIfNoCLSFound(t *testing.T) {
	// given
	fakeSKRInstanceID := "fake-skr-instance-id"
	fakeInstance := servicemanager.InstanceKey{
		BrokerID:   "fake-broker-id",
		ServiceID:  "fake-service-id",
		PlanID:     "fake-plan-id",
		InstanceID: "fake-instance-id",
	}

	fakeStorage := storage.NewMemoryStorage().CLSInstances()
	smClientMock := &smautomock.Client{}
	removerMock := &automock.InstanceRemover{}
	removerMock.On("RemoveInstance", smClientMock, fakeInstance).Return(nil)

	deprovisioner := &Deprovisioner{
		remover: removerMock,
		storage: fakeStorage,
	}

	// when
	_, err := deprovisioner.Deprovision(smClientMock, &DeprovisionRequest{
		SKRInstanceID: fakeSKRInstanceID,
		Instance:      fakeInstance,
	}, logger.NewLogDummy())

	// then
	require.NoError(t, err)
	removerMock.AssertNumberOfCalls(t, "RemoveInstance", 1)
}

func TestDeprovisionReturnsEarlyIfCLSNotReferenced(t *testing.T) {
	// given
	fakeSKRInstanceID := "fake-skr-instance-id"
	fakeInstance := servicemanager.InstanceKey{
		BrokerID:   "fake-broker-id",
		ServiceID:  "fake-service-id",
		PlanID:     "fake-plan-id",
		InstanceID: "fake-instance-id",
	}

	found := internal.NewCLSInstance("fake-global-id", "eu",
		internal.WithID(fakeInstance.InstanceID),
		internal.WithReferences("other-fake-skr-instance-id-1", "other-fake-skr-instance-id-2"))
	fakeStorage := storage.NewMemoryStorage().CLSInstances()
	fakeStorage.Insert(*found)

	deprovisioner := &Deprovisioner{
		storage: fakeStorage,
	}

	smClientMock := &smautomock.Client{}

	// when
	_, err := deprovisioner.Deprovision(smClientMock, &DeprovisionRequest{
		SKRInstanceID: fakeSKRInstanceID,
		Instance:      fakeInstance,
	}, logger.NewLogDummy())

	// then
	require.NoError(t, err)
}

func TestDeprovisionUnreferencesIfNotLastCLSReference(t *testing.T) {
	// given
	firstFakeSKRInstanceID := "fake-skr-instance-id-1"
	secondFakeSKRInstanceID := "fake-skr-instance-id-2"
	fakeInstance := servicemanager.InstanceKey{
		BrokerID:   "fake-broker-id",
		ServiceID:  "fake-service-id",
		PlanID:     "fake-plan-id",
		InstanceID: "fake-instance-id",
	}

	found := internal.NewCLSInstance("fake-global-id", "eu",
		internal.WithID(fakeInstance.InstanceID),
		internal.WithReferences(firstFakeSKRInstanceID, secondFakeSKRInstanceID))
	fakeStorage := storage.NewMemoryStorage().CLSInstances()
	fakeStorage.Insert(*found)

	deprovisioner := &Deprovisioner{
		storage: fakeStorage,
	}

	smClientMock := &smautomock.Client{}

	// when
	result, err := deprovisioner.Deprovision(smClientMock, &DeprovisionRequest{
		SKRInstanceID: secondFakeSKRInstanceID,
		Instance:      fakeInstance,
	}, logger.NewLogDummy())

	// then
	require.NoError(t, err)
	require.False(t, result.IsLastReference)

	instance, exists, _ := fakeStorage.FindByID(fakeInstance.InstanceID)
	require.True(t, exists)
	require.ElementsMatch(t, instance.References(), []string{firstFakeSKRInstanceID})
}

func TestDeprovisionFailsIfUpdateQueryFailsAfterCLSUnreferencing(t *testing.T) {
	// given
	fakeSKRInstanceID := "fake-skr-instance-id"
	fakeInstance := servicemanager.InstanceKey{
		BrokerID:   "fake-broker-id",
		ServiceID:  "fake-service-id",
		PlanID:     "fake-plan-id",
		InstanceID: "fake-instance-id",
	}

	found := internal.NewCLSInstance("fake-global-id", "eu",
		internal.WithID(fakeInstance.InstanceID),
		internal.WithReferences(fakeSKRInstanceID))
	storageMock := &automock.DeprovisionerStorage{}
	storageMock.On("FindByID", fakeInstance.InstanceID).Return(found, true, nil)
	storageMock.On("Update", mock.Anything).Return(errors.New("unable to connect"))

	smClientMock := &smautomock.Client{}
	removerMock := &automock.InstanceRemover{}
	removerMock.On("RemoveInstance", smClientMock, fakeInstance).Return(nil)

	deprovisioner := &Deprovisioner{
		storage: storageMock,
		remover: removerMock,
	}

	// when
	_, err := deprovisioner.Deprovision(smClientMock, &DeprovisionRequest{
		SKRInstanceID: fakeSKRInstanceID,
		Instance:      fakeInstance,
	}, logger.NewLogDummy())

	// then
	require.Error(t, err)
	removerMock.AssertNumberOfCalls(t, "RemoveInstance", 0)
}

func TestDeprovisionRemovesIfLastCLSReference(t *testing.T) {
	// given
	fakeSKRInstanceID := "fake-skr-instance-id"
	fakeInstance := servicemanager.InstanceKey{
		BrokerID:   "fake-broker-id",
		ServiceID:  "fake-service-id",
		PlanID:     "fake-plan-id",
		InstanceID: "fake-instance-id",
	}

	found := internal.NewCLSInstance("fake-global-id", "eu",
		internal.WithID(fakeInstance.InstanceID),
		internal.WithReferences(fakeSKRInstanceID))
	fakeStorage := storage.NewMemoryStorage().CLSInstances()
	fakeStorage.Insert(*found)

	smClientMock := &smautomock.Client{}
	removerMock := &automock.InstanceRemover{}
	removerMock.On("RemoveInstance", smClientMock, fakeInstance).Return(nil)

	deprovisioner := &Deprovisioner{
		storage: fakeStorage,
		remover: removerMock,
	}

	// when
	result, err := deprovisioner.Deprovision(smClientMock, &DeprovisionRequest{
		SKRInstanceID: fakeSKRInstanceID,
		Instance:      fakeInstance,
	}, logger.NewLogDummy())

	// then
	require.NoError(t, err)
	require.True(t, result.IsLastReference)

	_, exists, _ := fakeStorage.FindByID(fakeInstance.InstanceID)
	require.False(t, exists)
}

func TestDeprovisionRemovesInstanceIfLastReference(t *testing.T) {
	// given
	fakeSKRInstanceID := "fake-skr-instance-id"
	fakeInstance := servicemanager.InstanceKey{
		BrokerID:   "fake-broker-id",
		ServiceID:  "fake-service-id",
		PlanID:     "fake-plan-id",
		InstanceID: "fake-instance-id",
	}

	found := internal.NewCLSInstance("fake-global-id", "eu",
		internal.WithID(fakeInstance.InstanceID),
		internal.WithReferences(fakeSKRInstanceID))
	fakeStorage := storage.NewMemoryStorage().CLSInstances()
	fakeStorage.Insert(*found)

	smClientMock := &smautomock.Client{}
	removerMock := &automock.InstanceRemover{}
	removerMock.On("RemoveInstance", smClientMock, fakeInstance).Return(nil)

	deprovisioner := &Deprovisioner{
		storage: fakeStorage,
		remover: removerMock,
	}

	// when
	_, err := deprovisioner.Deprovision(smClientMock, &DeprovisionRequest{
		SKRInstanceID: fakeSKRInstanceID,
		Instance:      fakeInstance,
	}, logger.NewLogDummy())

	// then
	require.NoError(t, err)
	removerMock.AssertNumberOfCalls(t, "RemoveInstance", 1)
}
