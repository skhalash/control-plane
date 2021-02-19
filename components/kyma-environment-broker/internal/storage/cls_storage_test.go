package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal"
	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/storage"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestClsPostgres(t *testing.T) {
	ctx := context.Background()

	cleanupNetwork, err := storage.EnsureTestNetworkForDB(t, ctx)
	require.NoError(t, err)
	defer cleanupNetwork()

	t.Run("CLS", func(t *testing.T) {
		//given
		containerCleanupFunc, cfg, err := storage.InitTestDBContainer(t, ctx, "test_DB_1")
		require.NoError(t, err)
		defer containerCleanupFunc()

		err = storage.InitTestDBTables(t, cfg.ConnectionURL())
		require.NoError(t, err)

		brokerStorage, _, err := storage.NewFromConfig(cfg, logrus.StandardLogger())
		storage := brokerStorage.CLSInstances()
		require.NotNil(t, brokerStorage)
		require.NoError(t, err)

		globalAccountID := "fake-global-account-id"

		//when
		newClsInstance := internal.CLSInstance{
			ID:                       "fake-id",
			GlobalAccountID:          globalAccountID,
			Region:                   "eu",
			CreatedAt:                time.Now().UTC(),
			ReferencedSKRInstanceIDs: []string{"fake-skr-instance-id-1"},
		}
		err = storage.InsertInstance(newClsInstance)
		require.NoError(t, err)

		err = storage.Reference(newClsInstance.Version, globalAccountID, "fake-skr-instance-id-2")
		require.NoError(t, err)

		err = storage.Reference(newClsInstance.Version, globalAccountID, "fake-skr-instance-id-3")
		require.NoError(t, err)

		gotClsInstance, found, err := storage.FindInstance("fake-global-account-id")

		//then
		require.NoError(t, err)
		require.NotNil(t, gotClsInstance)
		require.True(t, found)
		require.Equal(t, newClsInstance.ID, gotClsInstance.ID)
		require.Equal(t, newClsInstance.GlobalAccountID, gotClsInstance.GlobalAccountID)
		require.Equal(t, newClsInstance.Region, gotClsInstance.Region)
		require.ElementsMatch(t, []string{"fake-skr-instance-id-1", "fake-skr-instance-id-2", "fake-skr-instance-id-3"}, gotClsInstance.ReferencedSKRInstanceIDs)
		require.NoError(t, err)
	})
}
