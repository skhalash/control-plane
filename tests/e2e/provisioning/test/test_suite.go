package test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/kyma-incubator/compass/tests/e2e/provisioning/internal/director"
	"github.com/kyma-incubator/compass/tests/e2e/provisioning/internal/director/oauth"
	"github.com/kyma-incubator/compass/tests/e2e/provisioning/internal/gardener"
	"github.com/kyma-incubator/compass/tests/e2e/provisioning/internal/hyperscaler"
	"github.com/kyma-incubator/compass/tests/e2e/provisioning/internal/hyperscaler/azure"
	"github.com/kyma-incubator/compass/tests/e2e/provisioning/pkg/client/broker"
	"github.com/kyma-incubator/compass/tests/e2e/provisioning/pkg/client/runtime"
	"github.com/kyma-incubator/compass/tests/e2e/provisioning/pkg/client/v1_client"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/uuid"
	gcli "github.com/machinebox/graphql"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vrischmann/envconfig"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

type Config struct {
	Broker   broker.Config
	Runtime  runtime.Config
	Director director.Config
	Gardener gardener.Config

	TenantID             string `default:"d9994f8f-7e46-42a8-b2c1-1bfff8d2fe05"`
	SkipCertVerification bool   `envconfig:"default=true"`

	ProvisionTimeout   time.Duration `default:"3h"`
	DeprovisionTimeout time.Duration `default:"1h"`
	ConfigName         string        `default:"e2e-runtime-config"`
	DeployNamespace    string        `default:"compass-system"`

	DummyTest                 bool `default:"false"`
	CleanupPhase              bool `default:"false"`
	TestAzureEventHubsEnabled bool `default:"true"`
}

// Suite provides set of clients able to provision and test Kyma runtime
type Suite struct {
	t *testing.T

	log             logrus.FieldLogger
	brokerClient    *broker.Client
	runtimeClient   *runtime.Client
	secretClient    v1_client.Secrets
	configMapClient v1_client.ConfigMaps
	accountProvider hyperscaler.AccountProvider
	azureClient     *azure.Interface

	dashboardChecker *runtime.DashboardChecker

	ProvisionTimeout   time.Duration
	DeprovisionTimeout time.Duration

	InstanceID string

	ConfigName      string
	DeployNamespace string

	IsDummyTest                 bool
	IsCleanupPhase              bool
	IsTestAzureEventHubsEnabled bool
}

const (
	instanceIdKey        = "instanceId"
	dashboardUrlKey      = "dashboardUrl"
	kubeconfigKey        = "config"
	subAccountID         = "39ba9a66-2c1a-4fe4-a28e-6e5db434084e"
	DefaultAzureEHRegion = "westeurope"
)

func newTestSuite(t *testing.T) *Suite {
	var azureClient *azure.Interface
	cfg := &Config{}
	err := envconfig.InitWithPrefix(cfg, "APP")
	require.NoError(t, err)

	log := logrus.New()

	k8sConfig, err := config.GetConfig()
	if err != nil {
		panic(err)
	}

	cli, err := client.New(k8sConfig, client.Options{})
	secretClient := v1_client.NewSecretClient(cli, log)
	configMapClient := v1_client.NewConfigMapClient(cli, log)

	instanceID := uuid.New().String()
	if cfg.CleanupPhase {
		cfgMap, err := configMapClient.Get(cfg.ConfigName, cfg.DeployNamespace)
		require.NoError(t, err)

		instanceID = cfgMap.Data[instanceIdKey]
		log.Infof("using instance ID %s", instanceID)
	}

	httpClient := newHTTPClient(cfg.SkipCertVerification)

	// create director client on the base of graphQL client and OAuth client
	graphQLClient := gcli.NewClient(cfg.Director.URL, gcli.WithHTTPClient(httpClient))
	graphQLClient.Log = func(s string) { log.Println(s) }

	oauthClient := oauth.NewOauthClient(httpClient, cli, cfg.Director.OauthCredentialsSecretName, cfg.Director.Namespace)
	err = oauthClient.WaitForCredentials()
	if err != nil {
		panic(err)
	}

	brokerClient := broker.NewClient(cfg.Broker, cfg.TenantID, instanceID, subAccountID, *httpClient, log.WithField("service", "broker_client"))

	directorClient := director.NewDirectorClient(oauthClient, graphQLClient, log.WithField("service", "director_client"))

	runtimeClient := runtime.NewClient(cfg.Runtime, cfg.TenantID, instanceID, *httpClient, directorClient, log.WithField("service", "runtime_client"))

	dashboardChecker := runtime.NewDashboardChecker(*httpClient, log.WithField("service", "dashboard_checker"))

	if cfg.TestAzureEventHubsEnabled {
		azureClient = newAzureClient(t, cfg, brokerClient.GlobalAccountID())
	}

	return &Suite{
		t:   t,
		log: log,

		dashboardChecker: dashboardChecker,
		brokerClient:     brokerClient,
		runtimeClient:    runtimeClient,
		secretClient:     secretClient,
		configMapClient:  configMapClient,
		azureClient:      azureClient,

		InstanceID:         instanceID,
		ProvisionTimeout:   cfg.ProvisionTimeout,
		DeprovisionTimeout: cfg.DeprovisionTimeout,

		ConfigName:      cfg.ConfigName,
		DeployNamespace: cfg.DeployNamespace,

		IsDummyTest:                 cfg.DummyTest,
		IsCleanupPhase:              cfg.CleanupPhase,
		IsTestAzureEventHubsEnabled: cfg.TestAzureEventHubsEnabled,
	}
}

// Cleanup removes all data associated with the test along with runtime
func (ts *Suite) Cleanup() {
	ts.log.Info("Cleaning up...")
	err := ts.cleanupResources()
	assert.NoError(ts.t, err)
	err = ts.runtimeClient.EnsureUAAInstanceRemoved()
	assert.NoError(ts.t, err)
	operationID, err := ts.brokerClient.DeprovisionRuntime()
	require.NoError(ts.t, err)
	err = ts.brokerClient.AwaitOperationSucceeded(operationID, ts.DeprovisionTimeout)
	assert.NoError(ts.t, err)

	if ts.IsTestAzureEventHubsEnabled {
		ts.log.Info("Checking the de-provisioned Azure EventHubs")
		ts.ensureAzureResourceGroupRemoved()
	}
}

// ensureAzureResourceGroupRemoved ensures ResourceGroup is removed which also means EventHub is removed
func (ts *Suite) ensureAzureResourceGroupRemoved() {
	filter := fmt.Sprintf("tagName eq 'InstanceID' and tagValue eq '%s'", ts.InstanceID)
	groupListResultPage, err := (*ts.azureClient).ListResourceGroup(context.TODO(), filter, nil)

	assert.NoError(ts.t, err)
	assert.Equal(ts.t, http.StatusOK, groupListResultPage.Response().StatusCode, "HTTP GET fails for ListResourceGroup")
	assert.Equal(ts.t, 0, len(groupListResultPage.Values()), "groupListResultPage should return 0 ResourceGroup")
}

// cleanupResources removes secret and config map used to store data about the test
func (ts *Suite) cleanupResources() error {
	ts.log.Infof("removing secret %s", ts.ConfigName)
	err := ts.secretClient.Delete(v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ts.ConfigName,
			Namespace: ts.DeployNamespace,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "while waiting for secret %s deletion", ts.ConfigName)
	}

	ts.log.Infof("removing config map %s", ts.ConfigName)
	err = ts.configMapClient.Delete(v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ts.ConfigName,
			Namespace: ts.DeployNamespace,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "while waiting for config map %s deletion", ts.ConfigName)
	}
	return nil
}

func (ts *Suite) testSecret(config *string) v1.Secret {
	return v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ts.ConfigName,
			Namespace: ts.DeployNamespace,
		},
		Data: map[string][]byte{
			kubeconfigKey: []byte(*config),
		},
	}
}

func (ts *Suite) testConfigMap() v1.ConfigMap {
	return v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ts.ConfigName,
			Namespace: ts.DeployNamespace,
		},
		Data: map[string]string{
			instanceIdKey: ts.InstanceID,
		},
	}
}

func newHTTPClient(insecureSkipVerify bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureSkipVerify,
			},
		},
	}
}

func newAzureClient(t *testing.T, cfg *Config, globalAccountID string) *azure.Interface {
	hypType := hyperscaler.Azure

	hyperscalerProvider := azure.NewAzureProvider()

	gardenerClusterConfig, err := gardener.NewGardenerClusterConfig(cfg.Gardener.KubeconfigPath)
	require.NoError(t, err)

	gardenerSecrets, err := gardener.NewGardenerSecretsInterface(gardenerClusterConfig, cfg.Gardener.Project)
	require.NoError(t, err)

	gardenerAccountPool := hyperscaler.NewAccountPool(gardenerSecrets)

	accountProvider := hyperscaler.NewAccountProvider(nil, gardenerAccountPool)

	credentials, err := accountProvider.GardenerCredentials(hypType, globalAccountID)
	assert.NoError(t, err)

	azureCfg, err := azure.GetConfigFromHAPCredentialsAndProvisioningParams(credentials, DefaultAzureEHRegion)
	assert.NoError(t, err)

	azureClient, err := hyperscalerProvider.GetClient(azureCfg)
	assert.NoError(t, err)

	return &azureClient
}
