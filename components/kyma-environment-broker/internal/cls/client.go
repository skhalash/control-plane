package cls

import (
	"github.com/google/uuid"

	"github.com/kyma-project/control-plane/components/kyma-environment-broker/internal/servicemanager"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type parameters struct {
	RetentionPeriod    int  `json:"retentionPeriod"`
	MaxDataInstances   int  `json:"maxDataInstances"`
	MaxIngestInstances int  `json:"maxIngestInstances"`
	EsAPIEnabled       bool `json:"esApiEnabled"`
	SAML               struct {
		Enabled     bool   `json:"enabled"`
		AdminGroup  string `json:"admin_group"`
		Initiated   bool   `json:"initiated"`
		ExchangeKey string `json:"exchange_key"`
		RolesKey    string `json:"roles_key"`
		Idp         struct {
			MetadataURL string `json:"metadata_url"`
			EntityID    string `json:"entity_id"`
		} `json:"idp"`
		Sp struct {
			EntityID            string `json:"entity_id"`
			SignaturePrivateKey string `json:"signature_private_key"`
		} `json:"sp"`
	} `json:"saml"`
}

// Client wraps a generic servicemanager.Client an performs CLS specific calls
type Client struct {
	config *Config
	log    logrus.FieldLogger
}

//NewClient creates a new Client instance
func NewClient(config *Config, log logrus.FieldLogger) *Client {
	return &Client{
		config: config,
		log:    log,
	}
}

type CreateInstanceRequest struct {
	ServiceID string
	PlanID    string
	BrokerID  string
}

// CreateInstance create the CLS Instance
// Instance creation means creation of a cluster, which must be reusable for the same instance/region/project
func (c *Client) CreateInstance(smClient servicemanager.Client, request *CreateInstanceRequest) (string, error) {
	var input servicemanager.ProvisioningInput
	input.ID = uuid.New().String()
	input.ServiceID = request.ServiceID
	input.PlanID = request.PlanID
	input.SpaceGUID = uuid.New().String()
	input.OrganizationGUID = uuid.New().String()
	input.Context = map[string]interface{}{
		"platform": "kubernetes",
	}

	params := parameters{
		RetentionPeriod:    c.config.RetentionPeriod,
		MaxDataInstances:   c.config.MaxDataInstances,
		MaxIngestInstances: c.config.MaxIngestInstances,
		EsAPIEnabled:       false,
	}
	params.SAML.Enabled = true
	params.SAML.AdminGroup = c.config.SAML.AdminGroup
	params.SAML.Initiated = true
	params.SAML.ExchangeKey = c.config.SAML.ExchangeKey
	params.SAML.RolesKey = c.config.SAML.RolesKey
	params.SAML.Idp.EntityID = c.config.SAML.Idp.EntityID
	params.SAML.Idp.MetadataURL = c.config.SAML.Idp.MetadataURL
	params.SAML.Sp.EntityID = c.config.SAML.Sp.EntityID
	params.SAML.Sp.SignaturePrivateKey = c.config.SAML.Sp.SignaturePrivateKey
	input.Parameters = params

	resp, err := smClient.Provision(request.BrokerID, input, true)
	if err != nil {
		return "", errors.Wrapf(err, "Provision() call failed for brokerID: %s; service manager : %#v", request.BrokerID, input)
	}
	c.log.Infof("response from CLS provisioning call: %#v", resp)

	return input.ID, nil
}