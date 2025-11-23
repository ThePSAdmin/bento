package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/warpstreamlabs/bento/internal/impl/kafka"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	kafka.AzureSASLFromConfigFn = func(c *service.ParsedConfig) (sasl.Mechanism, error) {
		nsConf := c.Namespace("azure")

		// Get the broker address to construct the scope
		brokerAddress, err := nsConf.FieldString("broker_address")
		if err != nil {
			return nil, err
		}

		// Event Hub namespace FQDN (e.g., "mynamespace.servicebus.windows.net")
		// The scope for Event Hubs Kafka auth is https://<namespace>.servicebus.windows.net/.default
		scope := fmt.Sprintf("https://%s/.default", brokerAddress)

		// Check if custom scopes are provided
		if nsConf.Contains("scopes") {
			scopes, err := nsConf.FieldStringList("scopes")
			if err != nil {
				return nil, err
			}
			if len(scopes) > 0 {
				// Use the first custom scope if provided
				scope = scopes[0]
			}
		}

		// Create the Azure credential using DefaultAzureCredential
		// This will automatically use WorkloadIdentityCredential when running in AKS with workload identity
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("creating Azure credential: %w", err)
		}

		// Create the OAuth callback function
		oauthFn := func(ctx context.Context) (oauth.Auth, error) {
			tok, err := cred.GetToken(ctx, policy.TokenRequestOptions{
				Scopes: []string{scope},
			})
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("getting Azure token: %w", err)
			}

			return oauth.Auth{
				Token: tok.Token,
				// Zid and Extensions can stay empty for Event Hubs
			}, nil
		}

		return oauth.Oauth(oauthFn), nil
	}
}
