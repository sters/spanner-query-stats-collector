package stats

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

// Client wrapped *spanner.Client for easy to use stats collect
type Client struct {
	spannerClient *spanner.Client
}

// NewClient is constructor of Client
func NewClient(ctx context.Context, projectID, instanceID, databaseID, credentialFile string) *Client {
	client, err := spanner.NewClientWithConfig(
		ctx,
		fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
		spanner.ClientConfig{
			NumChannels:       2,
			SessionPoolConfig: spanner.SessionPoolConfig{MinOpened: 2, MaxOpened: 2},
		},
		option.WithCredentialsFile(credentialFile),
	)
	if err != nil {
		return nil
	}

	return &Client{
		spannerClient: client,
	}
}
