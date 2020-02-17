package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sters/spanner-stats-collector/stats"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func main() {
	projectID := flag.String("project_id", "", "Google Cloud Platform's PROJECT_ID")
	instanceID := flag.String("instance_id", "", "Google Cloud Spanner's INSTANCE_ID")
	databaseID := flag.String("database_id", "", "Google Cloud Spanner's DATABASE_ID")
	credentialFile := flag.String("credential", "", "Google Cloud Platform's Credential file a.k.a. IAM Key file")
	flag.Parse()

	if *projectID == "" || *instanceID == "" || *databaseID == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *credentialFile == "" {
		fmt.Fprintln(os.Stderr, "*WARNING* Use your default credential file")
	}

	ctx, cancel := context.WithCancel(context.Background())

	client, err := stats.NewClient(ctx, *projectID, *instanceID, *databaseID, *credentialFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	zapConfig := zap.NewProductionConfig()
	zapConfig.OutputPaths = []string{"stdout"}
	zapConfig.ErrorOutputPaths = []string{"stderr"}
	logger, _ := zapConfig.Build()

	worker := stats.NewWorker(
		client,
		stats.StatTypeMin,
		stats.NewZapWriter(logger),
	)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { worker.Start(ctx); return nil })

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, os.Interrupt)
	select {
	case <-sigCh:
	case <-ctx.Done():
	}

	worker.Stop()
	cancel()
	eg.Wait()
	logger.Sync()
}
