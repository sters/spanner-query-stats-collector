package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/sters/spanner-query-stats-collector/stats"
	"go.opentelemetry.io/otel/api/global"
	metricdogstatsd "go.opentelemetry.io/otel/exporters/metric/dogstatsd"
	metricstdout "go.opentelemetry.io/otel/exporters/metric/stdout"
	"go.opentelemetry.io/otel/sdk/metric/controller/push"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type config struct {
	ProjectID      string `envconfig:"PROJECT_ID" required:"true"`
	InstanceID     string `envconfig:"INSTANCE_ID" required:"true"`
	DatabaseID     string `envconfig:"DATABASE_ID" required:"true"`
	CredentialFile string `envconfig:"CREDENTIAL_FILE"`
	Writer         struct {
		Mode      string `envconfig:"MODE" default:"stdout"`
		DogStatsd struct {
			URL string `envconfig:"URL"`
		} `envconfig:"DOGSTATSD"`
	} `envconfig:"WRITER"`
}

func main() {
	cfg := config{}
	if err := envconfig.Process("", &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse env configure: %s", err)
		os.Exit(1)
	}

	if cfg.CredentialFile == "" {
		fmt.Fprintln(os.Stderr, "*WARNING* Use your default credential file")
	}

	ctx, cancel := context.WithCancel(context.Background())

	client, err := stats.NewClient(ctx, cfg.ProjectID, cfg.InstanceID, cfg.DatabaseID, cfg.CredentialFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var writer stats.Writer

	switch cfg.Writer.Mode {
	case "stdout", "log", "zap":
		zapConfig := zap.NewProductionConfig()
		zapConfig.OutputPaths = []string{"stdout"}
		zapConfig.ErrorOutputPaths = []string{"stderr"}
		logger, _ := zapConfig.Build()
		defer logger.Sync()
		writer = stats.NewZapWriter(logger)

	case "metricstdout":
		defer initMetricstdout().Stop()
		writer = stats.NewOpenTelemetryWriter()

	case "dogstatsd":
		if cfg.Writer.DogStatsd.URL == "" {
			fmt.Fprintln(os.Stderr, "failed to initialize dogstatsd exporter: unexpected dogstatsd URL")
			os.Exit(1)
		}
		// See: https://docs.datadoghq.com/ja/tagging/
		fmt.Fprintln(os.Stderr, "*WARNING* Currently not supported dogstatsd export because SQL can't escaped for dd tags")
		defer initDogstatsd(cfg.Writer.DogStatsd.URL).Stop()
		writer = stats.NewOpenTelemetryWriter()

	default:
		fmt.Fprintf(os.Stderr, "unexpected writer mode: %s", cfg.Writer.Mode)
		os.Exit(1)
	}

	worker := stats.NewWorker(
		client,
		stats.StatTypeMin,
		writer,
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
}

func initMetricstdout() *push.Controller {
	pusher, err := metricstdout.InstallNewPipeline(metricstdout.Config{
		Quantiles:   []float64{1.0},
		PrettyPrint: false,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize metric stdout exporter: %s", err)
		os.Exit(1)
	}

	global.SetMeterProvider(pusher)
	return pusher
}

func initDogstatsd(url string) *push.Controller {
	pusher, err := metricdogstatsd.NewExportPipeline(metricdogstatsd.Config{
		Writer: os.Stdout,
		//URL: url
	}, time.Minute)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize dogstatsd exporter: %s", err)
		os.Exit(1)
	}

	global.SetMeterProvider(pusher)
	return pusher
}
