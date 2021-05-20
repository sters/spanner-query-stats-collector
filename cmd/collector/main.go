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
	"go.opentelemetry.io/contrib/exporters/metric/dogstatsd"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout"
	"go.opentelemetry.io/otel/metric/global"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
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

const (
	collectPeriod = 10 * time.Second
	serviceName   = "spanner-query-stats-collector"
)

func main() {
	if err := realmain(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
}

func realmain() error {
	cfg := config{}
	if err := envconfig.Process("", &cfg); err != nil {
		return fmt.Errorf("failed to parse env configure: %s", err)
	}

	if cfg.CredentialFile == "" {
		fmt.Fprintln(os.Stderr, "*WARNING* Use your default credential file")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := stats.NewClient(ctx, cfg.ProjectID, cfg.InstanceID, cfg.DatabaseID, cfg.CredentialFile)
	if err != nil {
		return err
	}

	fmt.Printf("%#q", cfg)

	var writer stats.Writer

	switch cfg.Writer.Mode {
	case "stdout", "log", "zap":
		zapConfig := zap.NewProductionConfig()
		zapConfig.OutputPaths = []string{"stdout"}
		zapConfig.ErrorOutputPaths = []string{"stderr"}
		logger, _ := zapConfig.Build()
		defer func() { _ = logger.Sync() }()
		writer = stats.NewZapWriter(logger)

	case "metricstdout", "dogstatsd":
		f := map[string](func() (*controller.Controller, error)){
			"metricstdout": func() (*controller.Controller, error) {
				return initMetricstdout(ctx)
			},
			"dogstatsd": func() (*controller.Controller, error) {
				if cfg.Writer.DogStatsd.URL == "" {
					return nil, fmt.Errorf("failed to initialize dogstatsd exporter: unexpected dogstatsd URL")
				}

				return initDogstatsd(ctx, cfg.Writer.DogStatsd.URL)
			},
		}[cfg.Writer.Mode]

		pusher, err := f()
		if err != nil {
			return fmt.Errorf("failed to initialize opentelemetry: %s", err)
		}

		global.SetMeterProvider(pusher.MeterProvider())

		defer func() { _ = pusher.Stop(ctx) }()
		writer = stats.NewOpenTelemetryWriter()

	default:
		return fmt.Errorf("unexpected writer mode: %s", cfg.Writer.Mode)
	}

	worker := stats.NewWorker(
		client,
		stats.StatDurationMin,
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
	return eg.Wait()
}

func otelControllerOptions() []controller.Option {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}

	return []controller.Option{
		controller.WithCollectPeriod(collectPeriod),
		controller.WithResource(
			resource.NewWithAttributes(attribute.String("host", host)),
		),
		controller.WithResource(
			resource.NewWithAttributes(attribute.String("service.name", serviceName)),
		),
	}
}

func initMetricstdout(ctx context.Context) (*controller.Controller, error) {
	exporter, err := stdout.NewExporter(
		stdout.WithPrettyPrint(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metric stdout exporter: %s", err)
	}

	pusher := controller.New(
		processor.New(
			simple.NewWithExactDistribution(),
			exporter,
		),
		append(
			otelControllerOptions(),
			controller.WithExporter(exporter),
		)...,
	)

	err = pusher.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start pusher: %s", err)
	}

	return pusher, nil
}

func initDogstatsd(ctx context.Context, url string) (*controller.Controller, error) {
	// See: https://docs.datadoghq.com/ja/tagging/
	fmt.Fprintln(os.Stderr, "*WARNING* Currently not fully supported dogstatsd export because SQL can't escaped for dd tags")

	pusher, err := dogstatsd.NewExportPipeline(
		dogstatsd.Config{
			// The Writer field provides test support.
			Writer: os.Stdout,

			// URL: fmt.Sprint("unix://", path),
		},
		otelControllerOptions()...,
	)
	if err != nil {
		return nil, fmt.Errorf("could not initialize dogstatsd exporter: %s", err)
	}

	// note: pusher is already started inside dogstatsd package.

	return pusher, nil
}
