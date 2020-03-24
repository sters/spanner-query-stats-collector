package stats

import (
	"context"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/key"
	"go.opentelemetry.io/otel/api/metric"
	"go.uber.org/zap"
)

// Writer for stats collection
type Writer interface {
	// Write stats collection to anything
	Write([]*Stat)
}

type zapWriter struct {
	logger *zap.Logger
}

func (w *zapWriter) Write(stats []*Stat) {
	for _, s := range stats {
		w.logger.Info(
			"",
			zap.Time("IntervalEnd", s.IntervalEnd),
			zap.String("Text", s.Text),
			zap.Bool("TextTruncated", s.TextTruncated),
			zap.Int64("TextFingerprint", s.TextFingerprint),
			zap.Int64("ExecutionCount", s.ExecutionCount),
			zap.Float64("AvgLatencySeconds", s.AvgLatencySeconds),
			zap.Float64("AvgRows", s.AvgRows),
			zap.Float64("AvgBytes", s.AvgBytes),
			zap.Float64("AvgRowsScanned", s.AvgRowsScanned),
			zap.Float64("AvgCPUSeconds", s.AvgCPUSeconds),
		)
	}
}

// NewZapWriter return new Writer of *zap.Logger
func NewZapWriter(logger *zap.Logger) Writer {
	return &zapWriter{
		logger: logger,
	}
}

type otelWriter struct {
	meter    metric.Meter
	measures otelMeasures
}

type otelMeasures struct {
	intervalEnd       metric.Int64Measure
	executionCount    metric.Int64Counter
	avgLatencySeconds metric.Float64Measure
	avgRows           metric.Float64Measure
	avgBytes          metric.Float64Measure
	avgRowsScanned    metric.Float64Measure
	avgCPUSeconds     metric.Float64Measure
}

func (w *otelWriter) Write(stats []*Stat) {
	for _, s := range stats {
		w.meter.RecordBatch(
			context.Background(),
			w.meter.Labels(
				key.String("Text", s.Text),
				key.Int64("TextFingerprint", s.TextFingerprint),
			),
			w.measures.intervalEnd.Measurement(s.IntervalEnd.UnixNano()),
			w.measures.executionCount.Measurement(s.ExecutionCount),
			w.measures.avgLatencySeconds.Measurement(s.AvgLatencySeconds),
			w.measures.avgRows.Measurement(s.AvgRows),
			w.measures.avgBytes.Measurement(s.AvgBytes),
			w.measures.avgRowsScanned.Measurement(s.AvgRowsScanned),
			w.measures.avgCPUSeconds.Measurement(s.AvgCPUSeconds),
		)
	}
}

// NewOpenTelemetryWriter return new Writer of OpenTelemetry
func NewOpenTelemetryWriter() Writer {
	const name = "spanner-query-stats-collector"

	meter := global.Meter(name)
	must := metric.Must(meter)

	return &otelWriter{
		meter: meter,
		measures: otelMeasures{
			intervalEnd:       must.NewInt64Measure("IntervalEnd"),
			executionCount:    must.NewInt64Counter("ExecutionCount"),
			avgLatencySeconds: must.NewFloat64Measure("AvgLatencySeconds"),
			avgRows:           must.NewFloat64Measure("AvgRows"),
			avgBytes:          must.NewFloat64Measure("AvgBytes"),
			avgRowsScanned:    must.NewFloat64Measure("AvgRowsScanned"),
			avgCPUSeconds:     must.NewFloat64Measure("AvgCpuSeconds"),
		},
	}
}
