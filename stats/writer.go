package stats

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/key"
	"go.opentelemetry.io/otel/api/metric"
	"go.uber.org/zap"
)

// Writer for stats collection
type Writer interface {
	// Write stats collection to anything
	Write([]stat)
}

type zapWriter struct {
	logger *zap.Logger
}

func (w *zapWriter) Write(stats []stat) {
	for _, s := range stats {
		w.logger.Info("spanner stats", w.getFields(s)...)
	}
}

func (w *zapWriter) getFields(s stat) []zap.Field {
	switch s := s.(type) {
	case *QueryStat:
		return []zap.Field{
			zap.String("type", "QueryStat"),
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
		}

	case *TransactionStat:
		return []zap.Field{
			zap.String("type", "TransactionStat"),
			zap.Time("IntervalEnd", s.IntervalEnd),
			zap.Int64("Fprint", s.Fprint),
			zap.Strings("ReadColumns", s.ReadColumns),
			zap.Strings("WriteConstructiveColumns", s.WriteConstructiveColumns),
			zap.Strings("WriteDeleteTables", s.WriteDeleteTables),
			zap.Int64("CommitAttemptCount", s.CommitAttemptCount),
			zap.Int64("CommitFailedPreconditionCount", s.CommitFailedPreconditionCount),
			zap.Int64("CommitAbortCount", s.CommitAbortCount),
			zap.Float64("AvgParticipants", s.AvgParticipants),
			zap.Float64("AvgTotalLatencySeconds", s.AvgTotalLatencySeconds),
			zap.Float64("AvgCommitLatencySeconds", s.AvgCommitLatencySeconds),
			zap.Float64("AvgBytes", s.AvgBytes),
		}

	case *LockStat:
		return []zap.Field{
			zap.String("type", "LockStat"),
			zap.Time("IntervalEnd", s.IntervalEnd),
			zap.ByteString("RowRangeStartKey", s.RowRangeStartKey),
			zap.Float64("LockWaitSeconds", s.LockWaitSeconds),
			zap.Any("SampleLockRequests", s.SampleLockRequests),
		}
	}

	return nil
}

// NewZapWriter return new Writer of *zap.Logger
func NewZapWriter(logger *zap.Logger) Writer {
	return &zapWriter{
		logger: logger,
	}
}

type otelWriter struct {
	query       otelWriterQuery
	transaction otelWriterTransaction
	lock        otelWriterLock
}

type otelWriterQuery struct {
	meter    metric.Meter
	measures otelWriterQueryMeasures
}

type otelWriterQueryMeasures struct {
	intervalEnd       metric.Int64Measure
	executionCount    metric.Int64Counter
	avgLatencySeconds metric.Float64Measure
	avgRows           metric.Float64Measure
	avgBytes          metric.Float64Measure
	avgRowsScanned    metric.Float64Measure
	avgCPUSeconds     metric.Float64Measure
}

type otelWriterTransaction struct {
	meter    metric.Meter
	measures otelWriterTransactionMeasures
}

type otelWriterTransactionMeasures struct {
	intervalEnd                   metric.Int64Measure
	commitAttemptCount            metric.Int64Measure
	commitFailedPreconditionCount metric.Int64Measure
	commitAbortCount              metric.Int64Measure
	avgParticipants               metric.Float64Measure
	avgTotalLatencySeconds        metric.Float64Measure
	avgCommitLatencySeconds       metric.Float64Measure
	avgBytes                      metric.Float64Measure
}

type otelWriterLock struct {
	meter    metric.Meter
	measures otelWriterLockMeasures
}

type otelWriterLockMeasures struct {
	intervalEnd     metric.Int64Measure
	lockWaitSeconds metric.Float64Measure
}

const (
	otelMeterNameQuery       = "spanner.stats.query"
	otelMeterNameTransaction = "spanner.stats.transaction"
	otelMeterNameLock        = "spanner.stats.lock"
)

func (w *otelWriter) Write(stats []stat) {
	for _, s := range stats {
		switch s := s.(type) {
		case *QueryStat:
			w.query.meter.RecordBatch(
				context.Background(),
				w.query.meter.Labels(
					key.String(
						"Text",
						strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(s.Text),
					),
					key.Int64("TextFingerprint", s.TextFingerprint),
				),
				w.query.measures.intervalEnd.Measurement(s.IntervalEnd.UnixNano()),
				w.query.measures.executionCount.Measurement(s.ExecutionCount),
				w.query.measures.avgLatencySeconds.Measurement(s.AvgLatencySeconds),
				w.query.measures.avgRows.Measurement(s.AvgRows),
				w.query.measures.avgBytes.Measurement(s.AvgBytes),
				w.query.measures.avgRowsScanned.Measurement(s.AvgRowsScanned),
				w.query.measures.avgCPUSeconds.Measurement(s.AvgCPUSeconds),
			)

		case *TransactionStat:
			w.transaction.meter.RecordBatch(
				context.Background(),
				w.transaction.meter.Labels(
					key.String("ReadColumns", strings.Join(s.ReadColumns, ",")),
					key.String("WriteConstructiveColumns", strings.Join(s.WriteConstructiveColumns, ",")),
					key.String("WriteDeleteTables", strings.Join(s.WriteDeleteTables, ",")),
					key.Int64("Fprint", s.Fprint),
				),
				w.transaction.measures.intervalEnd.Measurement(s.IntervalEnd.UnixNano()),
				w.transaction.measures.commitAttemptCount.Measurement(s.CommitAttemptCount),
				w.transaction.measures.commitFailedPreconditionCount.Measurement(s.CommitFailedPreconditionCount),
				w.transaction.measures.commitAbortCount.Measurement(s.CommitAbortCount),
				w.transaction.measures.avgParticipants.Measurement(s.AvgParticipants),
				w.transaction.measures.avgTotalLatencySeconds.Measurement(s.AvgTotalLatencySeconds),
				w.transaction.measures.avgCommitLatencySeconds.Measurement(s.AvgCommitLatencySeconds),
				w.transaction.measures.avgBytes.Measurement(s.AvgBytes),
			)

		case *LockStat:
			w.lock.meter.RecordBatch(
				context.Background(),
				w.lock.meter.Labels(
					key.String("RowRangeStartKey", string(string(s.RowRangeStartKey))),
					key.String("SampleLockRequests", func() string {
						result := strings.Builder{}
						for _, l := range s.SampleLockRequests {
							result.WriteRune('(')
							result.WriteString(l.Column)
							result.WriteRune(',')
							result.WriteString(l.LockMode)
							result.WriteString("),")
						}
						return result.String()
					}()),
				),
				w.lock.measures.intervalEnd.Measurement(s.IntervalEnd.UnixNano()),
				w.lock.measures.lockWaitSeconds.Measurement(s.LockWaitSeconds),
			)
		}
	}
}

// NewOpenTelemetryWriter return new Writer of OpenTelemetry
func NewOpenTelemetryWriter() Writer {
	queryMeter := global.Meter(otelMeterNameQuery)
	queryMust := metric.Must(queryMeter)
	transactionMeter := global.Meter(otelMeterNameTransaction)
	transactionMust := metric.Must(transactionMeter)
	lockMeter := global.Meter(otelMeterNameLock)
	lockMust := metric.Must(lockMeter)

	return &otelWriter{
		query: otelWriterQuery{
			meter: queryMeter,
			measures: otelWriterQueryMeasures{
				intervalEnd:       queryMust.NewInt64Measure(otelMeterNameQuery + ".IntervalEnd"),
				executionCount:    queryMust.NewInt64Counter(otelMeterNameQuery + ".ExecutionCount"),
				avgLatencySeconds: queryMust.NewFloat64Measure(otelMeterNameQuery + ".AvgLatencySeconds"),
				avgRows:           queryMust.NewFloat64Measure(otelMeterNameQuery + ".AvgRows"),
				avgBytes:          queryMust.NewFloat64Measure(otelMeterNameQuery + ".AvgBytes"),
				avgRowsScanned:    queryMust.NewFloat64Measure(otelMeterNameQuery + ".AvgRowsScanned"),
				avgCPUSeconds:     queryMust.NewFloat64Measure(otelMeterNameQuery + ".AvgCpuSeconds"),
			},
		},
		transaction: otelWriterTransaction{
			meter: transactionMeter,
			measures: otelWriterTransactionMeasures{
				intervalEnd:                   transactionMust.NewInt64Measure(otelMeterNameQuery + ".IntervalEnd"),
				commitAttemptCount:            transactionMust.NewInt64Measure(otelMeterNameQuery + ".CommitAttemptCount"),
				commitFailedPreconditionCount: transactionMust.NewInt64Measure(otelMeterNameQuery + ".CommitFailedPreconditionCount"),
				commitAbortCount:              transactionMust.NewInt64Measure(otelMeterNameQuery + ".CommitAbortCount"),
				avgParticipants:               transactionMust.NewFloat64Measure(otelMeterNameQuery + ".AvgParticipants"),
				avgTotalLatencySeconds:        transactionMust.NewFloat64Measure(otelMeterNameQuery + ".AvgTotalLatencySeconds"),
				avgCommitLatencySeconds:       transactionMust.NewFloat64Measure(otelMeterNameQuery + ".AvgCommitLatencySeconds"),
				avgBytes:                      transactionMust.NewFloat64Measure(otelMeterNameQuery + ".AvgBytes"),
			},
		},
		lock: otelWriterLock{
			meter: transactionMeter,
			measures: otelWriterLockMeasures{
				intervalEnd:     lockMust.NewInt64Measure(otelMeterNameLock + ".IntervalEnd"),
				lockWaitSeconds: lockMust.NewFloat64Measure(otelMeterNameQuery + ".LockWaitSeconds"),
			},
		},
	}
}
