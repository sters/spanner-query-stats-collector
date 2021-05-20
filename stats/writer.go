package stats

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
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
	intervalEnd       metric.Int64ValueRecorder
	executionCount    metric.Int64Counter
	avgLatencySeconds metric.Float64ValueRecorder
	avgRows           metric.Float64ValueRecorder
	avgBytes          metric.Float64ValueRecorder
	avgRowsScanned    metric.Float64ValueRecorder
	avgCPUSeconds     metric.Float64ValueRecorder
}

type otelWriterTransaction struct {
	meter    metric.Meter
	measures otelWriterTransactionMeasures
}

type otelWriterTransactionMeasures struct {
	intervalEnd                   metric.Int64ValueRecorder
	commitAttemptCount            metric.Int64Counter
	commitFailedPreconditionCount metric.Int64Counter
	commitAbortCount              metric.Int64Counter
	avgParticipants               metric.Float64ValueRecorder
	avgTotalLatencySeconds        metric.Float64ValueRecorder
	avgCommitLatencySeconds       metric.Float64ValueRecorder
	avgBytes                      metric.Float64ValueRecorder
}

type otelWriterLock struct {
	meter    metric.Meter
	measures otelWriterLockMeasures
}

type otelWriterLockMeasures struct {
	intervalEnd     metric.Int64ValueRecorder
	lockWaitSeconds metric.Float64ValueRecorder
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
				[]attribute.KeyValue{
					attribute.String(
						"Text",
						strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(s.Text),
					),
					attribute.Int64("TextFingerprint", s.TextFingerprint),
				},
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
				[]attribute.KeyValue{
					attribute.String("ReadColumns", strings.Join(s.ReadColumns, ",")),
					attribute.String("WriteConstructiveColumns", strings.Join(s.WriteConstructiveColumns, ",")),
					attribute.String("WriteDeleteTables", strings.Join(s.WriteDeleteTables, ",")),
					attribute.Int64("Fprint", s.Fprint),
				},
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
				[]attribute.KeyValue{
					attribute.String("RowRangeStartKey", string(string(s.RowRangeStartKey))),
					attribute.String("SampleLockRequests", func() string {
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
				},
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
				intervalEnd:       queryMust.NewInt64ValueRecorder(otelMeterNameQuery + ".IntervalEnd"),
				executionCount:    queryMust.NewInt64Counter(otelMeterNameQuery + ".ExecutionCount"),
				avgLatencySeconds: queryMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgLatencySeconds"),
				avgRows:           queryMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgRows"),
				avgBytes:          queryMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgBytes"),
				avgRowsScanned:    queryMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgRowsScanned"),
				avgCPUSeconds:     queryMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgCpuSeconds"),
			},
		},
		transaction: otelWriterTransaction{
			meter: transactionMeter,
			measures: otelWriterTransactionMeasures{
				intervalEnd:                   transactionMust.NewInt64ValueRecorder(otelMeterNameQuery + ".IntervalEnd"),
				commitAttemptCount:            transactionMust.NewInt64Counter(otelMeterNameQuery + ".CommitAttemptCount"),
				commitFailedPreconditionCount: transactionMust.NewInt64Counter(otelMeterNameQuery + ".CommitFailedPreconditionCount"),
				commitAbortCount:              transactionMust.NewInt64Counter(otelMeterNameQuery + ".CommitAbortCount"),
				avgParticipants:               transactionMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgParticipants"),
				avgTotalLatencySeconds:        transactionMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgTotalLatencySeconds"),
				avgCommitLatencySeconds:       transactionMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgCommitLatencySeconds"),
				avgBytes:                      transactionMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".AvgBytes"),
			},
		},
		lock: otelWriterLock{
			meter: transactionMeter,
			measures: otelWriterLockMeasures{
				intervalEnd:     lockMust.NewInt64ValueRecorder(otelMeterNameLock + ".IntervalEnd"),
				lockWaitSeconds: lockMust.NewFloat64ValueRecorder(otelMeterNameQuery + ".LockWaitSeconds"),
			},
		},
	}
}
