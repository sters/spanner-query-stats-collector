package stats

import (
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
