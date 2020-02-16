package stats

import (
	"context"
	"time"
)

// Worker of stats collector
type Worker struct {
	client          *Client
	statType        statType
	writer          Writer
	ctx             context.Context
	canceler        context.CancelFunc
	lastIntervalEnd time.Time
}

// NewWorker returns the new stats collector
func NewWorker(client *Client, statType statType, writer Writer) *Worker {
	return &Worker{
		client:          client,
		statType:        statType,
		writer:          writer,
		lastIntervalEnd: time.Now().Add(-2 * statType.Duration()),
	}
}

// Start the stats collector
func (w *Worker) Start(ctx context.Context) {
	w.ctx, w.canceler = context.WithCancel(ctx)

	// in the first time, do it as soon as possible.
	w.ticker(w.ctx)

	timer := time.NewTicker(w.statType.Duration())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.ctx.Done():
			return
		case <-timer.C:
			w.ticker(w.ctx)
		}
	}
}

// Stop the stats collector
func (w *Worker) Stop() {
	w.canceler()
}

func (w *Worker) ticker(ctx context.Context) {
	stats := w.client.GetStats(ctx, StatTypeMin)
	if len(stats) == 0 {
		return
	}

	// filter last 1 intervalEnd
	e := stats[0].IntervalEnd
	for i, s := range stats {
		if e != s.IntervalEnd || w.lastIntervalEnd.After(s.IntervalEnd) {
			stats = stats[:i]
			w.lastIntervalEnd = e
			break
		}
	}

	if len(stats) == 0 {
		return
	}
	w.writer.Write(stats)
}
