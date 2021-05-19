package stats

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// Worker of stats collector
type Worker struct {
	client          *Client
	statType        statDuration
	writer          Writer
	ctx             context.Context
	canceler        context.CancelFunc
	lastIntervalEnd time.Time
}

// NewWorker returns the new stats collector
func NewWorker(client *Client, statType statDuration, writer Writer) *Worker {
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
	eg, ctx := errgroup.WithContext(ctx)

	getters := []statGetter{
		w.client.GetQueryStats,
		w.client.GetTransactionStats,
		w.client.GetQueryStats,
	}
	for _, getter := range getters {
		getter := getter
		eg.Go(func() error {
			stats := w.getStat(ctx, getter)
			if len(stats) == 0 {
				return nil
			}
			w.writer.Write(stats)

			return nil
		})
	}

	eg.Wait()
}

func (w *Worker) getStat(
	ctx context.Context,
	getter statGetter,
) []stat {
	stats := getter(ctx, w.statType, w.lastIntervalEnd)
	if len(stats) == 0 {
		return nil
	}

	// filter last 1 intervalEnd
	e := stats[0].getIntervalEnd()
	for i, s := range stats {
		if e != s.getIntervalEnd() || w.lastIntervalEnd.After(s.getIntervalEnd()) {
			stats = stats[:i]
			w.lastIntervalEnd = e
			break
		}
	}

	return stats
}
