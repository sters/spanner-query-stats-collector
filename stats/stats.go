package stats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type statType int

const (
	// StatTypeMin focus 1 minute
	StatTypeMin statType = iota
	// StatType10Min focus 10 minutes
	StatType10Min
	// StatTypeHour focus 1 hour
	StatTypeHour
)

func (s statType) String() string {
	switch s {
	case StatType10Min:
		return "10minute"
	case StatTypeHour:
		return "hour"
	}
	return "minute"
}

func (s statType) Duration() time.Duration {
	switch s {
	case StatType10Min:
		return 10 * time.Minute
	case StatTypeHour:
		return 1 * time.Hour
	}
	return 1 * time.Minute
}

// Stat track the queries with the highest CPU usage during a specific time period
// followed https://cloud.google.com/spanner/docs/query-stats-tables
type Stat struct {
	IntervalEnd       time.Time `spanner:"INTERVAL_END"`
	Text              string    `spanner:"TEXT"`
	TextTruncated     bool      `spanner:"TEXT_TRUNCATED"`
	TextFingerprint   int64     `spanner:"TEXT_FINGERPRINT"`
	ExecutionCount    int64     `spanner:"EXECUTION_COUNT"`
	AvgLatencySeconds float64   `spanner:"AVG_LATENCY_SECONDS"`
	AvgRows           float64   `spanner:"AVG_ROWS"`
	AvgBytes          float64   `spanner:"AVG_BYTES"`
	AvgRowsScanned    float64   `spanner:"AVG_ROWS_SCANNED"`
	AvgCPUSeconds     float64   `spanner:"AVG_CPU_SECONDS"`
}

// GetStats returns Stat collection with specific time period
func (c *Client) GetStats(ctx context.Context, t statType) []*Stat {
	txn, err := c.spannerClient.BatchReadOnlyTransaction(ctx, spanner.ExactStaleness(time.Minute))
	if err != nil {
		return nil
	}
	defer txn.Close()

	iter := txn.Query(ctx, spanner.NewStatement(fmt.Sprintf(
		`SELECT text,
	interval_end,
	execution_count,
	avg_latency_seconds,
	avg_rows,
	avg_bytes,
	avg_rows_scanned,
	avg_cpu_seconds
FROM spanner_sys.query_stats_top_%s
ORDER BY interval_end DESC;`,
		t.String(),
	)))
	defer iter.Stop()

	results := make([]*Stat, 0, iter.RowCount)

	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil
		}

		var b Stat
		err = row.ToStruct(&b)
		if err != nil {
			return nil
		}

		b.Text = strings.TrimSpace(b.Text)
		results = append(results, &b)
	}

	return results
}
