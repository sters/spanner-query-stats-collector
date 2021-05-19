package stats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type statDuration int

const (
	// StatDurationMin focus 1 minute
	StatDurationMin statDuration = iota
	// StatDuration10Min focus 10 minutes
	StatDuration10Min
	// StatDurationHour focus 1 hour
	StatDurationHour
)

func (s statDuration) String() string {
	switch s {
	case StatDuration10Min:
		return "10minute"
	case StatDurationHour:
		return "hour"
	}
	return "minute"
}

func (s statDuration) Duration() time.Duration {
	switch s {
	case StatDuration10Min:
		return 10 * time.Minute
	case StatDurationHour:
		return 1 * time.Hour
	}
	return 1 * time.Minute
}

type stat interface {
	getIntervalEnd() time.Time
}

type statGetter func(context.Context, statDuration, time.Time) []stat

// QueryStat track the queries with the highest CPU usage during a specific time period
// followed https://cloud.google.com/spanner/docs/query-stats-tables
type QueryStat struct {
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

func (q *QueryStat) getIntervalEnd() time.Time {
	return q.IntervalEnd
}

// GetQueryStats returns Stat collection with specific time period
func (c *Client) GetQueryStats(ctx context.Context, t statDuration, lastIntervalEnd time.Time) []stat {
	txn, err := c.spannerClient.BatchReadOnlyTransaction(ctx, spanner.ExactStaleness(time.Minute))
	if err != nil {
		return nil
	}
	defer txn.Close()

	stmt := spanner.NewStatement(fmt.Sprintf(
		`SELECT text,
	interval_end,
	execution_count,
	avg_latency_seconds,
	avg_rows,
	avg_bytes,
	avg_rows_scanned,
	avg_cpu_seconds
FROM spanner_sys.query_stats_top_%s
WHERE interval_end > @last_interval_end
ORDER BY interval_end DESC;`,
		t.String(),
	))
	stmt.Params["last_interval_end"] = lastIntervalEnd

	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	var results []stat

	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil
		}

		var b QueryStat
		err = row.ToStruct(&b)
		if err != nil {
			return nil
		}

		b.Text = strings.TrimSpace(b.Text)
		results = append(results, &b)
	}

	return results
}

// TransactionStat track the transactions during a specific time period
// followed https://cloud.google.com/spanner/docs/introspection/transaction-statistics
type TransactionStat struct {
	IntervalEnd                   time.Time `spanner:"INTERVAL_END"`
	Fprint                        int64     `spanner:"FPRINT"`
	ReadColumns                   []string  `spanner:"READ_COLUMNS"`
	WriteConstructiveColumns      []string  `spanner:"WRITE_CONSTRUCTIVE_COLUMNS"`
	WriteDeleteTables             []string  `spanner:"WRITE_DELETE_TABLES"`
	CommitAttemptCount            int64     `spanner:"COMMIT_ATTEMPT_COUNT"`
	CommitFailedPreconditionCount int64     `spanner:"COMMIT_FAILED_PRECONDITION_COUNT"`
	CommitAbortCount              int64     `spanner:"COMMIT_ABORT_COUNT"`
	AvgParticipants               float64   `spanner:"AVG_PARTICIPANTS"`
	AvgTotalLatencySeconds        float64   `spanner:"AVG_TOTAL_LATENCY_SECONDS"`
	AvgCommitLatencySeconds       float64   `spanner:"AVG_COMMIT_LATENCY_SECONDS"`
	AvgBytes                      float64   `spanner:"AVG_BYTES"`
}

func (q *TransactionStat) getIntervalEnd() time.Time {
	return q.IntervalEnd
}

// GetTransactionStats returns stat collection with specific time period
func (c *Client) GetTransactionStats(ctx context.Context, t statDuration, lastIntervalEnd time.Time) []stat {
	txn, err := c.spannerClient.BatchReadOnlyTransaction(ctx, spanner.ExactStaleness(time.Minute))
	if err != nil {
		fmt.Printf("%+v", err)
		return nil
	}
	defer txn.Close()

	stmt := spanner.NewStatement(fmt.Sprintf(
		`SELECT
	interval_end,
	fprint,
	read_columns,
	write_constructive_columns,
	write_delete_tables,
	commit_attempt_count,
	commit_failed_precondition_count,
	commit_abort_count,
	avg_participants,
	avg_total_latency_seconds,
	avg_commit_latency_seconds,
	avg_bytes,
FROM spanner_sys.txn_stats_top_%s
WHERE interval_end > @last_interval_end
ORDER BY interval_end DESC;`,
		t.String(),
	))
	stmt.Params["last_interval_end"] = lastIntervalEnd

	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	var results []stat

	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			fmt.Printf("%+v\n", err)
			return nil
		}

		var b TransactionStat
		err = row.ToStruct(&b)
		if err != nil {
			fmt.Printf("%+v\n", err)
			return nil
		}

		results = append(results, &b)
	}

	return results
}

// LockStat track the lock columns during a specific time period
// followed https://cloud.google.com/spanner/docs/introspection/lock-statistics
type LockStat struct {
	IntervalEnd        time.Time `spanner:"INTERVAL_END"`
	RowRangeStartKey   []byte    `spanner:"ROW_RANGE_START_KEY"`
	LockWaitSeconds    float64   `spanner:"LOCK_WAIT_SECONDS"`
	SampleLockRequests []struct {
		LockMode string `spanner:"lock_mode"`
		Column   string `spanner:"column"`
	} `spanner:"SAMPLE_LOCK_REQUESTS"`
}

func (q *LockStat) getIntervalEnd() time.Time {
	return q.IntervalEnd
}

// GetLockStats returns Stat collection with specific time period
func (c *Client) GetLockStats(ctx context.Context, t statDuration, lastIntervalEnd time.Time) []stat {
	txn, err := c.spannerClient.BatchReadOnlyTransaction(ctx, spanner.ExactStaleness(time.Minute))
	if err != nil {
		fmt.Printf("%+v", err)
		return nil
	}
	defer txn.Close()

	stmt := spanner.NewStatement(fmt.Sprintf(
		`SELECT
	interval_end,
	row_range_start_key,
	lock_wait_seconds,
	sample_lock_requests
FROM spanner_sys.lock_stats_top_%s
WHERE interval_end > @last_interval_end
ORDER BY interval_end DESC;`,
		t.String(),
	))
	stmt.Params["last_interval_end"] = lastIntervalEnd

	iter := txn.Query(ctx, stmt)
	defer iter.Stop()

	var results []stat

	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			fmt.Printf("%+v\n", err)
			return nil
		}

		var b LockStat
		err = row.ToStruct(&b)
		if err != nil {
			fmt.Printf("%+v\n", err)
			return nil
		}

		results = append(results, &b)
	}

	return results
}
