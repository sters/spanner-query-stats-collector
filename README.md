# spanner-query-stats-collector

Now, you can use [googlecloudspannerreceiver in open-telemetry/opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/googlecloudspannerreceiver) for collect query stats.

This `spanner-query-stats-collector` does not need anymore.

---


![Go](https://github.com/sters/spanner-query-stats-collector/workflows/Go/badge.svg)
![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/sters/spanner-query-stats-collector?style=plastic)


[Google Cloud Spanner](https://cloud.google.com/spanner)'s query stats collector.

## Usage

See releases that can be easy to use collect your query stats.
Or you can use `go get` command.

```sh
go get github.com/sters/spanner-query-stats-collector
```

This application is [cmd/collector/main.go](https://github.com/sters/spanner-query-stats-collector/blob/master/cmd/collector/main.go) use envconfig for stats writer. Default as 1 miniute query stats to stdout with JSON format.

```json
{"level":"info","ts":1581839172.210752,"caller":"stats/writer.go:22","msg":"","IntervalEnd":1581839100,"Text":"SELECT 1","TextTruncated":false,"TextFingerprint":0,"ExecutionCount":78,"AvgLatencySeconds":0.0005415128205128205,"AvgRows":1,"AvgBytes":8,"AvgRowsScanned":0,"AvgCPUSeconds":0.00002253846153846154}
```

### With Docker

Simple example with your local credential.

Use this [sters/spanner-query-stats-collector - Docker Hub](https://hub.docker.com/r/sters/spanner-query-stats-collector)

```sh
docker run \
  --rm
  -e PROJECT_ID="xxxxx" \
  -e INSTANCE_ID="xxxxx" \
  -e DATABASE_ID="xxxxx" \
  -e CREDENTIAL_FILE="/etc/google/application_default_credentials.json" \
  -v ~/.config/gcloud/:/etc/google/:ro \
  sters/spanner-query-stats-collector:latest
```

## Customize to your application

You can find example at [cmd/collector/main.go](https://github.com/sters/spanner-query-stats-collector/blob/master/cmd/collector/main.go).

This package support only `SPANNER_SYS.QUERY_STATS_TOP_*` Tables, from [This document](https://cloud.google.com/spanner/docs/query-stats-tables).
