# spanner-stats-collector

![Go](https://github.com/sters/spanner-stats-collector/workflows/Go/badge.svg)
![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/sters/spanner-stats-collector?style=plastic)


[Google Cloud Spanner](https://cloud.google.com/spanner)'s query stats collector.

## Usage

See releases that can be easy to use collect your query stats.
Or you can use `go get` command.

```sh
go get github.com/sters/spanner-stats-collector
```

This application is [cmd/collector/main.go](https://github.com/sters/spanner-stats-collector/blob/master/cmd/collector/main.go) that shows 1 miniute query stats to stdout with JSON format.

```json
{"level":"info","ts":1581839172.210752,"caller":"stats/writer.go:22","msg":"","IntervalEnd":1581839100,"Text":"SELECT 1","TextTruncated":false,"TextFingerprint":0,"ExecutionCount":78,"AvgLatencySeconds":0.0005415128205128205,"AvgRows":1,"AvgBytes":8,"AvgRowsScanned":0,"AvgCPUSeconds":0.00002253846153846154}
```

### With Docker

Simple example with your local credential.

Use this [sters/spanner-stats-collector - Docker Hub](https://hub.docker.com/r/sters/spanner-stats-collector)

```sh
docker run \
  -e PROJECT_ID="xxxxx" \
  -e INSTANCE_ID="xxxxx" \
  -e DATABASE_ID="xxxxx" \
  -e CREDENTIAL_FILE="/etc/google/application_default_credentials.json" \
  -v ~/.config/gcloud/:/etc/google/:ro \
  sters/spanner-stats-collector:latest
```

## Customize to your application

You can find example at [cmd/collector/main.go](https://github.com/sters/spanner-stats-collector/blob/master/cmd/collector/main.go).

This package support only `SPANNER_SYS.QUERY_STATS_TOP_*` Tables, from [This document](https://cloud.google.com/spanner/docs/query-stats-tables).
