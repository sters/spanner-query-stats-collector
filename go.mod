module github.com/sters/spanner-query-stats-collector

go 1.16

require (
	cloud.google.com/go/spanner v1.25.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golangci/golangci-lint v1.42.1
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/quasilyte/go-consistent v0.0.0-20200404105227-766526bf1e96
	go.opentelemetry.io/contrib/exporters/metric/dogstatsd v0.20.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/exporters/stdout v0.20.0
	go.opentelemetry.io/otel/metric v0.20.0
	go.opentelemetry.io/otel/sdk v0.20.0
	go.opentelemetry.io/otel/sdk/metric v0.20.0
	go.uber.org/zap v1.19.1
	golang.org/x/net v0.0.0-20210510120150-4163338589ed // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.56.0
)
