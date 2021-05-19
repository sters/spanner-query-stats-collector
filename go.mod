module github.com/sters/spanner-query-stats-collector

go 1.16

require (
	cloud.google.com/go v0.82.0 // indirect
	cloud.google.com/go/spanner v1.18.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/exporters/stdout v0.20.0
	go.opentelemetry.io/otel/metric v0.20.0
	go.opentelemetry.io/otel/sdk/metric v0.20.0
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20210510120150-4163338589ed // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210514084401-e8d321eab015 // indirect
	google.golang.org/api v0.46.0
	google.golang.org/genproto v0.0.0-20210518161634-ec7691c0a37d // indirect
	gopkg.in/yaml.v2 v2.2.7 // indirect
)
