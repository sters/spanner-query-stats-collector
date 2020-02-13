
export GO111MODULE=on

.PHONY: bootstrap-tools
bootstrap-tools:
	GO111MODULE=off go get -u golang.org/x/lint/golint

.PHONY: lint
lint:
	golint ./...

.PHONY: test
test:
	go test -race ./...

.PHONY: cover
cover:
	go test -cover -race ./...

