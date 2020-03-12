FROM golang:1.13 as builder

WORKDIR /go/src/github.com/sters/spanner-query-stats-collector

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go install -v github.com/sters/spanner-query-stats-collector/cmd/collector


FROM alpine:latest

ENV PROJECT_ID ""
ENV INSTANCE_ID ""
ENV DATABASE_ID ""
ENV CREDENTIAL_FILE "/etc/google/service-account.json"

RUN apk add --update --no-cache ca-certificates tzdata

COPY --from=builder /go/bin/collector /bin/collector
RUN addgroup -g 1001 defaultuser && adduser -D -G defaultuser -u 1001 defaultuser
USER 1001

CMD ["sh", "-c", "/bin/collector -project_id=${PROJECT_ID} -instance_id=${INSTANCE_ID} -database_id=${DATABASE_ID} -credential=${CREDENTIAL_FILE}"]
