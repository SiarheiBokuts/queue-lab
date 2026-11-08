# Worker image (Go). Built by the app through the Docker API the first time Go workers are used.
# Unit tests have their own file: docker build -f workers/go/Dockerfile.test workers/go
# The Kafka client is Confluent's, built on librdkafka like the Python and Node.js ones, so settings are identical.
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN go mod tidy && CGO_ENABLED=1 go build -o /worker .

FROM debian:bookworm-slim
COPY --from=build /worker /worker
CMD ["/worker"]