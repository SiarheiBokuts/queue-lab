package main

// Everything a worker reads from its environment, parsed once at start-up.
//

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// required reads a required environment variable or stops with a clear message.
func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("Missing environment variable " + name)
	}
	return value
}

var (
	role      string // "producer" or "consumer"
	boot      string // broker address
	topic     string // the queue
	runID     string // id of this run; messages of other runs are ignored
	reportURL string // where the once-per-second report is POSTed
	// Latency histogram: instead of sending every latency we count messages per bucket. edgesMs are the upper edge of
	// each bucket in milliseconds; the backend sends them so they are defined only once.
	edgesMs []float64
	// concurrency is how many messages one consumer handles at the same time (consumer only).
	concurrency = 1
	// ordering is "key" (one user stays in one lane, in order) or "none" (any lane), consumer only.
	ordering = "key"
)

// initSettings reads the environment. It runs from main, not at package start, so the unit tests need no environment.
func initSettings() {
	role = required("ROLE")
	boot = required("KAFKA_BOOTSTRAP")
	topic = required("TOPIC")
	runID = required("RUN_ID")
	reportURL = required("REPORT_URL")
	edgesMs = parseEdges(required("HIST_EDGES"))
	if n, err := strconv.Atoi(os.Getenv("CONCURRENCY")); err == nil && n > 1 {
		concurrency = n
	}
	if v := os.Getenv("ORDERING"); v != "" {
		ordering = v
	}
}

// workerID is the container name, which identifies this worker in reports.
func workerID() string {
	if v := os.Getenv("WORKER_ID"); v != "" {
		return v
	}
	return "worker"
}

// kafkaConf turns the Kafka client properties sent by the backend as JSON (librdkafka names) into a ConfigMap.
func kafkaConf(variable string) kafka.ConfigMap {
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(required(variable)), &raw); err != nil {
		panic(fmt.Sprintf("%s is not valid JSON: %v", variable, err))
	}
	conf := kafka.ConfigMap{}
	for key, value := range raw {
		if number, isNumber := value.(float64); isNumber && number == float64(int64(number)) {
			conf[key] = int(number) // JSON numbers are floats; the client wants integers for whole numbers
		} else {
			conf[key] = value
		}
	}
	return conf
}
