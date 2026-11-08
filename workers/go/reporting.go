package main

// Progress reporting shared by the producer and the consumer: the stop flag and the HTTP report.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// stopping is set on SIGTERM so loops can finish and send a final report.
var stopping atomic.Bool

var httpClient = &http.Client{Timeout: 3 * time.Second}

// post sends a progress report to the backend. A failed report is logged and skipped, never fatal.
func post(payload map[string]interface{}) {
	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Println("report failed:", err)
		return
	}
	resp, err := httpClient.Post(reportURL, "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Println("report failed:", err)
		return
	}
	resp.Body.Close()
}

// startReporter calls send once per second on a background goroutine until the worker stops.
func startReporter(send func(), keepGoing func() bool) {
	go func() {
		for !stopping.Load() && keepGoing() {
			time.Sleep(time.Second)
			send()
		}
	}()
}