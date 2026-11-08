package main

// The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done".

import (
	"crypto/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// poolBytes: random padding is cut from one pre-generated megabyte, so generating it costs almost nothing.
const poolBytes = 1 << 20

func envInt(name string) int {
	n, err := strconv.Atoi(required(name))
	if err != nil {
		panic(name + " is not a number")
	}
	return n
}

func runProducer() {
	total := envInt("MESSAGES") // this producer's share of the messages
	size := envInt("MSG_SIZE")
	nparts := envInt("PARTITIONS")
	index := envInt("PRODUCER_INDEX") // which producer this is (0, 1, ...) when several share the load
	rate, _ := strconv.ParseFloat(required("RATE"), 64) // this producer's target messages per second; 0 = as fast as possible
	randomPayload := required("PAYLOAD") == "random"    // incompressible data instead of a repeated filler
	users := envInt("USERS")                            // how many different users (message keys) the messages are spread over

	var mu sync.Mutex
	var sent, delivered, errs int64
	var done bool
	var started, finished float64

	// Batching, acks, queue size and compression come from the backend (the same values for every language).
	conf := kafkaConf("PRODUCER_CONF")
	conf["bootstrap.servers"] = boot
	producer, err := kafka.NewProducer(&conf)
	if err != nil {
		panic(err)
	}
	go func() { // delivery reports: the broker confirmed or rejected a message
		for event := range producer.Events() {
			if message, ok := event.(*kafka.Message); ok {
				mu.Lock()
				if message.TopicPartition.Error != nil {
					errs++
				} else {
					delivered++
				}
				mu.Unlock()
			}
		}
	}()

	pool := []byte{}
	if randomPayload {
		pool = make([]byte, poolBytes+size)
		_, _ = rand.Read(pool)
	}
	sendReport := func() {
		mu.Lock()
		payload := map[string]interface{}{
			"runId": runID, "worker": workerID(), "role": "producer", "lang": "go", "ts": float64(time.Now().UnixNano()) / 1e9,
			"sent": sent, "delivered": delivered, "errors": errs, "done": done, "started": nilIfZero(started), "finished": nilIfZero(finished),
		}
		mu.Unlock()
		post(payload)
	}
	startReporter(sendReport, func() bool { mu.Lock(); defer mu.Unlock(); return !done })

	// @producer-start
	seqs := make([]int64, nparts) // each partition has its own sequence counter for this producer, starting at 0
	begin := time.Now()
	started = float64(begin.UnixNano()) / 1e9
	checkEvery := pacingChunk(rate, 500) // how often to look at the clock and serve delivery callbacks
	topicName := topic
	for i := 0; i < total && !stopping.Load(); i++ {
		part := (i + index) % nparts // round robin; producers start on different partitions so the load stays even
		head := buildHeader(runID, index, part, seqs[part], nowMs())
		seqs[part]++
		fill := size - len(head) // pad up to the requested message size
		if fill < 0 {
			fill = 0
		}
		payload := make([]byte, 0, len(head)+fill)
		payload = append(payload, head...)
		if randomPayload {
			offset := (i * 997) % poolBytes
			payload = append(payload, pool[offset:offset+fill]...)
		} else {
			for k := 0; k < fill; k++ {
				payload = append(payload, 'x')
			}
		}
		message := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: int32(part)}, Key: userKey(part, i, nparts, users), Value: payload}
		for {
			err := producer.Produce(message, nil)
			if err == nil {
				break
			}
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrQueueFull {
				time.Sleep(50 * time.Millisecond) // the local queue is full: let the client drain it, then retry
				continue
			}
			panic(err)
		}
		mu.Lock()
		sent = int64(i + 1)
		mu.Unlock()
		if (i+1)%checkEvery == 0 {
			if wait := pacingDelay(i+1, rate, time.Since(begin)); wait > 0 { // ahead of the target rate? then wait
				time.Sleep(wait)
			}
		}
	}
	// @producer-end
	producer.Flush(60000) // wait until every message is confirmed by the broker
	mu.Lock()
	finished = float64(time.Now().UnixNano()) / 1e9
	done = true
	mu.Unlock()
	sendReport()
	producer.Close()
	_ = os.Stdout.Sync()
}

// nilIfZero reports a timestamp that was never set as JSON null.
func nilIfZero(v float64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}