package main

// The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done".
//
// The file reads top to bottom in the same groups as the producer of every other language:
//  1. settings      what this producer was asked to do
//  2. counters      what the report shows, shared by the loop, the delivery callbacks and the reporter
//  3. client        the Kafka producer and its delivery reports
//  4. message       how one message is built and handed to the client
//  5. run           the steps in order (the sending loop is the part shown on the Code page)

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

// ---- 1. settings -------------------------------------------------------------------------------------------------

type producerSettings struct {
	total         int     // this producer's share of the messages
	size          int     // bytes per message
	partitions    int     // partitions of the queue
	index         int     // which producer this is (0, 1, ...) when several share the load
	rate          float64 // this producer's target messages per second; 0 = as fast as possible
	randomPayload bool    // incompressible data instead of a repeated filler
	users         int     // how many different users (message keys) the messages are spread over
}

func readProducerSettings() producerSettings {
	rate, _ := strconv.ParseFloat(required("RATE"), 64)
	return producerSettings{
		total:         envInt("MESSAGES"),
		size:          envInt("MSG_SIZE"),
		partitions:    envInt("PARTITIONS"),
		index:         envInt("PRODUCER_INDEX"),
		rate:          rate,
		randomPayload: required("PAYLOAD") == "random",
		users:         envInt("USERS"),
	}
}

// ---- 2. counters -------------------------------------------------------------------------------------------------

type producerCounters struct {
	mu                      sync.Mutex
	sent, delivered, errors int64
	done                    bool
	started, finished       float64 // seconds since 1970; 0 = not yet
}

func (c *producerCounters) setSent(n int64) {
	c.mu.Lock()
	c.sent = n
	c.mu.Unlock()
}

// confirmed is called for every message once the broker confirmed or rejected it.
func (c *producerCounters) confirmed(failed bool) {
	c.mu.Lock()
	if failed {
		c.errors++
	} else {
		c.delivered++
	}
	c.mu.Unlock()
}

func (c *producerCounters) start() {
	c.mu.Lock()
	c.started = nowSeconds()
	c.mu.Unlock()
}

func (c *producerCounters) finish() {
	c.mu.Lock()
	c.finished = nowSeconds()
	c.done = true
	c.mu.Unlock()
}

func (c *producerCounters) running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.done
}

func (c *producerCounters) report() {
	c.mu.Lock()
	payload := map[string]interface{}{
		"runId": runID, "worker": workerID(), "role": "producer", "lang": "go", "ts": nowSeconds(),
		"sent": c.sent, "delivered": c.delivered, "errors": c.errors, "done": c.done,
		"started": nilIfZero(c.started), "finished": nilIfZero(c.finished),
	}
	c.mu.Unlock()
	post(payload)
}

// nowSeconds is the time in seconds since 1970, as the report wants it.
func nowSeconds() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// nilIfZero reports a timestamp that was never set as JSON null.
func nilIfZero(v float64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

// ---- 3. client ---------------------------------------------------------------------------------------------------

func newKafkaProducer(counters *producerCounters) *kafka.Producer {
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
				counters.confirmed(message.TopicPartition.Error != nil)
			}
		}
	}()
	return producer
}

// ---- 4. message --------------------------------------------------------------------------------------------------

// messageMaker builds message number i of this producer: which partition, which user, and the bytes to send.
// It owns what has to be remembered between messages: the sequence counter of every partition.
type messageMaker struct {
	cfg  producerSettings
	seqs []int64 // each partition has its own sequence counter for this producer, starting at 0
	pool []byte  // the data random padding is cut from (empty when the padding is a repeated filler)
}

func newMessageMaker(cfg producerSettings) *messageMaker {
	maker := &messageMaker{cfg: cfg, seqs: make([]int64, cfg.partitions)}
	if cfg.randomPayload {
		maker.pool = make([]byte, poolBytes+cfg.size)
		_, _ = rand.Read(maker.pool)
	}
	return maker
}

func (m *messageMaker) make(i int) *kafka.Message {
	part := (i + m.cfg.index) % m.cfg.partitions // round robin; producers start on different partitions so the load stays even
	head := buildHeader(runID, m.cfg.index, part, m.seqs[part], nowMs())
	m.seqs[part]++
	topicName := topic
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: int32(part)},
		Key:            userKey(part, i, m.cfg.partitions, m.cfg.users),
		Value:          m.pad(head, i),
	}
}

// pad is the header (who sent it, where, which number, when) padded up to the requested message size.
func (m *messageMaker) pad(head []byte, i int) []byte {
	fill := m.cfg.size - len(head)
	if fill < 0 {
		fill = 0
	}
	payload := make([]byte, 0, len(head)+fill)
	payload = append(payload, head...)
	if m.cfg.randomPayload {
		offset := (i * 997) % poolBytes
		return append(payload, m.pool[offset:offset+fill]...)
	}
	for k := 0; k < fill; k++ {
		payload = append(payload, 'x')
	}
	return payload
}

// produce hands a message to the client; when the local queue is full it waits for the client to drain it.
func produce(producer *kafka.Producer, message *kafka.Message) {
	for {
		err := producer.Produce(message, nil)
		if err == nil {
			return
		}
		if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrQueueFull {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		panic(err)
	}
}

// ---- 5. run ------------------------------------------------------------------------------------------------------

func runProducer() {
	cfg := readProducerSettings()
	counters := &producerCounters{}
	producer := newKafkaProducer(counters)
	maker := newMessageMaker(cfg)
	startReporter(counters.report, counters.running)

	// @producer-start
	begin := time.Now()
	counters.start()
	checkEvery := pacingChunk(cfg.rate, 500) // how often to look at the clock
	for i := 0; i < cfg.total && !stopping.Load(); i++ {
		produce(producer, maker.make(i)) // message number i: its partition, its user and its bytes
		counters.setSent(int64(i + 1))
		if (i+1)%checkEvery == 0 {
			if wait := pacingDelay(i+1, cfg.rate, time.Since(begin)); wait > 0 { // ahead of the target rate? then wait
				time.Sleep(wait)
			}
		}
	}
	// @producer-end
	producer.Flush(60000) // wait until every message is confirmed by the broker
	counters.finish()
	counters.report()
	producer.Close()
	_ = os.Stdout.Sync()
}
