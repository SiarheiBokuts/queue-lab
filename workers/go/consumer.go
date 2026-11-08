package main

// The consumer (the "handler"): reads messages in batches, verifies them and runs the chosen scenario.
//
// The same loop is written in every language of the lab, with the same steps and names:
//
//	readBatch -> groupByUser -> runBatch (handleGroup for every user at the same time) -> commit
//
// A batch is at most `concurrency` messages. The messages of one user are always handled one after another, like in
// production; the messages of different users are handled at the same time. The next batch is read when the whole
// batch is done: the client has already fetched the next messages in the background, and starting early could let a
// message of a busy user overtake an older one.

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// readBatch waits at most batchWaitMs for a message, then takes up to `concurrency` messages that have already
// arrived. It never waits to fill the batch.
func readBatch(consumer *kafka.Consumer, stats *consumerStats) []*kafka.Message {
	var batch []*kafka.Message
	wait := batchWaitMs
	for len(batch) < concurrency {
		event := consumer.Poll(wait)
		if event == nil {
			break // nothing more has arrived
		}
		switch e := event.(type) {
		case *kafka.Message:
			batch = append(batch, e)
			wait = 0 // after the first message only take what is already there
		case kafka.Error:
			stats.countError()
		}
	}
	return batch
}

// handleGroup handles the messages of one user one after another.
func handleGroup(count int, handle func()) {
	for i := 0; i < count; i++ {
		handle()
	}
}

// runBatch handles all groups at the same time and waits until every one of them is done.
func runBatch(groups [][]int, handle func()) {
	var running sync.WaitGroup
	for _, group := range groups {
		running.Add(1)
		go func(count int) {
			defer running.Done()
			handleGroup(count, handle)
		}(len(group))
	}
	running.Wait()
}

// commit tells Kafka that everything read so far is handled. A crash before this repeats the batch.
func commit(consumer *kafka.Consumer) {
	if _, err := consumer.Commit(); err != nil {
		if kerr, ok := err.(kafka.Error); !ok || kerr.Code() != kafka.ErrNoOffset {
			println("commit failed:", err.Error())
		}
	}
}

func runConsumer() {
	scenario := os.Getenv("SCENARIO")
	if scenario == "" {
		scenario = "echo"
	}
	param, _ := strconv.Atoi(os.Getenv("SCENARIO_PARAM"))
	stats := newConsumerStats()

	// Offset reset, commit and timeout settings come from the backend (the same values for every language).
	conf := kafkaConf("CONSUMER_CONF")
	conf["bootstrap.servers"] = boot
	conf["group.id"] = required("GROUP_ID")
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		panic(err)
	}
	err = consumer.SubscribeTopics([]string{topic}, func(c *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions: // Kafka gave us partitions (called again after every rebalance)
			ids := make([]int32, len(e.Partitions))
			for i, p := range e.Partitions {
				ids[i] = p.Partition
			}
			stats.assign(ids)
			return c.Assign(e.Partitions)
		case kafka.RevokedPartitions: // Kafka took partitions away, usually because another worker joined or left
			ids := make([]int32, len(e.Partitions))
			for i, p := range e.Partitions {
				ids[i] = p.Partition
			}
			stats.revoke(ids)
			return c.Unassign()
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	startReporter(func() { stats.report(false) }, func() bool { return true })

	cpuSlots := make(chan struct{}, cpuThreads)
	// @scenario-start
	// handle is what the worker does with one message. The same three scenarios in every language:
	// echo = nothing, the message is only counted; delay = wait, like a call to a database;
	// json = parse a JSON document and write it back, like a service reading a request.
	handle := func() {
		if scenario == "delay" && param > 0 {
			time.Sleep(time.Duration(param) * time.Millisecond) // wait param milliseconds and do nothing else
		} else if scenario == "json" && param > 0 {
			cpuSlots <- struct{}{} // only one message per core may work at once, so no language takes the cores of another
			defer func() { <-cpuSlots }()
			text := jsonDoc
			for i := 0; i < param; i++ { // parse the document and write it back, param times
				var value interface{}
				if err := json.Unmarshal(text, &value); err != nil {
					break
				}
				text, _ = json.Marshal(value)
			}
		}
		// echo: nothing to do
	}
	// @scenario-end

	for !stopping.Load() {
		batch := readBatch(consumer, stats)
		if len(batch) == 0 {
			continue
		}
		arrived := nowMs()
		var keys [][]byte
		for _, message := range batch {
			head, valid := parseHeader(message.Value, runID)
			if !valid {
				continue // a leftover from another run: ignore it
			}
			stats.record(head, message.TopicPartition.Partition, int64(message.TopicPartition.Offset), arrived)
			keys = append(keys, message.Key)
		}
		runBatch(groupByUser(keys), handle)
		commit(consumer)
	}
	stats.report(true)
	consumer.Close() // leave the group cleanly so the partitions are reassigned at once
}
