package group_consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
)

type Consumer interface {
	Info() struct {
		topic string
		group string
	}
	Start(ctx context.Context, wg *sync.WaitGroup)
	Pause()
	Resume()
	CurrentOffsetMap() map[int]int64
	MessageChannel() <-chan kafka.Message
}

type KafkaConsumer struct {
	brokers          []string
	topic            string
	group            string
	currentOffsetMap map[int]int64
	reader           *kafka.Reader
	msgChan          chan kafka.Message
	pauseChan        chan struct{}
	resumeChan       chan struct{}
	mu               sync.Mutex
	paused           bool
}

func NewKafkaConsumer(brokers []string, topic string, group string) Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  group,
		Topic:    topic,
		MinBytes: 10e2,
		MaxBytes: 10e6,
	})

	return &KafkaConsumer{
		brokers:          brokers,
		topic:            topic,
		group:            group,
		currentOffsetMap: make(map[int]int64),
		reader:           reader,
		msgChan:          make(chan kafka.Message),
		pauseChan:        make(chan struct{}),
		resumeChan:       make(chan struct{}),
		mu:               sync.Mutex{},
		paused:           false,
	}
}

func (c *KafkaConsumer) Info() struct {
	topic string
	group string
} {
	return struct {
		topic string
		group string
	}{topic: c.topic, group: c.group}
}

func (c *KafkaConsumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	fmt.Printf("Starting consumer on topic %s group %s\n", c.topic, c.group)
	defer wg.Done()
	n := int64(100)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Shutting down consumer for group %s\n", c.group)
			return
		case <-c.pauseChan:
			fmt.Printf("Pausing consumer for group %s\n", c.group)
			<-c.resumeChan
			fmt.Printf("Resuming consumer for group %s\n", c.group)
		default:
			m, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					fmt.Printf("Consumer for group %s canceled\n", c.group)
					return
				}
				log.Fatalf("could not read message %v", err)
			}
			c.currentOffsetMap[m.Partition] = m.Offset
			c.msgChan <- m
			if m.Offset%n == 0 { // 100개마다 로그 출력
				fmt.Printf("consumer Partition %d message at offset %d: %s = %s\n", m.Partition, m.Offset, string(m.Key), string(m.Value))
			}
		}
	}
}

func (c *KafkaConsumer) Pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.paused {
		c.pauseChan <- struct{}{}
		c.paused = true
	}
}

func (c *KafkaConsumer) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paused {
		c.resumeChan <- struct{}{}
		c.paused = false
	}
}

func (c *KafkaConsumer) CurrentOffsetMap() map[int]int64 {
	return c.currentOffsetMap
}

func (c *KafkaConsumer) MessageChannel() <-chan kafka.Message {
	return c.msgChan
}
