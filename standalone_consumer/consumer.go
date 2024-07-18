package standalone_consumer

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
		topic     string
		partition int
	}
	Start(ctx context.Context, wg *sync.WaitGroup)
	CurrentOffset() int64
	SetOffset(offset int64) error
	MessageChannel() <-chan kafka.Message
}

type KafkaConsumer struct {
	brokers       []string
	topic         string
	partition     int
	currentOffset int64
	reader        *kafka.Reader
	msgChan       chan kafka.Message
}

func NewKafkaConsumer(brokers []string, topic string, partition int) Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e2,
		MaxBytes:  10e6,
	})

	return &KafkaConsumer{
		brokers:   brokers,
		topic:     topic,
		partition: partition,
		reader:    reader,
		msgChan:   make(chan kafka.Message),
	}
}
func (c *KafkaConsumer) Info() struct {
	topic     string
	partition int
} {
	return struct {
		topic     string
		partition int
	}{topic: c.topic, partition: c.partition}
}

func (c *KafkaConsumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	fmt.Printf("Starting standalone_consumer on topic %s partition %d\n", c.topic, c.partition)
	defer wg.Done()
	n := int64(100)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Shutting down standalone_consumer for partition %d\n", c.partition)
			return
		default:
			m, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					fmt.Printf("Consumer for partition %d canceled\n", c.partition)
					return
				}
				log.Fatalf("could not read message %v", err)
			}
			c.currentOffset = m.Offset
			c.msgChan <- m
			if m.Offset%n == 0 { // 100개 마다 찍어
				//fmt.Printf("Partition %d message at offset %d: %s = %s\n", c.partition, m.Offset, string(m.Key), string(m.Value))
			}
		}
	}
}

func (c *KafkaConsumer) CurrentOffset() int64 {
	return c.currentOffset
}

func (c *KafkaConsumer) SetOffset(offset int64) error {
	return c.reader.SetOffset(offset)
}

func (c *KafkaConsumer) MessageChannel() <-chan kafka.Message {
	return c.msgChan
}
