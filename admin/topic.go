package admin

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

type KafkaAdmin interface {
	CreateTopic(topic string, numPartitions int, replicationFactor int) error
	ChangeOffset(topic string, group string, offset int64) error
	Close()
}

type KafkaAdminClient struct {
	client *kafka.Client
}

func NewKafkaAdmin(brokers []string) KafkaAdmin {
	client := &kafka.Client{
		Addr: kafka.TCP(brokers...),
	}
	return &KafkaAdminClient{client: client}
}

func (k *KafkaAdminClient) CreateTopic(topic string, numPartitions int, replicationFactor int) error {
	createTopicsRequest := &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     numPartitions,
				ReplicationFactor: replicationFactor,
			},
		},
	}

	ctx := context.Background()
	response, err := k.client.CreateTopics(ctx, createTopicsRequest)
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	for topicName, createTopicErr := range response.Errors {
		if createTopicErr != nil {
			return fmt.Errorf("failed to create topic %s: %v", topicName, createTopicErr)
		}
	}

	fmt.Printf("Topic %s created successfully\n", topic)
	return nil
}

func (k *KafkaAdminClient) ChangeOffset(topic string, group string, offset int64) error {
	ctx := context.Background()

	// Step 1: Fetch partitions for the topic
	metadata, err := k.client.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{topic},
	})
	if err != nil {
		return err
	}
	if len(metadata.Topics) == 0 {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	for _, partition := range metadata.Topics[0].Partitions {
		// Create a new consumer to commit offsets
		consumerGroup, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
			Brokers: []string{k.client.Addr.String()},
			Topics:  []string{topic},
			ID:      group,
		})
		if err != nil {
			return fmt.Errorf("failed to create consumer group: %v", err)
		}
		defer consumerGroup.Close()

		// Set the offset
		next, err := consumerGroup.Next(context.Background())
		if err != nil {
			return fmt.Errorf("failed next consumger group : %v", err)
		}
		err = next.CommitOffsets(map[string]map[int]int64{
			topic: {
				int(partition.ID): offset,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to commit offset: %v", err)
		}

		fmt.Printf("Successfully set offset for partition %d to %d\n", partition.ID, offset)
	}

	return nil
}

func (k *KafkaAdminClient) Close() {
	// kafka-go 는 close 가 별도로 없넹
}
