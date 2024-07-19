package standalone_consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Manager struct {
	consumerMap map[string][]Consumer
	mu          sync.Mutex
}

func NewManager() *Manager {
	return &Manager{
		consumerMap: make(map[string][]Consumer),
	}
}

func (m *Manager) AddConsumer(consumer Consumer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumers := m.consumerMap[consumer.Info().topic]
	consumers = append(consumers, consumer)
	m.consumerMap[consumer.Info().topic] = consumers
}

func (m *Manager) StartAllConsumers(ctx context.Context, wg *sync.WaitGroup) {
	for _, consumers := range m.consumerMap {
		for _, consumer := range consumers {
			go func(c Consumer) {
				wg.Add(1)
				c.Start(ctx, wg)
			}(consumer)
		}
	}
}

func (m *Manager) GetConsumers(topic string) ([]Consumer, bool) {
	consumers, ok := m.consumerMap[topic]
	return consumers, ok
}

func (m *Manager) SetOffset(topic string, partition int, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	consumers, exists := m.consumerMap[topic]
	if !exists {
		return fmt.Errorf("standalone_consumer for topic=%s not found", topic)
	}

	if len(consumers)-1 < partition {
		return fmt.Errorf("standalone_consumer for topic=%s, partition %s not found", topic, partition)
	}
	fmt.Printf("change offset parition=%d, offset=%d", partition, offset)
	return consumers[partition].SetOffset(offset)
}

func (m *Manager) GetConsumerMessageChannel(topic string, partition int) (<-chan kafka.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	consumers, exists := m.consumerMap[topic]
	if !exists {
		return nil, fmt.Errorf("standalone_consumer for topic=%s not found", topic)
	}

	if len(consumers)-1 < partition {
		return nil, fmt.Errorf("standalone_consumer for topic=%s, partition %s not found", topic, partition)
	}
	return consumers[partition].MessageChannel(), nil
}
