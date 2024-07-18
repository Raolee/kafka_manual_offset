package group_consumer

import (
	"context"
	"fmt"
	"kakfa-manual-offset/admin"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Manager struct {
	admin                 admin.KafkaAdmin
	topicGroupConsumerMap map[string]map[string]Consumer
	mu                    sync.RWMutex
}

func NewManager(admin admin.KafkaAdmin) *Manager {
	return &Manager{
		admin:                 admin,
		topicGroupConsumerMap: make(map[string]map[string]Consumer),
		mu:                    sync.RWMutex{},
	}
}

func (m *Manager) AddConsumer(consumer Consumer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic := consumer.Info().topic
	group := consumer.Info().group
	if _, ok := m.topicGroupConsumerMap[topic]; !ok {
		m.topicGroupConsumerMap[topic] = make(map[string]Consumer)
	}
	m.topicGroupConsumerMap[topic][group] = consumer
}

func (m *Manager) StartAllConsumers(ctx context.Context, wg *sync.WaitGroup) {
	for _, groupAndConsumer := range m.topicGroupConsumerMap {
		for _, consumer := range groupAndConsumer {
			go func(c Consumer) {
				wg.Add(1)
				c.Start(ctx, wg)
			}(consumer)
		}
	}
}

func (m *Manager) GetConsumers(topic string, group string) (Consumer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.topicGroupConsumerMap[topic]; !ok {
		return nil, false
	}
	consumer, ok := m.topicGroupConsumerMap[topic][group]
	return consumer, ok
}

func (m *Manager) SetOffset(topic string, group string, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, ok := m.GetConsumers(topic, group)
	if !ok {
		return fmt.Errorf("consumer for topic=%s, group=%s not found", topic, group)
	}

	// Pause the consumer
	consumer.Pause()

	// Change the offset
	fmt.Printf("change offset group=%s, offset=%d", group, offset)
	if err := m.admin.ChangeOffset(topic, group, offset); err != nil {
		return err
	}

	// Resume the consumer
	consumer.Resume()

	return nil
}

func (m *Manager) GetConsumerMessageChannel(topic string, group string) (<-chan kafka.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	groupAndConsumer, ok := m.topicGroupConsumerMap[topic]
	if !ok {
		return nil, fmt.Errorf("consumer for topic=%s not found", topic)
	}

	consumer, ok := groupAndConsumer[group]
	if !ok {
		return nil, fmt.Errorf("consumer for topic=%s, group=%s not found", topic, group)
	}

	return consumer.MessageChannel(), nil
}
