package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"kakfa-manual-offset/admin"
	"kakfa-manual-offset/group_consumer"
	"kakfa-manual-offset/standalone_consumer"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/orlangure/gnomock"
	gk "github.com/orlangure/gnomock/preset/kafka"
)

type Config struct {
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		Topics  []struct {
			Topic          string `yaml:"topic"`
			PartitionCount int    `yaml:"partition-count"`
		} `yaml:"topics"`
	} `yaml:"kafka"`
}

func main() {
	// Load configuration
	config := loadConfig("config.yaml")

	p := gk.Preset()

	container, err := gnomock.Start(
		p,
		gnomock.WithDebugMode(), gnomock.WithLogWriter(os.Stdout),
		gnomock.WithContainerName("kafka"),
	)
	if err != nil {
		panic(err)
	}

	config.Kafka.Brokers = []string{container.Address(gk.BrokerPort)}

	log.Println(config)

	for i := range config.Kafka.Topics {

		setupTopicAndMessages(config.Kafka.Brokers, config.Kafka.Topics[i].Topic, config.Kafka.Topics[i].PartitionCount, 200000)
	}

	// Initialize Consumer Manager
	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	appWg := &sync.WaitGroup{}

	startStandaloneConsumer(ctx, config, appWg)
	startGroupConsumer(ctx, config, appWg)

	//// Start REST API server
	//apiServer := api.NewAPI(standaloneConsumerManager)
	//router := gin.Default()
	//router.PUT("/offset", apiServer.SetOffsetHandler)
	//go router.Run(":8080")

	// Wait for shutdown signal
	<-sigs
	fmt.Println("Shutting down...")
	cancel()
	appWg.Wait()
	fmt.Println("Shutdown complete.")
	time.Sleep(1 * time.Second)
}

func loadConfig(file string) Config {
	var config Config
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("could not read config file: %v", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("could not unmarshal config data: %v", err)
	}
	return config
}

func setupTopicAndMessages(brokers []string, topic string, numPartitions int, count int) {
	kafkaAdmin := admin.NewKafkaAdmin(brokers)
	err := kafkaAdmin.CreateTopic(topic, numPartitions, 1)
	if err != nil {
		log.Fatalf("could not create topic: %v", err)
	}
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	go func() {
		defer w.Close()
		last := 0
		for {
			messages := make([]kafka.Message, count)
			for i := 0; i < count; i++ {
				messages[i] = kafka.Message{
					Key:   []byte(fmt.Sprintf("Key-%d", last+i)),
					Value: []byte(fmt.Sprintf("Value-%d", last+i)),
				}
			}
			last += count
			err = w.WriteMessages(context.Background(), messages...)
			if err != nil {
				log.Fatalf("could not write messages: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}()
}

func startStandaloneConsumer(ctx context.Context, config Config, appWg *sync.WaitGroup) {

	// Start standalone consumers
	standaloneConsumerManager := standalone_consumer.NewManager()
	for _, topicInfo := range config.Kafka.Topics {
		for i := 0; i < topicInfo.PartitionCount; i++ {
			standaloneConsumerManager.AddConsumer(standalone_consumer.NewKafkaConsumer(config.Kafka.Brokers, topicInfo.Topic, i))
			ch, err := standaloneConsumerManager.GetConsumerMessageChannel(topicInfo.Topic, i)
			if err != nil {
				log.Fatal(err)
			}
			go func(ch <-chan kafka.Message) {
				for msg := range ch {
					_ = msg
				}
			}(ch)
		}
	}
	standaloneConsumerManager.StartAllConsumers(ctx, appWg)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			for _, topicInfo := range config.Kafka.Topics {
				consumers, ok := standaloneConsumerManager.GetConsumers(topicInfo.Topic)
				if !ok {
					log.Fatalf("not exists standalone_consumer in consumerGroup: %s", topicInfo.Topic)
				}
				for _, c := range consumers {
					offset := c.CurrentOffset() / 2 // 절반으로 잘라!
					setOffsetErr := c.SetOffset(offset)
					if setOffsetErr != nil {
						log.Fatalf("왜 set offset 안됨? : %s \n", setOffsetErr.Error())
					}
				}
			}
		}
	}()

}

func startGroupConsumer(ctx context.Context, config Config, appWg *sync.WaitGroup) {
	// Start group consumers
	kafkaAdmin := admin.NewKafkaAdmin(config.Kafka.Brokers)
	groupConsumerManager := group_consumer.NewManager(kafkaAdmin)
	for _, topicInfo := range config.Kafka.Topics {
		groupConsumerManager.AddConsumer(group_consumer.NewKafkaConsumer(config.Kafka.Brokers, topicInfo.Topic, "test-group"))
		ch, err := groupConsumerManager.GetConsumerMessageChannel(topicInfo.Topic, "test-group")
		if err != nil {
			log.Fatal(err)
		}
		go func(ch <-chan kafka.Message) {
			for msg := range ch {
				_ = msg
			}
		}(ch)
	}
	groupConsumerManager.StartAllConsumers(ctx, appWg)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop() // 종료 시 ticker를 정리

		for range ticker.C {
			for _, topicInfo := range config.Kafka.Topics {

				consumer, ok := groupConsumerManager.GetConsumers(topicInfo.Topic, "test-group")
				if !ok {
					log.Fatalf("not exists group_consumer=%s in consumerGroup: %s", "test-group", topicInfo.Topic)
				}

				var minOffset int64 = math.MaxInt64
				for _, offset := range consumer.CurrentOffsetMap() {
					if minOffset > offset {
						minOffset = offset
					}
				}
				if minOffset == math.MaxInt64 {
					break
				}
				setOffsetErr := groupConsumerManager.SetOffset(topicInfo.Topic, "test-group", minOffset/2)

				if setOffsetErr != nil {
					log.Fatalf("왜 set offset 안됨? : %s \n", setOffsetErr.Error())
				}
			}
		}
	}()
}
