package main

import (
	"flag"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/kafka"
	log "github.com/sirupsen/logrus"
)

var kafkaVersionStr string
var brokerStr string
var brokers []string
var topic string
var fallbackNumPartitions int
var kafkaNet *kafka.KafkaNet

func ConfigKafka() {
	FlagSet := flag.NewFlagSet("kafka", flag.ExitOnError)
	FlagSet.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	FlagSet.StringVar(&kafkaVersionStr, "kafka-version", "2.0.0", "Kafka version in semver format. All brokers must be this version or newer.")
	FlagSet.StringVar(&topic, "topic", "metrictank", "kafka topic")
	FlagSet.IntVar(&fallbackNumPartitions, "fallback-num-partitions", 10000, "Number of partitions for kafka topic. Only used if it cannot be resolved from kafka")

	kafkaNet = kafka.ConfigNet(FlagSet)

	globalconf.Register("kafka", FlagSet, flag.ExitOnError)
}

func producerConfig() *sarama.Config {

	kafkaVersion, err := sarama.ParseKafkaVersion(kafkaVersionStr)
	if err != nil {
		log.Fatalf("kafka-cluster: invalid kafka-version. %s", err)
	}

	brokers = strings.Split(brokerStr, ",")

	config := sarama.NewConfig()
	config.ClientID = "control-cluster"
	config.Version = kafkaVersion
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	kafkaNet.Configure(config)

	err = config.Validate()
	if err != nil {
		log.Fatalf("kafka: invalid config: %s", err)
	}

	return config
}

type Producer struct {
	client   sarama.Client
	producer sarama.SyncProducer
}

func NewProducer() *Producer {
	config := producerConfig()
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("kafka: failed to start client: %s", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalf("kafka: failed to initialize producer: %s", err)
	}

	return &Producer{client: client, producer: producer}
}

func (p *Producer) numPartitions() int {
	partitions, err := p.client.Partitions(topic)
	if err != nil {
		log.Warnf("Failed to get partitions for topic %s, defaulting to %d: err = %s", topic, fallbackNumPartitions, err)
		return fallbackNumPartitions
	}
	return len(partitions)
}
