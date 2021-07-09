package notifierKafka

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

var Enabled bool
var kafkaVersionStr string
var brokerStr string
var brokers []string
var topic string
var offsetStr string
var config *sarama.Config
var offsetDuration time.Duration
var partitionStr string
var partitions []int32
var backlogProcessTimeout time.Duration
var backlogProcessTimeoutStr string
var partitionOffset map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionLag map[int32]*stats.Gauge64
var kafkaNet *kafka.KafkaNet

var FlagSet *flag.FlagSet

// metric cluster.notifier.kafka.messages-published is a counter of messages published to the kafka cluster notifier
var messagesPublished = stats.NewCounter32("cluster.notifier.kafka.messages-published")

// metric cluster.notifier.kafka.message_size is the sizes seen of messages through the kafka cluster notifier
var messagesSize = stats.NewMeter32("cluster.notifier.kafka.message_size", false)

func init() {
	FlagSet = flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	FlagSet.BoolVar(&Enabled, "enabled", false, "")
	FlagSet.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	FlagSet.StringVar(&kafkaVersionStr, "kafka-version", "2.0.0", "Kafka version in semver format. All brokers must be this version or newer.")
	FlagSet.StringVar(&topic, "topic", "metricpersist", "kafka topic")
	FlagSet.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's. This should match the partitions used for kafka-mdm-in")
	FlagSet.StringVar(&offsetStr, "offset", "newest", "Set the offset to start consuming from. Can be oldest, newest or a time duration")
	FlagSet.StringVar(&backlogProcessTimeoutStr, "backlog-process-timeout", "60s", "Maximum time backlog processing can block during metrictank startup. Setting to a low value may result in data loss")

	kafkaNet = kafka.ConfigNet(FlagSet)

	globalconf.Register("kafka-cluster", FlagSet, flag.ExitOnError)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(kafkaVersionStr)
	if err != nil {
		log.Fatalf("kafka-cluster: invalid kafka-version. %s", err)
	}

	switch offsetStr {
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatalf("kafka-cluster: invalid offest format. %s", err)
		}
	}
	brokers = strings.Split(brokerStr, ",")

	config = sarama.NewConfig()
	config.ClientID = instance + "-cluster"
	config.Version = kafkaVersion
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	kafkaNet.Configure(config)

	err = config.Validate()
	if err != nil {
		log.Fatalf("kafka-cluster: invalid consumer config: %s", err)
	}

	backlogProcessTimeout, err = time.ParseDuration(backlogProcessTimeoutStr)
	if err != nil {
		log.Fatalf("kafka-cluster: unable to parse backlog-process-timeout. %s", err)
	}

	if partitionStr != "*" {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.Fatalf("kafka-cluster: could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			partitions = append(partitions, int32(i))
		}
	}
	// validate our partitions
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("kafka-cluster: failed to create client. %s", err)
	}
	defer client.Close()

	availParts, err := kafka.GetPartitions(client, []string{topic})
	if err != nil {
		log.Fatalf("kafka-cluster: %s", err.Error())
	}
	if partitionStr == "*" {
		partitions = availParts
	} else {
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.Fatalf("kafka-cluster: configured partitions not in list of available partitions. missing %v", missing)
		}
	}

	// initialize our offset metrics
	partitionOffset = make(map[int32]*stats.Gauge64)
	partitionLogSize = make(map[int32]*stats.Gauge64)
	partitionLag = make(map[int32]*stats.Gauge64)

	for _, part := range partitions {
		// metric cluster.notifier.kafka.partition.%d.offset is the current offset for the partition (%d) that we have consumed
		partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.offset", part))
		// metric cluster.notifier.kafka.partition.%d.log_size is the size of the kafka partition (%d), aka the newest available offset.
		partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.log_size", part))
		// metric cluster.notifier.kafka.partition.%d.lag is how many messages (mechunkWriteRequestsrics) there are in the kafka
		// partition (%d) that we have not yet consumed.
		partitionLag[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.lag", part))
	}
	log.Infof("kafka-cluster: consuming from partitions %v", partitions)
}
