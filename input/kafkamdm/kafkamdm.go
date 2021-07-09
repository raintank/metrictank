package kafkamdm

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounterRate32("input.kafka-mdm.metrics_decode_err")

type KafkaMdm struct {
	input.Handler
	consumer   sarama.Consumer
	client     sarama.Client
	lagMonitor *LagMonitor
	wg         sync.WaitGroup

	shutdown chan struct{}
	// signal to caller that it should shutdown
	cancel context.CancelFunc
}

func (k *KafkaMdm) Name() string {
	return "kafka-mdm"
}

var Enabled bool
var orgId uint
var kafkaVersionStr string
var brokerStr string
var brokers []string
var topicStr string
var topics []string
var partitionStr string
var partitions []int32
var offsetStr string
var config *sarama.Config
var channelBufferSize int
var consumerFetchMin int
var consumerFetchDefault int
var consumerMaxWaitTime time.Duration
var consumerMaxProcessingTime time.Duration
var netMaxOpenRequests int
var offsetDuration time.Duration
var kafkaStats stats.Kafka
var kafkaNet *kafka.KafkaNet

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.UintVar(&orgId, "org-id", 0, "For incoming MetricPoint messages without org-id, assume this org id")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&kafkaVersionStr, "kafka-version", "2.0.0", "Kafka version in semver format. All brokers must be this version or newer.")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offsetStr, "offset", "newest", "Set the offset to start consuming from. Can be oldest, newest or a time duration")
	inKafkaMdm.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000, "The number of metrics to buffer in internal and external channels")
	inKafkaMdm.IntVar(&consumerFetchMin, "consumer-fetch-min", 1, "The minimum number of message bytes to fetch in a request")
	inKafkaMdm.IntVar(&consumerFetchDefault, "consumer-fetch-default", 32768, "The default number of message bytes to fetch in a request")
	inKafkaMdm.DurationVar(&consumerMaxWaitTime, "consumer-max-wait-time", time.Second, "The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyway")
	inKafkaMdm.DurationVar(&consumerMaxProcessingTime, "consumer-max-processing-time", time.Second, "The maximum amount of time the consumer expects a message takes to process")
	inKafkaMdm.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "How many outstanding requests a connection is allowed to have before sending on it blocks")

	kafkaNet = kafka.ConfigNet(inKafkaMdm)

	globalconf.Register("kafka-mdm-in", inKafkaMdm, flag.ExitOnError)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(kafkaVersionStr)
	if err != nil {
		log.Fatalf("kafkamdm: invalid kafka-version. %s", err)
	}

	if consumerMaxWaitTime == 0 {
		log.Fatal("kafkamdm: consumer-max-wait-time must be greater then 0")
	}
	if consumerMaxProcessingTime == 0 {
		log.Fatal("kafkamdm: consumer-max-processing-time must be greater then 0")
	}

	switch offsetStr {
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatalf("kafkamdm: invalid offest format. %s", err)
		}
	}

	brokers = strings.Split(brokerStr, ",")
	topics = strings.Split(topicStr, ",")

	config = sarama.NewConfig()

	config.ClientID = instance + "-mdm"
	config.ChannelBufferSize = channelBufferSize
	config.Consumer.Fetch.Min = int32(consumerFetchMin)
	config.Consumer.Fetch.Default = int32(consumerFetchDefault)
	config.Consumer.MaxWaitTime = consumerMaxWaitTime
	config.Consumer.MaxProcessingTime = consumerMaxProcessingTime
	config.Net.MaxOpenRequests = netMaxOpenRequests
	config.Version = kafkaVersion

	kafkaNet.Configure(config)

	err = config.Validate()
	if err != nil {
		log.Fatalf("kafkamdm: invalid config: %s", err)
	}
	// validate our partitions
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("kafkamdm: failed to create client. %s", err)
	}
	defer client.Close()

	availParts, err := kafka.GetPartitions(client, topics)
	if err != nil {
		log.Fatalf("kafkamdm: %s", err.Error())
	}
	log.Infof("kafkamdm: available partitions %v", availParts)
	if partitionStr == "*" {
		partitions = availParts
	} else {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.Fatalf("could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			partitions = append(partitions, int32(i))
		}
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.Fatalf("kafkamdm: configured partitions not in list of available partitions. missing %v", missing)
		}
	}
	// record our partitions so others (MetricIdx) can use the partitioning information.
	// but only if the manager has been created (e.g. in metrictank), not when this input plugin is used in other contexts
	if cluster.Manager != nil {
		cluster.Manager.SetPartitions(partitions)
	}

	// the extra empty newlines are because metrics2docs doesn't recognize the comments properly otherwise
	// metric input.kafka-mdm.partition.%d.offset is the current offset for the partition (%d) that we have consumed.

	// metric input.kafka-mdm.partition.%d.log_size is the current size of the kafka partition (%d), aka the newest available offset.

	// metric input.kafka-mdm.partition.%d.lag is how many messages (metrics) there are in the kafka partition (%d) that we have not yet consumed.
	kafkaStats = stats.NewKafka("input.kafka-mdm", partitions)
}

func New() *KafkaMdm {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("kafkamdm: failed to create client. %s", err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("kafkamdm: failed to create consumer: %s", err)
	}
	log.Info("kafkamdm: consumer created without error")
	k := KafkaMdm{
		consumer:   consumer,
		client:     client,
		lagMonitor: NewLagMonitor(10, partitions),
		shutdown:   make(chan struct{}),
	}

	return &k
}

func (k *KafkaMdm) Start(handler input.Handler, cancel context.CancelFunc) error {
	k.Handler = handler
	k.cancel = cancel
	var err error
	for _, topic := range topics {
		for _, partition := range partitions {
			var offset int64
			switch offsetStr {
			case "oldest":
				offset = sarama.OffsetOldest
			case "newest":
				offset = sarama.OffsetNewest
			default:
				offset, err = k.client.GetOffset(topic, partition, time.Now().Add(-1*offsetDuration).UnixNano()/int64(time.Millisecond))
				if err != nil {
					offset = sarama.OffsetOldest
					log.Warnf("kafkamdm: failed to get offset %s: %s -> will use oldest instead", offsetDuration, err)
				}
			}
			k.wg.Add(1)
			go k.consumePartition(topic, partition, offset)
		}
	}
	return nil
}

// tryGetOffset will to query kafka repeatedly for the requested offset and give up after attempts unsuccesfull attempts
// an error is returned when it had to give up
func (k *KafkaMdm) tryGetOffset(topic string, partition int32, offset int64, attempts int, sleep time.Duration) (int64, error) {

	var val int64
	var err error
	var offsetStr string

	switch offset {
	case sarama.OffsetNewest:
		offsetStr = "newest"
	case sarama.OffsetOldest:
		offsetStr = "oldest"
	default:
		offsetStr = strconv.FormatInt(offset, 10)
	}

	attempt := 1
	for {
		val, err = k.client.GetOffset(topic, partition, offset)
		if err == nil {
			break
		}

		err = fmt.Errorf("failed to get offset %s of partition %s:%d. %s (attempt %d/%d)", offsetStr, topic, partition, err, attempt, attempts)
		if attempt == attempts {
			break
		}
		log.Warnf("kafkamdm: %s", err.Error())
		attempt += 1
		time.Sleep(sleep)
	}
	return val, err
}

// consumePartition consumes from the topic until k.shutdown is triggered.
func (k *KafkaMdm) consumePartition(topic string, partition int32, currentOffset int64) {
	defer k.wg.Done()

	// determine the pos of the topic and the initial offset of our consumer
	newest, err := k.tryGetOffset(topic, partition, sarama.OffsetNewest, 7, time.Second*10)
	if err != nil {
		log.Errorf("kafkamdm: %s", err.Error())
		k.cancel()
		return
	}
	if currentOffset == sarama.OffsetNewest {
		currentOffset = newest
	} else if currentOffset == sarama.OffsetOldest {
		currentOffset, err = k.tryGetOffset(topic, partition, sarama.OffsetOldest, 7, time.Second*10)
		if err != nil {
			log.Errorf("kafkamdm: %s", err.Error())
			k.cancel()
			return
		}
	}

	kafkaStats := kafkaStats[partition]
	kafkaStats.Offset.Set(int(currentOffset))
	kafkaStats.LogSize.Set(int(newest))
	kafkaStats.Lag.Set(int(newest - currentOffset))
	kafkaStats.Priority.Set(k.lagMonitor.GetPartitionPriority(partition))
	go k.trackStats(topic, partition)

	log.Infof("kafkamdm: consuming from %s:%d from offset %d", topic, partition, currentOffset)
	pc, err := k.consumer.ConsumePartition(topic, partition, currentOffset)
	if err != nil {
		log.Errorf("kafkamdm: failed to start partitionConsumer for %s:%d. %s", topic, partition, err)
		k.cancel()
		return
	}
	messages := pc.Messages()
	for {
		select {
		case msg, ok := <-messages:
			// https://github.com/Shopify/sarama/wiki/Frequently-Asked-Questions#why-am-i-getting-a-nil-message-from-the-sarama-consumer
			if !ok {
				log.Errorf("kafkamdm: kafka consumer for %s:%d has shutdown. stop consuming", topic, partition)
				k.cancel()
				return
			}
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("kafkamdm: received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
			}
			k.handleMsg(msg.Value, partition)
			kafkaStats.Offset.Set(int(msg.Offset))
		case <-k.shutdown:
			pc.Close()
			log.Infof("kafkamdm: consumer for %s:%d ended.", topic, partition)
			return
		}
	}
}

func (k *KafkaMdm) handleMsg(data []byte, partition int32) {
	format, isPointMsg := msg.IsPointMsg(data)
	if isPointMsg {
		_, point, err := msg.ReadPointMsg(data, uint32(orgId))
		if err != nil {
			metricsDecodeErr.Inc()
			log.Errorf("kafkamdm: decode error, skipping message. %s", err)
			return
		}
		k.Handler.ProcessMetricPoint(point, format, partition)
		return
	}

	isControlMsg := msg.IsIndexControlMsg(data)
	if isControlMsg {
		cm, err := msg.ReadIndexControlMsg(data)
		if err != nil {
			log.Errorf("kafkamdm: decode error, skipping control message. %s", err)
			return
		}
		k.Handler.ProcessIndexControlMsg(cm, partition)
		return
	}

	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		metricsDecodeErr.Inc()
		log.Errorf("kafkamdm: decode error, skipping message. %s", err)
		return
	}
	metricsPerMessage.ValueUint32(1)
	k.Handler.ProcessMetricData(&md, partition)
}

// Stop will initiate a graceful stop of the Consumer (permanent)
// and block until it stopped.
func (k *KafkaMdm) Stop() {
	// closes notifications and messages channels, amongst others
	close(k.shutdown)
	k.wg.Wait()
	k.client.Close()
}

func (k *KafkaMdm) trackStats(topic string, partition int32) {
	ticker := time.NewTicker(time.Second)
	kafkaStats := kafkaStats[partition]
	for {
		select {
		case <-k.shutdown:
			ticker.Stop()
			return
		case ts := <-ticker.C:
			currentOffset := int64(kafkaStats.Offset.Peek())
			newest, err := k.tryGetOffset(topic, partition, sarama.OffsetNewest, 1, 0)
			if err != nil {
				log.Errorf("kafkamdm: %s", err.Error())
				continue
			}
			kafkaStats.LogSize.Set(int(newest))
			lag := int(newest - currentOffset)
			kafkaStats.Lag.Set(lag)
			k.lagMonitor.StoreOffsets(partition, currentOffset, newest, ts)
			kafkaStats.Priority.Set(k.lagMonitor.GetPartitionPriority(partition))
			if cluster.Manager != nil {
				kafkaStats.Ready.Set(cluster.Manager.IsReady())
			}
		}
	}
}

func (k *KafkaMdm) MaintainPriority() {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-k.shutdown:
				ticker.Stop()
				return
			case <-ticker.C:
				cluster.Manager.SetPriority(k.lagMonitor.Metric())
			}
		}
	}()
}

func (k *KafkaMdm) ExplainPriority() interface{} {
	return struct {
		Title       string
		Explanation interface{}
	}{
		"kafka-mdm:",
		k.lagMonitor.Explain(),
	}
}
