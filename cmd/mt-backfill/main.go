package main

import (
	"context"
	"flag"
	"fmt"
	l "log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/api"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/input"
	inKafka "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	cassandraStore "github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
)

var (
	// metrictank
	aggMetrics  *mdata.AggMetrics
	metricIndex idx.MetricIndex
	inputKafka  input.Plugin
	store       mdata.Store

	// config file
	confFile = flag.String("config", "/etc/metrictank/metrictank.ini", "config file path")

	// Data: the following configs are the same with the normal metrictank configs:
	chunkMaxStaleStr  = flag.String("chunk-max-stale", "1m", "chunk max stale age.")
	metricMaxStaleStr = flag.String("metric-max-stale", "5m", "metric max stale age.")
	gcIntervalStr     = flag.String("gc-interval", "2m", "gc interval.")
	publicOrg         = flag.Int("public-org", 0, "org Id")
	timeout           = flag.Int("timeout", 10, "the tool will exit if no kafka message is received during this interval ")

	// backfilling
	lastRcvTime int64        // epoch time when the previous kafka message was received
	mux         sync.Mutex   // mutex to protect lastRcvTime
	handler     inputHandler // input message handler to track the last kafka receive event and handles kafka messages
)

// a kafka message handler that implements the input.Handler interface
type inputHandler struct {
	handler  input.DefaultHandler // default handler that processes metric metadata and points
	finished chan bool
}

func newInputHandler(metrics mdata.Metrics, metricIndex idx.MetricIndex, pluginName string) inputHandler {
	dh := input.NewDefaultHandler(metrics, metricIndex, pluginName)
	return inputHandler{
		handler:  dh,
		finished: make(chan bool),
	}
}

// input.Handler interface
func (ih inputHandler) ProcessMetricData(metric *schema.MetricData, partition int32) {
	ih.handler.ProcessMetricData(metric, partition)
	mux.Lock()
	defer mux.Unlock()
	lastRcvTime = int64(time.Now().Unix())
}

// input.Handler interface
func (ih inputHandler) ProcessMetricPoint(point schema.MetricPoint, format msg.Format, partition int32) {
	ih.handler.ProcessMetricPoint(point, format, partition)
	mux.Lock()
	defer mux.Unlock()
	lastRcvTime = int64(time.Now().Unix())
}

func main() {
	log.Infof("metrictank backfilling")

	flag.Parse()

	// logger
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2019-03-21 10:00:00.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)

	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(*confFile); err == nil {
		path = *confFile
	}
	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration file error: %s", err)
		os.Exit(1)
	}

	// load configs
	inKafka.ConfigSetup()
	memory.ConfigSetup()
	api.ConfigSetup()
	cluster.ConfigSetup()
	mdata.ConfigSetup()
	cassandraStore.ConfigSetup()
	config.ParseAll()

	// cluster is required because of aggMetric.add()
	// this should be configured as single mode
	api.ConfigProcess()
	cluster.ConfigProcess()
	addrParts := strings.Split(api.Addr, ":")
	port, err := strconv.ParseInt(addrParts[len(addrParts)-1], 10, 64)
	if err != nil {
		log.Fatalf("Could not parse port from listenAddr. %s", api.Addr)
	}
	cluster.Init("backfill", "none", time.Now(), "http", int(port))

	// other settings
	inKafka.ConfigProcess("backfill")
	mdata.ConfigProcess()
	memory.ConfigProcess()

	// interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// backend store
	cassandraStore.CliConfig.Enabled = true
	store, err = cassandraStore.NewCassandraStore(cassandraStore.CliConfig, mdata.TTLs(), uint32(cassandraStore.CliConfig.MaxChunkSpan.Seconds()))
	if err != nil {
		log.Fatalf("failed to initialize cassandra store. %s", err)
	}

	// memory store
	chunkMaxStale := dur.MustParseNDuration("chunk-max-stale", *chunkMaxStaleStr)
	metricMaxStale := dur.MustParseNDuration("metric-max-stale", *metricMaxStaleStr)
	gcInterval := time.Duration(dur.MustParseNDuration("gc-interval", *gcIntervalStr)) * time.Second
	aggMetrics = mdata.NewAggMetrics(store, nil, false, nil /*todo*/, chunkMaxStale, metricMaxStale, gcInterval)

	// input
	// we use kafkamdm as input
	inKafka.Enabled = true
	sarama.Logger = l.New(os.Stdout, "[Sarama] ", l.LstdFlags)
	inputKafka = inKafka.New()

	// cluster manager
	cluster.Start()

	if *publicOrg < 0 {
		log.Fatal("public-org cannot be <0")
	}
	idx.OrgIdPublic = uint32(*publicOrg)

	memory.Enabled = true
	metricIndex = memory.New()

	// load index entries
	err = metricIndex.Init()
	if err != nil {
		log.Fatalf("Failed to initialize metricIndex: %s", err.Error())
	}

	// start input
	ctx, cancel := context.WithCancel(context.Background())
	handler = newInputHandler(aggMetrics, metricIndex, "kafkamdm")
	err = inputKafka.Start(handler, cancel)
	if err != nil {
		shutdown()
		log.Warn("Cannot start input.")
		return
	}
	inputKafka.MaintainPriority()
	lastRcvTime = int64(time.Now().Unix())
	go handlerTimeout()

	cluster.Manager.SetReady()

	// wait for shutdown
	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	case <-ctx.Done():
		log.Info("The input plugin signalled a fatal error. Shutting down")
	case <-handler.finished:
		log.Infof("Received finished signal from input handler. Shutting down")
	}
	shutdown()

	defer cancel()
}

// if there is no update in 3 mins, shut down the tool
func handlerTimeout() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case now := <-ticker.C:
			mux.Lock()
			prevRcv := lastRcvTime
			mux.Unlock()
			// wait for timeout seconds, if there is no more input, shut down
			if now.Unix()-prevRcv > int64(*timeout) {
				log.Infof("Handler timeout, shutting down")
				handler.finished <- true
				ticker.Stop()
				return
			}
		}
	}
}

// normal shutdown
func shutdown() {
	cluster.Stop()
	timer := time.NewTimer(time.Second * 20)
	kafkaStopped := make(chan bool)
	go func() {
		log.Infof("Shutting down kafka consumer")
		inputKafka.Stop()
		log.Infof("kafka consumer finished shutdown")
		kafkaStopped <- true
	}()

	select {
	case <-timer.C:
		log.Warn("Plugin shutdown timeout.")
	case <-kafkaStopped:
		timer.Stop()
	}
	store.Stop()
	log.Info("shutting down.")
}
