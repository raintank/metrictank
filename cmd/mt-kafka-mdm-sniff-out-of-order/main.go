package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/grafana/globalconf"
	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

var (
	confFile    = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")
	format      = flag.String("format", "{{.Bad.Md.Id}} {{.Bad.Md.Name}} {{.Bad.Mp.MKey}} {{.DeltaTime}} {{.DeltaSeen}} {{.NumBad}}", "template to render event with")
	prefix      = flag.String("prefix", "", "only show metrics with a name that has this prefix")
	substr      = flag.String("substr", "", "only show metrics with a name that has this substring")
	doUnknownMP = flag.Bool("do-unknown-mp", true, "process MetricPoint messages for which no MetricData messages have been seen. If you use prefix/substr filter, this may report on metrics you wanted to filter out!")
)

type Tracker struct {
	Head      Msg    // last successfully added message
	Bad       Msg    // current (last seen) point that could not be added (assuming no re-order buffer)
	NumBad    int    // number of failed points since last successful add
	DeltaTime uint32 // delta between Head and Bad time properties in seconds (point timestamps)
	DeltaSeen uint32 // delta between Head and Bad seen time in seconds (consumed from kafka)
}

type Msg struct {
	Part int32
	Seen time.Time
	Md   schema.MetricData // either this one or below will be valid depending on input
	Mp   schema.MetricPoint
}

func (m Msg) Time() uint32 {
	if m.Md.Id != "" {
		return uint32(m.Md.Time)
	}
	return m.Mp.Time
}

// find out of order metrics
type inputOOOFinder struct {
	tpl  template.Template
	data map[schema.MKey]Tracker
	lock sync.Mutex
}

func newInputOOOFinder(format string) *inputOOOFinder {
	tpl := template.Must(template.New("format").Parse(format + "\n"))
	return &inputOOOFinder{
		*tpl,
		make(map[schema.MKey]Tracker),
		sync.Mutex{},
	}
}

func (ip *inputOOOFinder) ProcessMetricData(metric *schema.MetricData, partition int32) {
	if *prefix != "" && !strings.HasPrefix(metric.Name, *prefix) {
		return
	}
	if *substr != "" && !strings.Contains(metric.Name, *substr) {
		return
	}
	mkey, err := schema.MKeyFromString(metric.Id)
	if err != nil {
		log.Errorf("could not parse id %q: %s", metric.Id, err.Error())
		return
	}

	now := Msg{
		Part: partition,
		Seen: time.Now(),
		Md:   *metric,
	}
	ip.lock.Lock()
	tracker, ok := ip.data[mkey]
	if !ok {
		ip.data[mkey] = Tracker{
			Head: now,
		}
	} else {
		if uint32(metric.Time) > tracker.Head.Time() {
			tracker.Head = now
			tracker.NumBad = 0
			ip.data[mkey] = tracker
		} else {
			// if metric time <= head point time, update "bad", generate event and print
			tracker.Bad = now
			tracker.NumBad += 1
			tracker.DeltaTime = tracker.Head.Time() - uint32(metric.Time)
			tracker.DeltaSeen = uint32(now.Seen.Unix()) - uint32(tracker.Head.Seen.Unix())
			err := ip.tpl.Execute(os.Stdout, tracker)
			if err != nil {
				log.Errorf("executing template: %s", err.Error())
			}
			ip.data[mkey] = tracker
		}
	}
	ip.lock.Unlock()
}

func (ip *inputOOOFinder) ProcessMetricPoint(mp schema.MetricPoint, format msg.Format, partition int32) {
	now := Msg{
		Part: partition,
		Seen: time.Now(),
		Mp:   mp,
	}
	ip.lock.Lock()
	tracker, ok := ip.data[mp.MKey]
	if !ok {
		if !*doUnknownMP {
			return
		}
		ip.data[mp.MKey] = Tracker{
			Head: now,
		}
	} else {
		if mp.Time > tracker.Head.Time() {
			// highest TS seen so far -> update "head"
			tracker.Head = now
			tracker.NumBad = 0
			ip.data[mp.MKey] = tracker
		} else {
			// if metric time <= head point time, update "bad", generate event and print
			tracker.Bad = now
			tracker.NumBad += 1
			tracker.DeltaTime = tracker.Head.Time() - mp.Time
			tracker.DeltaSeen = uint32(now.Seen.Unix()) - uint32(tracker.Head.Seen.Unix())
			err := ip.tpl.Execute(os.Stdout, tracker)
			if err != nil {
				log.Errorf("executing template: %s", err.Error())
			}
			ip.data[mp.MKey] = tracker
		}
	}
	ip.lock.Unlock()
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-sniff-out-of-order")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports out of order data (does not take into account reorder buffer)")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "# Mechanism")
		fmt.Fprintln(os.Stderr, "* it sniffs points being added on a per-series (metric Id) level")
		fmt.Fprintln(os.Stderr, "* for every series, tracks the last 'correct' point.  E.g. a point that was able to be added to the series because its timestamp is higher than any previous timestamp")
		fmt.Fprintln(os.Stderr, "* if for any series, a point comes in with a timestamp equal or lower than the last point correct point - which metrictank would not add unless it falls within the reorder buffer - it triggers an event for this out-of-order point")
		fmt.Fprintln(os.Stderr, "every event is printed using the specified, respective format based on the message format")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "# Event formatting")
		fmt.Fprintln(os.Stderr, "Uses standard golang templating. E.g. {{field}} with these available fields:")
		fmt.Fprintln(os.Stderr, "NumBad - number of failed points since last successful add")
		fmt.Fprintln(os.Stderr, "DeltaTime - delta between Head and Bad time properties in seconds (point timestamps)")
		fmt.Fprintln(os.Stderr, "DeltaSeen - delta between Head and Bad seen time in seconds (consumed from kafka)")
		fmt.Fprintln(os.Stderr, ".Head.* - head is last successfully added message")
		fmt.Fprintln(os.Stderr, ".Bad.* - Bad is the current (last seen) point that could not be added (assuming no re-order buffer)")
		fmt.Fprintln(os.Stderr, "under Head and Bad, the following subfields are available:")
		fmt.Fprintln(os.Stderr, "Part (partition) and Seen (when the msg was consumed from kafka)")
		fmt.Fprintln(os.Stderr, "for MetricData, prefix these with Md. : Time OrgId Id Name Metric Interval Value Unit Mtype Tags")
		fmt.Fprintln(os.Stderr, "for MetricPoint, prefix these with Mp. : Time MKey Value")

		//	formatMd = flag.String("format-md", "{{.Part}} {{.OrgId}} {{.Id}} {{.Name}} {{.Interval}} {{.Value}} {{.Time}} {{.Unit}} {{.Mtype}} {{.Tags}}", "template to render MetricData with")
		//	formatP  = flag.String("format-point", "{{.Part}} {{.MKey}} {{.Value}} {{.Time}}", "template to render MetricPoint data with")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
	instance := "mt-kafka-mdm-sniff-out-of-order" + strconv.Itoa(rand.Int())

	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(*confFile); err == nil {
		path = *confFile
	}
	conf, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		log.Fatalf("error with configuration file: %s", err.Error())
		os.Exit(1)
	}
	inKafkaMdm.ConfigSetup()
	conf.ParseAll()

	// config may have had it disabled
	inKafkaMdm.Enabled = true

	inKafkaMdm.ConfigProcess(instance)

	stats.NewDevnull() // make sure metrics don't pile up without getting discarded

	mdm := inKafkaMdm.New()
	ctx, cancel := context.WithCancel(context.Background())
	mdm.Start(newInputOOOFinder(*format), cancel)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	case <-ctx.Done():
		log.Info("Mdm input plugin signalled a fatal error. Shutting down")
	}
	mdm.Stop()
}
