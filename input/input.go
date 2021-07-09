// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"flag"
	"fmt"
	"math"
	"strconv"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

var (
	rejectInvalidInput bool
)

func ConfigSetup() {
	input := flag.NewFlagSet("input", flag.ExitOnError)
	input.BoolVar(&rejectInvalidInput, "reject-invalid-input", true, "reject received metrics that have invalid input data")
	globalconf.Register("input", input, flag.ExitOnError)
}

type Handler interface {
	ProcessMetricData(md *schema.MetricData, partition int32)
	ProcessMetricPoint(point schema.MetricPoint, format msg.Format, partition int32)
	ProcessIndexControlMsg(msg schema.ControlMsg, partition int32)
}

// TODO: clever way to document all metrics for all different inputs

// Default is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type DefaultHandler struct {
	receivedMD     *stats.Counter32
	receivedMP     *stats.Counter32
	receivedMPNO   *stats.Counter32
	invalidMD      *stats.CounterRate32
	invalidInputMD *stats.CounterRate32
	invalidMP      *stats.CounterRate32
	unknownMP      *stats.Counter32

	metrics     mdata.Metrics
	metricIndex idx.MetricIndex
}

// Possible reason labels for Prometheus metric discarded_samples_total
const (
	invalidTimestamp = "invalid-timestamp"
	invalidInterval  = "invalid-interval"
	invalidOrgId     = "invalid-orgID"
	invalidName      = "invalid-name"
	invalidMtype     = "invalid-mtype"
	invalidInput     = "invalid-input"
	unknownPointId   = "unknown-point-id"
)

func NewDefaultHandler(metrics mdata.Metrics, metricIndex idx.MetricIndex, input string) DefaultHandler {
	return DefaultHandler{
		// metric input.%s.metricdata.received is the count of metricdata datapoints received by input plugin
		receivedMD: stats.NewCounter32(fmt.Sprintf("input.%s.metricdata.received", input)),
		// metric input.%s.metricpoint.received is the count of metricpoint datapoints received by input plugin
		receivedMP: stats.NewCounter32(fmt.Sprintf("input.%s.metricpoint.received", input)),
		// metric input.%s.metricpoint_no_org.received is the count of metricpoint_no_org datapoints received by input plugin
		receivedMPNO: stats.NewCounter32(fmt.Sprintf("input.%s.metricpoint_no_org.received", input)),
		// metric input.%s.metricdata.discarded.invalid is a count of times a metricdata was invalid by input plugin
		invalidMD: stats.NewCounterRate32(fmt.Sprintf("input.%s.metricdata.discarded.invalid", input)),
		// metric input.%s.metricdata.discarded.invalid_input is a count of times a metricdata was considered invalid due to
		// invalid input data in the metric definition. all rejected metrics counted here are also counted in the above "invalid" counter
		invalidInputMD: stats.NewCounterRate32(fmt.Sprintf("input.%s.metricdata.discarded.invalid_input", input)),
		// metric input.%s.metricpoint.discarded.invalid is a count of times a metricpoint was invalid by input plugin
		invalidMP: stats.NewCounterRate32(fmt.Sprintf("input.%s.metricpoint.discarded.invalid", input)),
		// metric input.%s.metricpoint.discarded.unknown is the count of times the ID of a received metricpoint was not in the index, by input plugin
		unknownMP: stats.NewCounter32(fmt.Sprintf("input.%s.metricpoint.discarded.unknown", input)),

		metrics:     metrics,
		metricIndex: metricIndex,
	}
}

// ProcessMetricPoint updates the index if possible, and stores the data if we have an index entry
// concurrency-safe.
func (in DefaultHandler) ProcessMetricPoint(point schema.MetricPoint, format msg.Format, partition int32) {
	if format == msg.FormatMetricPoint {
		in.receivedMP.Inc()
	} else {
		in.receivedMPNO.Inc()
	}
	// in cassandra we store timestamps as 32bit signed integers.
	// math.MaxInt32 = Jan 19 03:14:07 UTC 2038
	if !point.Valid() || point.Time >= math.MaxInt32 {
		in.invalidMP.Inc()
		mdata.PromDiscardedSamples.WithLabelValues(invalidTimestamp, strconv.Itoa(int(point.MKey.Org))).Inc()
		log.Debugf("in: Invalid metric %v", point)
		return
	}

	archive, _, ok := in.metricIndex.Update(point, partition)

	if !ok {
		in.unknownMP.Inc()
		mdata.PromDiscardedSamples.WithLabelValues(unknownPointId, strconv.Itoa(int(point.MKey.Org))).Inc()
		return
	}

	m := in.metrics.GetOrCreate(point.MKey, archive.SchemaId, archive.AggId, uint32(archive.Interval))
	m.Add(point.Time, point.Value)
}

// ProcessMetricData assures the data is stored and the metadata is in the index
// concurrency-safe.
func (in DefaultHandler) ProcessMetricData(md *schema.MetricData, partition int32) {
	in.receivedMD.Inc()
	err := md.Validate()
	if err != nil {
		in.invalidMD.Inc()

		ignoreError := false
		// assuming that invalid input was the only issue found by Validate()
		// better make sure that invalid input is the last check which Validate() checks, to not accidentally ignore a potential following error
		if err == schema.ErrInvalidInput {
			in.invalidInputMD.Inc()
			if !rejectInvalidInput {
				if log.IsLevelEnabled(log.DebugLevel) {
					log.Debugf("in: Invalid metric %v, not rejecting it because rejection due to invalid input is disabled: %s", md, err)
				}
				ignoreError = true
			}
		}

		if !ignoreError {
			log.Debugf("in: Invalid metric %v: %s", md, err)

			var reason string
			switch err {
			case schema.ErrInvalidIntervalzero:
				reason = invalidInterval
			case schema.ErrInvalidOrgIdzero:
				reason = invalidOrgId
			case schema.ErrInvalidEmptyName:
				reason = invalidName
			case schema.ErrInvalidMtype:
				reason = invalidMtype
			case schema.ErrInvalidInput:
				reason = invalidInput
			default:
				reason = "unknown"
			}
			mdata.PromDiscardedSamples.WithLabelValues(reason, strconv.Itoa(md.OrgId)).Inc()

			return
		}
	}

	// in cassandra we store timestamps and interval as 32bit signed integers.
	// math.MaxInt32 = Jan 19 03:14:07 UTC 2038
	if md.Time <= 0 || md.Time >= math.MaxInt32 {
		in.invalidMD.Inc()
		mdata.PromDiscardedSamples.WithLabelValues(invalidTimestamp, strconv.Itoa(md.OrgId)).Inc()
		log.Warnf("in: invalid metric %q: .Time %d out of range", md.Id, md.Time)
		return
	}
	if md.Interval <= 0 || md.Interval >= math.MaxInt32 {
		in.invalidMD.Inc()
		mdata.PromDiscardedSamples.WithLabelValues(invalidInterval, strconv.Itoa(md.OrgId)).Inc()
		log.Warnf("in: invalid metric %q. .Interval %d out of range", md.Id, md.Interval)
		return
	}

	mkey, err := schema.MKeyFromString(md.Id)
	if err != nil {
		log.Errorf("in: Invalid metric %v: could not parse ID: %s", md, err)
		return
	}

	archive, _, _ := in.metricIndex.AddOrUpdate(mkey, md, partition)

	m := in.metrics.GetOrCreate(mkey, archive.SchemaId, archive.AggId, uint32(md.Interval))
	m.Add(uint32(md.Time), md.Value)
}

// ProcessIndexControlMsg updates the index if possible
// concurrency-safe.
func (in DefaultHandler) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {
	switch msg.Op {
	case schema.OpRemove:
		in.metricIndex.DeleteDefs(msg.Defs, false)
	case schema.OpArchive:
		in.metricIndex.DeleteDefs(msg.Defs, true)
	case schema.OpRestore:
		in.metricIndex.AddDefs(msg.Defs)
	}
}
