package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cmd/mt-control-server/controlmodels"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/grafana/metrictank/util"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
	"gopkg.in/macaron.v1"
)

var metrictankUrl string
var maxDefsPerMsg int
var indexConcurrency int

func ConfigHandlers() {
	FlagSet := flag.NewFlagSet("handlers", flag.ExitOnError)

	FlagSet.StringVar(&metrictankUrl, "metrictank-url", "http://metrictank:6060", "URL for MT cluster")
	FlagSet.IntVar(&maxDefsPerMsg, "max-defs-per-msg", 1000, "Maximum defs per control message")
	FlagSet.IntVar(&indexConcurrency, "index-concurrency", 5, "Concurrent queries to run against cassandra (per request)")

	globalconf.Register("handlers", FlagSet, flag.ExitOnError)
}

func makeLogId() string {
	logId := "uninit"
	uuid, err := gocql.RandomUUID()
	if err == nil {
		logId = uuid.String()
	}
	return logId
}

func makeMessage(partition int32, op schema.Operation, cm *schema.ControlMsg) (*sarama.ProducerMessage, error) {
	cm.Op = op
	var b bytes.Buffer
	b.WriteByte(byte(msg.FormatIndexControlMessage))
	w := msgp.NewWriterSize(&b, 300)
	err := cm.EncodeMsg(w)
	if err != nil {
		return nil, err
	}
	w.Flush()

	return &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(b.Bytes()),
		Partition: partition,
	}, nil
}

func appStatus(ctx *macaron.Context) {
	ctx.PlainText(200, []byte("OK"))
}

func tagsDelByQuery(ctx *macaron.Context, request controlmodels.IndexDelByQueryReq) {
	logId := makeLogId()

	log.Infof("Received tagsDelByQuery: logId = %s, req = %v", logId, request)

	// Translate to FindSeries
	findSeries := models.GraphiteTagFindSeries{
		Expr:   request.Expr,
		Limit:  request.Limit,
		To:     request.OlderThan,
		Meta:   true,
		Format: "full-json",
	}
	reqBody, err := json.Marshal(findSeries)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}
	// send to query address
	resp, err := http.Post(metrictankUrl+"/tags/findSeries", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Read in body so that the connection can be reused
		io.Copy(ioutil.Discard, resp.Body)
		response.Write(ctx, response.WrapError(fmt.Errorf("Request to cluster failed with status = %d", resp.StatusCode)))
		return
	}

	findSeriesResult := models.GraphiteTagFindSeriesFullResp{}
	err = json.NewDecoder(resp.Body).Decode(&findSeriesResult)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}

	log.Infof("job %s: Pulled %d series matching filters", logId, len(findSeriesResult.Series))

	retval := controlmodels.IndexDelByQueryResp{}
	retval.Count = len(findSeriesResult.Series)
	retval.Partitions = make(map[int]int)

	for _, s := range findSeriesResult.Series {
		retval.Partitions[int(s.Partition)]++
	}

	if request.Method != "dry-run" {
		op := schema.OpRemove
		if request.Method == "archive" {
			op = schema.OpArchive
		}

		var msgs []*sarama.ProducerMessage

		// Create message per partition
		partitionMsgs := make(map[int32]*schema.ControlMsg)
		for _, s := range findSeriesResult.Series {
			if _, ok := partitionMsgs[s.Partition]; !ok {
				partitionMsgs[s.Partition] = &schema.ControlMsg{}
			}
			cm := partitionMsgs[s.Partition]
			cm.Defs = append(cm.Defs, s)

			if len(cm.Defs) >= maxDefsPerMsg {
				msg, err := makeMessage(s.Partition, op, cm)
				if err == nil {
					msgs = append(msgs, msg)
				}
				partitionMsgs[s.Partition] = &schema.ControlMsg{}
			}
		}

		for partition, cm := range partitionMsgs {
			if len(cm.Defs) == 0 {
				continue
			}
			msg, err := makeMessage(partition, op, cm)
			if err == nil {
				msgs = append(msgs, msg)
			}
		}

		if len(msgs) > 0 {
			go func() {
				pre := time.Now()
				err = producer.producer.SendMessages(msgs)
				log.Infof("job %s: Sending %d batches took %v", logId, len(msgs), time.Since(pre))
			}()
		}
	}

	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}
	response.Write(ctx, response.NewJson(200, retval, ""))
}

func tagsRestore(ctx *macaron.Context, request controlmodels.IndexRestoreReq) {
	// For large archives, this could take quite a long time. Process asynchronously and
	// return response to know the job was kicked off. Provide an identifier to view logs.
	logId := makeLogId()

	log.Infof("Received tagsRestore: logId = %s, req = %v", logId, request)

	// Pre-process expressions
	filter, err := tagquery.NewQueryFromStrings(request.Expr, request.From, request.To)
	if err != nil {
		response.Write(ctx, response.WrapError(err))
		return
	}
	filtersByTag := make(map[string][]tagquery.Expression)
	for _, expr := range filter.Expressions {
		filtersByTag[expr.GetKey()] = append(filtersByTag[expr.GetKey()], expr)
	}

	numPartitions := request.NumPartitions
	if numPartitions < 1 {
		numPartitions = producer.numPartitions()
	}

	scanPartition := func(partition int32) {
		q := fmt.Sprintf("SELECT id, orgid, name, interval, unit, mtype, tags, lastupdate FROM %s WHERE partition = ?", cass.Config.ArchiveTable)

		var id, name, unit, mtype string
		var orgId, interval int
		var lastupdate int64
		var tags []string

		var defs []schema.MetricDefinition

		maxLastUpdate := time.Now().Unix()

		session := cass.Session.CurrentSession()
		iter := session.Query(q, partition).Iter()

	ITER:
		for iter.Scan(&id, &orgId, &name, &interval, &unit, &mtype, &tags, &lastupdate) {

			// Quick filter on lastupdate
			if filter.From > 0 && lastupdate < filter.From {
				continue ITER
			}

			if filter.To > 0 && lastupdate > filter.To {
				continue ITER
			}

			// Tags to map
			tagMap := make(map[string]string)
			for _, tag := range tags {
				tagParts := strings.SplitN(tag, "=", 2)
				if len(tagParts) != 2 {
					continue
				}
				tagMap[tagParts[0]] = tagParts[1]
			}

			idTagLookup := func(_ schema.MKey, tag, value string) bool {
				return tagMap[tag] == value
			}

			mkey, err := schema.MKeyFromString(id)
			if err != nil {
				log.Errorf("job=%s: load() could not parse ID %q: %s -> skipping", logId, id, err)
				continue
			}

			for _, expr := range filter.Expressions {
				matchFunc := expr.GetMetricDefinitionFilter(idTagLookup)
				if matchFunc(mkey, name, tags) == tagquery.Fail {
					continue ITER
				}
			}

			if orgId < 0 {
				orgId = int(idx.OrgIdPublic)
			}

			def := schema.MetricDefinition{
				Id:         mkey,
				OrgId:      uint32(orgId),
				Partition:  partition,
				Name:       name,
				Interval:   interval,
				Unit:       unit,
				Mtype:      mtype,
				Tags:       tags,
				LastUpdate: util.MinInt64(maxLastUpdate, lastupdate+request.LastUpdateOffset),
			}

			defs = append(defs, def)

		}
		if err := iter.Close(); err != nil {
			log.Errorf("job=%s: could not close iterator for partition = %d: %s", logId, partition, err.Error())
		}

		log.Infof("job=%s: Restoring %d series from partition %d", logId, len(defs), partition)

		if len(defs) == 0 {
			return
		}

		// Chunk into batches
		var defBatches [][]schema.MetricDefinition
		for i := 0; i < len(defs); i += maxDefsPerMsg {
			end := i + maxDefsPerMsg

			if end > len(defs) {
				end = len(defs)
			}

			defBatches = append(defBatches, defs[i:end])
		}

		for _, batch := range defBatches {
			cm := &schema.ControlMsg{
				Defs: batch,
				Op:   schema.OpRestore,
			}

			msg, err := makeMessage(partition, schema.OpRestore, cm)
			if err != nil {
				log.Warnf("job=%s: Failed to encode ControlMsg from partition = %d, err = %s", logId, partition, err)
				continue
			}

			_, _, err = producer.producer.SendMessage(msg)
			if err != nil {
				log.Warnf("job=%s: Failed to send ControlMsg for partition = %d, err = %s", logId, partition, err)
				continue
			}
		}
	}

	go func() {
		partitions := make(chan int, indexConcurrency)
		var wg sync.WaitGroup
		for i := 0; i < indexConcurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for partition := range partitions {
					scanPartition(int32(partition))
				}
			}()
		}

		for p := 0; p < numPartitions; p++ {
			partitions <- p
		}

		wg.Wait()
	}()

	retval := controlmodels.IndexRestoreResp{LogId: logId}
	response.Write(ctx, response.NewJson(200, retval, ""))
}
