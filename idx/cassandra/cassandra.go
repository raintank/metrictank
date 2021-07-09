package cassandra

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cassandra"
	cassUtils "github.com/grafana/metrictank/cassandra"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	// metric idx.cassadra.query-insert.ok is how many insert queries for a metric completed successfully (triggered by an add or an update)
	statQueryInsertOk = stats.NewCounter32("idx.cassandra.query-insert.ok")
	// metric idx.cassandra.query-insert.fail is how many insert queries for a metric failed (triggered by an add or an update)
	statQueryInsertFail = stats.NewCounter32("idx.cassandra.query-insert.fail")
	// metric idx.cassadra.query-delete.ok is how many delete queries for a metric completed successfully (triggered by an update or a delete)
	statQueryDeleteOk = stats.NewCounter32("idx.cassandra.query-delete.ok")
	// metric idx.cassandra.query-delete.fail is how many delete queries for a metric failed (triggered by an update or a delete)
	statQueryDeleteFail = stats.NewCounter32("idx.cassandra.query-delete.fail")

	// metric idx.cassandra.query-insert.wait is time inserts spent in queue before being executed
	statQueryInsertWaitDuration = stats.NewLatencyHistogram12h32("idx.cassandra.query-insert.wait")
	// metric idx.cassandra.query-insert.exec is time spent executing inserts (possibly repeatedly until success)
	statQueryInsertExecDuration = stats.NewLatencyHistogram15s32("idx.cassandra.query-insert.exec")
	// metric idx.cassandra.query-delete.exec is time spent executing deletes (possibly repeatedly until success)
	statQueryDeleteExecDuration = stats.NewLatencyHistogram15s32("idx.cassandra.query-delete.exec")

	// metric idx.cassandra.add is the duration of an add of one metric to the cassandra idx, including the add to the in-memory index, excluding the insert query
	statAddDuration = stats.NewLatencyHistogram15s32("idx.cassandra.add")
	// metric idx.cassandra.update is the duration of an update of one metric to the cassandra idx, including the update to the in-memory index, excluding any insert/delete queries
	statUpdateDuration = stats.NewLatencyHistogram15s32("idx.cassandra.update")
	// metric idx.cassandra.prune is the duration of a prune of the cassandra idx, including the prune of the in-memory index and all needed delete queries
	statPruneDuration = stats.NewLatencyHistogram15s32("idx.cassandra.prune")
	// metric idx.cassandra.delete is the duration of a delete of one or more metrics from the cassandra idx, including the delete from the in-memory index and the delete query
	statDeleteDuration = stats.NewLatencyHistogram15s32("idx.cassandra.delete")
	// metric idx.cassandra.save.skipped is how many saves have been skipped due to the writeQueue being full
	statSaveSkipped = stats.NewCounter32("idx.cassandra.save.skipped")

	// metric idx.cassandra.control.add is the duration of add control messages processed
	statControlRestoreDuration = stats.NewLatencyHistogram15s32("idx.cassandra.control.add")
	// metric idx.cassandra.control.delete is the duration of delete control messages processed
	statControlDeleteDuration = stats.NewLatencyHistogram15s32("idx.cassandra.control.delete")

	errmetrics = cassandra.NewErrMetrics("idx.cassandra")
)

type writeReq struct {
	def      *schema.MetricDefinition
	recvTime time.Time
}

// CasIdx implements the the "MetricIndex" interface
type CasIdx struct {
	memory.MemoryIndex
	Config           *IdxConfig
	cluster          *gocql.ClusterConfig
	Session          *cassandra.Session
	writeQueue       chan writeReq
	shutdown         chan struct{}
	wg               sync.WaitGroup
	updateInterval32 uint32
}

type cqlIterator interface {
	Scan(dest ...interface{}) bool
	Close() error
}

func New(cfg *IdxConfig) *CasIdx {
	if err := cfg.Validate(); err != nil {
		log.Fatalf("cassandra-idx: %s", err)
	}
	cluster := gocql.NewCluster(strings.Split(cfg.Hosts, ",")...)
	cluster.Consistency = gocql.ParseConsistency(cfg.Consistency)
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cluster.Timeout
	cluster.NumConns = cfg.NumConns
	cluster.ProtoVersion = cfg.ProtoVer
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup
	if cfg.SSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.CaPath,
			EnableHostVerification: cfg.HostVerification,
		}
	}
	if cfg.Auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	idx := &CasIdx{
		MemoryIndex:      memory.New(),
		Config:           cfg,
		cluster:          cluster,
		updateInterval32: uint32(cfg.updateInterval.Nanoseconds() / int64(time.Second)),
		shutdown:         make(chan struct{}),
	}
	if cfg.updateCassIdx {
		idx.writeQueue = make(chan writeReq, cfg.writeQueueSize)
	}

	return idx
}

// InitBare makes sure the keyspace, tables, and index exists in cassandra and creates a session
func (c *CasIdx) InitBare() error {
	var err error
	tmpSession, err := c.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("cassandra-idx: failed to create cassandra session: %s", err)
	}

	// read templates
	schemaKeyspace := util.ReadEntry(c.Config.SchemaFile, "schema_keyspace").(string)

	// create the keyspace or ensure it exists
	if c.Config.CreateKeyspace {
		log.Infof("cassandra-idx: ensuring that keyspace %s exists.", c.Config.Keyspace)
		err = tmpSession.Query(fmt.Sprintf(schemaKeyspace, c.Config.Keyspace)).Exec()
		if err != nil {
			return fmt.Errorf("cassandra-idx: failed to initialize cassandra keyspace: %s", err)
		}
	}

	schema := fmt.Sprintf(util.ReadEntry(c.Config.SchemaFile, "schema_table").(string), c.Config.Keyspace, c.Config.Table)
	err = cassUtils.EnsureTableExists(tmpSession, c.Config.CreateKeyspace, c.Config.Keyspace, schema, c.Config.Table)
	if err != nil {
		return err
	}

	schema = fmt.Sprintf(util.ReadEntry(c.Config.SchemaFile, "schema_archive_table").(string), c.Config.Keyspace, c.Config.ArchiveTable)
	err = cassUtils.EnsureTableExists(tmpSession, c.Config.CreateKeyspace, c.Config.Keyspace, schema, c.Config.ArchiveTable)
	if err != nil {
		return err
	}

	tmpSession.Close()
	c.cluster.Keyspace = c.Config.Keyspace

	c.Session, err = cassandra.NewSession(c.cluster, c.Config.ConnectionCheckTimeout, c.Config.ConnectionCheckInterval, c.Config.Hosts, "cassandra-idx")

	if err != nil {
		return fmt.Errorf("cassandra-idx: failed to create cassandra session: %s", err)
	}

	return nil
}

// Init makes sure the needed keyspace, table, index in cassandra exists, creates the session,
// rebuilds the in-memory index, sets up write queues, metrics and pruning routines
func (c *CasIdx) Init() error {
	log.Infof("cassandra-idx: initializing cassandra-idx. Hosts=%s", c.Config.Hosts)
	if err := c.MemoryIndex.Init(); err != nil {
		return err
	}

	if err := c.InitBare(); err != nil {
		return err
	}

	if c.Config.updateCassIdx {
		c.wg.Add(c.Config.NumConns)
		for i := 0; i < c.Config.NumConns; i++ {
			go c.processWriteQueue()
		}
		log.Infof("cassandra-idx: started %d writeQueue handlers", c.Config.NumConns)
	}

	//Rebuild the in-memory index.
	c.rebuildIndex()

	if memory.IndexRules.Prunable() {
		c.wg.Add(1)
		go c.prune()
	}

	return nil
}

func (c *CasIdx) Stop() {
	log.Info("cassandra-idx: stopping")
	c.MemoryIndex.Stop()

	close(c.shutdown)
	// if updateCassIdx is disabled then writeQueue should never have been initialized
	if c.Config.updateCassIdx {
		close(c.writeQueue)
	}

	c.wg.Wait()
	c.Session.Stop()
}

// Update updates an existing archive, if found.
// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
func (c *CasIdx) Update(point schema.MetricPoint, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	archive, oldPartition, inMemory := c.MemoryIndex.Update(point, partition)

	if !c.Config.updateCassIdx {
		statUpdateDuration.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// Cassandra uses partition id as the partitioning key, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and won't remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			c.deleteDefAsync(point.MKey, oldPartition)
		}

		// check if we need to save to cassandra.
		now := uint32(time.Now().Unix())
		if archive.LastSave < (now - c.updateInterval32) {
			archive = c.updateCassandra(now, inMemory, archive, partition)
		}
	}

	statUpdateDuration.Value(time.Since(pre))
	return archive, oldPartition, inMemory
}

func (c *CasIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	archive, oldPartition, inMemory := c.MemoryIndex.AddOrUpdate(mkey, data, partition)

	stat := statUpdateDuration
	if !inMemory {
		stat = statAddDuration
	}

	if !c.Config.updateCassIdx {
		stat.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// Cassandra uses partition id as the partitioning key, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and won't remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			c.deleteDefAsync(mkey, oldPartition)
		}
	}

	// check if we need to save to cassandra.
	now := uint32(time.Now().Unix())
	if archive.LastSave < (now - c.updateInterval32) {
		archive = c.updateCassandra(now, inMemory, archive, partition)
	}

	stat.Value(time.Since(pre))
	return archive, oldPartition, inMemory
}

// updateCassandra saves the archive to cassandra and
// updates the memory index with the updated fields.
func (c *CasIdx) updateCassandra(now uint32, inMemory bool, archive idx.Archive, partition int32) idx.Archive {
	// if the entry has not been saved for 1.5x updateInterval
	// then perform a blocking save.
	if archive.LastSave < (now - c.updateInterval32 - c.updateInterval32/2) {
		log.Debugf("cassandra-idx: updating def %s in index.", archive.MetricDefinition.Id)
		c.writeQueue <- writeReq{recvTime: time.Now(), def: &archive.MetricDefinition}
		archive.LastSave = now
		c.MemoryIndex.UpdateArchiveLastSave(archive.Id, archive.Partition, now)
	} else {
		// perform a non-blocking write to the writeQueue. If the queue is full, then
		// this will fail and we won't update the LastSave timestamp. The next time
		// the metric is seen, the previous lastSave timestamp will still be in place and so
		// we will try and save again.  This will continue until we are successful or the
		// lastSave timestamp become more then 1.5 x UpdateInterval, in which case we will
		// do a blocking write to the queue.
		select {
		case c.writeQueue <- writeReq{recvTime: time.Now(), def: &archive.MetricDefinition}:
			archive.LastSave = now
			c.MemoryIndex.UpdateArchiveLastSave(archive.Id, archive.Partition, now)
		default:
			statSaveSkipped.Inc()
			log.Debugf("cassandra-idx: writeQueue is full, update of %s not saved this time.", archive.MetricDefinition.Id)
		}
	}

	return archive
}

func (c *CasIdx) Find(orgId uint32, pattern string, from, limit int64) ([]idx.Node, error) {
	return c.MemoryIndex.Find(orgId, pattern, from, limit)
}

func (c *CasIdx) rebuildIndex() {
	log.Info("cassandra-idx: Rebuilding Memory Index from metricDefinitions in Cassandra")
	pre := time.Now()
	gate := make(chan struct{}, c.Config.InitLoadConcurrency)
	var wg sync.WaitGroup
	defPool := sync.Pool{
		New: func() interface{} {
			return []schema.MetricDefinition{}
		},
	}
	var num uint32
	for _, partition := range cluster.Manager.GetPartitions() {
		wg.Add(1)
		go func(p int32) {
			gate <- struct{}{}
			defs := defPool.Get().([]schema.MetricDefinition)
			defer func() {
				defPool.Put(defs[:0])
				wg.Done()
				<-gate
			}()
			defs = c.LoadPartitions([]int32{p}, defs, pre)
			atomic.AddUint32(&num, uint32(c.MemoryIndex.LoadPartition(p, defs)))
		}(partition)
	}

	wg.Wait()

	log.Infof("cassandra-idx: Rebuilding Memory Index Complete. Imported %d. Took %s", num, time.Since(pre))
}

func (c *CasIdx) Load(defs []schema.MetricDefinition, now time.Time) []schema.MetricDefinition {
	session := c.Session.CurrentSession()
	iter := session.Query(fmt.Sprintf("SELECT id, orgid, partition, name, interval, unit, mtype, tags, lastupdate from %s", c.Config.Table)).Iter()
	return c.load(defs, iter, now)
}

// LoadPartitions appends MetricDefinitions from the given partitions to defs and returns the modified defs, honoring pruning settings relative to now
func (c *CasIdx) LoadPartitions(partitions []int32, defs []schema.MetricDefinition, now time.Time) []schema.MetricDefinition {
	placeholders := make([]string, len(partitions))
	for i, p := range partitions {
		placeholders[i] = strconv.Itoa(int(p))
	}
	q := fmt.Sprintf("SELECT id, orgid, partition, name, interval, unit, mtype, tags, lastupdate from %s where partition in (%s)", c.Config.Table, strings.Join(placeholders, ","))

	session := c.Session.CurrentSession()
	iter := session.Query(q).Iter()
	return c.load(defs, iter, now)
}

// load appends MetricDefinitions from the iterator to defs and returns the modified defs, honoring pruning settings relative to now
func (c *CasIdx) load(defs []schema.MetricDefinition, iter cqlIterator, now time.Time) []schema.MetricDefinition {
	defsByNames := make(map[string][]*schema.MetricDefinition)
	var id, name, unit, mtype string
	var orgId, interval int
	var partition int32
	var lastupdate int64
	var tags []string

	maxLastUpdate := time.Now().Unix()
	updateInterval := int64(c.updateInterval32)

	for iter.Scan(&id, &orgId, &partition, &name, &interval, &unit, &mtype, &tags, &lastupdate) {
		mkey, err := schema.MKeyFromString(id)
		if err != nil {
			log.Errorf("cassandra-idx: load() could not parse ID %q: %s -> skipping", id, err)
			continue
		}
		if orgId < 0 {
			orgId = int(idx.OrgIdPublic)
		}

		mdef := &schema.MetricDefinition{
			Id:        mkey,
			OrgId:     uint32(orgId),
			Partition: partition,
			Name:      name,
			Interval:  interval,
			Unit:      unit,
			Mtype:     mtype,
			Tags:      tags,
			// because metricdefs get saved no more frequently than every updateInterval
			// the lastUpdate field may be out of date by that amount (or more if the process
			// struggled writing data. See updateCassandra() )
			// To compensate, we bump it here.  This should make sure to include all series
			// that have data for queries but didn't see an update to the index, at the cost
			// of potentially including some series in queries that don't have data, but that's OK
			// (that's how Graphite works anyway)
			LastUpdate: util.MinInt64(maxLastUpdate, lastupdate+updateInterval),
		}
		nameWithTags := mdef.NameWithTags()
		defsByNames[nameWithTags] = append(defsByNames[nameWithTags], mdef)
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("cassandra-idx: could not close iterator: %s", err.Error())
	}

	// getting all cutoffs once saves having to recompute everytime we have a match
	cutoffs := memory.IndexRules.Cutoffs(now)

NAMES:
	for nameWithTags, defsByName := range defsByNames {
		irId, _ := memory.IndexRules.Match(nameWithTags)
		cutoff := cutoffs[irId]
		for _, def := range defsByName {
			if def.LastUpdate >= cutoff {
				// if any of the defs for a given nameWithTags is not stale, then we need to load
				// all the defs for that nameWithTags.
				for _, defToAdd := range defsByNames[nameWithTags] {
					defs = append(defs, *defToAdd)
				}
				continue NAMES
			}
		}
	}

	return defs
}

// ArchiveDefs writes each of the provided defs to the archive table and
// then deletes the defs from the metric index table.
func (c *CasIdx) ArchiveDefs(defs []schema.MetricDefinition) (int, error) {
	defChan := make(chan *schema.MetricDefinition, c.Config.NumConns)
	g, ctx := errgroup.WithContext(context.Background())

	// keep track of how many defs were successfully archived.
	success := make([]int, c.Config.NumConns)

	for i := 0; i < c.Config.NumConns; i++ {
		i := i
		g.Go(func() error {
			for {
				select {
				case def, ok := <-defChan:
					if !ok {
						return nil
					}
					err := c.addDefToArchive(*def)
					if err != nil {
						// If we failed to add the def to the archive table then just continue on to the next def.
						// As we haven't yet removed  this def from the metric index table yet, the next time archiving
						// is performed this def will be processed again. As no action is needed by an operator, we
						// just log this as a warning.
						log.Warnf("cassandra-idx: Failed add def to archive table. error=%s. def=%+v", err, *def)
						continue
					}

					err = c.deleteDef(def.Id, def.Partition)
					if err != nil {
						// The next time archiving is performed this def will be processed again. Re-adding the def to the archive
						// table will just be treated like an update with only the archived_at field changing. As no action is needed
						// by an operator, we just log this as a warning.
						log.Warnf("cassandra-idx: Failed to remove archived def from %s table. error=%s. def=%+v", c.Config.Table, err, *def)
						continue
					}

					// increment counter of defs successfully archived
					success[i] = success[i] + 1
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}
	for i := range defs {
		defChan <- &defs[i]
	}
	close(defChan)

	// wait for all goroutines to complete.
	err := g.Wait()

	// get the count of defs successfully archived.
	total := 0
	for _, count := range success {
		total = total + count
	}

	return total, err
}

func (c *CasIdx) processWriteQueue() {
	defer c.wg.Done()

	var success bool
	var attempts int
	var err error
	var req writeReq
	qry := fmt.Sprintf("INSERT INTO %s (id, orgid, partition, name, interval, unit, mtype, tags, lastupdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", c.Config.Table)
	for {
		select {
		case req = <-c.writeQueue:
			if err != nil {
				log.Errorf("cassandra-idx: failed to marshal metricDef: %s. value was: %+v", err, *req.def)
				continue
			}
			statQueryInsertWaitDuration.Value(time.Since(req.recvTime))
			pre := time.Now()
			success = false
			attempts = 0

			for !success {
				select {
				case <-c.shutdown:
					log.Info("cassandra-idx: received shutdown, exiting processWriteQueue")
					return
				default:
					session := c.Session.CurrentSession()
					if err := session.Query(
						qry,
						req.def.Id.String(),
						req.def.OrgId,
						req.def.Partition,
						req.def.Name,
						req.def.Interval,
						req.def.Unit,
						req.def.Mtype,
						req.def.Tags,
						req.def.LastUpdate).Exec(); err != nil {

						statQueryInsertFail.Inc()
						errmetrics.Inc(err)
						if (attempts % 20) == 0 {
							log.Warnf("cassandra-idx: Failed to write def to cassandra. it will be retried. %s. the value was: %+v", err, *req.def)
						}
						sleepTime := 100 * attempts
						if sleepTime > 2000 {
							sleepTime = 2000
						}
						time.Sleep(time.Duration(sleepTime) * time.Millisecond)
						attempts++
					} else {
						success = true
						statQueryInsertExecDuration.Value(time.Since(pre))
						statQueryInsertOk.Inc()
						log.Debugf("cassandra-idx: metricDef %s saved to cassandra", req.def.Id)
					}
				}
			}
		case <-c.shutdown:
			log.Info("cassandra-idx: received shutdown, exiting processWriteQueue")
			return
		}
	}
}

func (c *CasIdx) addDefToArchive(def schema.MetricDefinition) error {
	insertQry := fmt.Sprintf("INSERT INTO %s (id, orgid, partition, name, interval, unit, mtype, tags, lastupdate, archived_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", c.Config.ArchiveTable)
	maxAttempts := 5
	now := time.Now().UTC().Unix()
	var err error

	for attempts := 0; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			sleepTime := 100 * attempts
			if sleepTime > 2000 {
				sleepTime = 2000
			}
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		}

		session := c.Session.CurrentSession()
		err := session.Query(
			insertQry,
			def.Id.String(),
			def.OrgId,
			def.Partition,
			def.Name,
			def.Interval,
			def.Unit,
			def.Mtype,
			def.Tags,
			def.LastUpdate,
			now).Exec()

		if err == nil {
			return nil
		}

		// log first failure as a warning.  If we reach max attempts, the error will bubble up to the caller.
		if attempts == 0 {
			log.Warnf("cassandra-idx: Failed to write def to cassandra. it will be retried. error=%s. def=%+v", err, def)
		}
	}

	return err
}

func (c *CasIdx) Delete(orgId uint32, pattern string) ([]idx.Archive, error) {
	pre := time.Now()
	defs, err := c.MemoryIndex.Delete(orgId, pattern)
	if err != nil {
		return defs, err
	}

	err = c.deleteDefs(defs)
	if err != nil {
		return nil, err
	}

	statDeleteDuration.Value(time.Since(pre))
	return defs, err
}

func (c *CasIdx) DeleteTagged(orgId uint32, query tagquery.Query) ([]idx.Archive, error) {
	pre := time.Now()
	defs, err := c.MemoryIndex.DeleteTagged(orgId, query)
	if err != nil {
		return nil, err
	}

	err = c.deleteDefs(defs)
	if err != nil {
		return nil, err
	}

	statDeleteDuration.Value(time.Since(pre))
	return defs, err
}

func (c *CasIdx) ArchiveTagged(orgId uint32, query tagquery.Query) ([]idx.Archive, error) {

	archives, err := c.MemoryIndex.DeleteTagged(orgId, query)
	if err != nil {
		return nil, err
	}

	// Due to an unfortunate evolution, Archive and delete have 2 different
	// interfaces (idx.Archive vs schema.MetricDefinition). So one more
	// translation step to what ArchiveDefs needs.
	defs := make([]schema.MetricDefinition, 0, len(archives))
	for _, archive := range archives {
		defs = append(defs, archive.MetricDefinition)
	}

	_, err = c.ArchiveDefs(defs)
	if err != nil {
		return nil, err
	}

	return archives, err
}

func (c *CasIdx) deleteDefs(defs []idx.Archive) error {
	var err error

	if c.Config.updateCassIdx {
		for _, def := range defs {
			delErr := c.deleteDef(def.Id, def.Partition)
			if delErr != nil {
				log.Errorf("cassandra-idx: %s", delErr.Error())
				err = delErr
			}
		}
	}

	return err
}

func (c *CasIdx) deleteDef(key schema.MKey, part int32) error {
	pre := time.Now()
	attempts := 0
	keyStr := key.String()
	for attempts < 5 {
		attempts++
		session := c.Session.CurrentSession()
		err := session.Query(fmt.Sprintf("DELETE FROM %s where partition=? AND id=?", c.Config.Table), part, keyStr).Exec()
		if err != nil {
			statQueryDeleteFail.Inc()
			errmetrics.Inc(err)
			log.Warnf("cassandra-idx: Failed to delete metricDef %s from cassandra: %s", keyStr, err)
			time.Sleep(time.Second)
		} else {
			statQueryDeleteOk.Inc()
			statQueryDeleteExecDuration.Value(time.Since(pre))
			return nil
		}
	}
	return fmt.Errorf("cassandra-idx: unable to delete metricDef %s from index after %d attempts", keyStr, attempts)
}

func (c *CasIdx) deleteDefAsync(key schema.MKey, part int32) {
	go func() {
		if err := c.deleteDef(key, part); err != nil {
			log.Errorf("cassandra-idx: %s", err.Error())
		}
	}()
}

func (c *CasIdx) Prune(now time.Time) ([]idx.Archive, error) {
	log.Info("cassandra-idx: start pruning of series")
	pruned, err := c.MemoryIndex.Prune(now)
	duration := time.Since(now)
	if err != nil {
		log.Errorf("cassandra-idx: pruning error: %s", err)
	} else {
		statPruneDuration.Value(duration)
		log.Infof("cassandra-idx: finished pruning of %d series in %s", len(pruned), duration)
	}
	return pruned, err
}

func (c *CasIdx) prune() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.Config.pruneInterval)
	for {
		select {
		case now := <-ticker.C:
			c.Prune(now)
		case <-c.shutdown:
			return
		}
	}
}

// AddDefs add defs to the index.
func (c *CasIdx) AddDefs(defs []schema.MetricDefinition) {
	pre := time.Now()

	c.MemoryIndex.AddDefs(defs)

	if c.Config.updateCassIdx {
		// Blocking write to make sure all get enqueued
		for _, def := range defs {
			c.writeQueue <- writeReq{recvTime: time.Now(), def: &def}
		}
	}

	statControlRestoreDuration.Value(time.Since(pre))
}

// DeleteDefs delete the matching defs, conditionally writing an archive record.
func (c *CasIdx) DeleteDefs(defs []schema.MetricDefinition, archive bool) {
	pre := time.Now()

	c.MemoryIndex.DeleteDefs(defs, archive)

	if c.Config.updateCassIdx {
		// TODO - this could create a lot of go routines if many deletes come in at the same time
		// Maybe better to enhance the write queue to process these?
		if archive {
			c.ArchiveDefs(defs)
		} else {
			go func() {
				for _, def := range defs {
					c.deleteDef(def.Id, def.Partition)
				}
			}()
		}
	}

	statControlDeleteDuration.Value(time.Since(pre))
}
