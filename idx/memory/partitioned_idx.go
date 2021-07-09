package memory

import (
	"context"
	"regexp"
	"sort"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Implements the the "MetricIndex" interface
type PartitionedMemoryIdx struct {
	Partition map[int32]*UnpartitionedMemoryIdx
}

func NewPartitionedMemoryIdx() *PartitionedMemoryIdx {
	idx := &PartitionedMemoryIdx{
		Partition: make(map[int32]*UnpartitionedMemoryIdx),
	}
	partitions := cluster.Manager.GetPartitions()
	log.Infof("PartitionedMemoryIdx: initializing with partitions: %v", partitions)
	for _, p := range partitions {
		idx.Partition[p] = NewUnpartitionedMemoryIdx()
	}
	return idx
}

// Init initializes the index at startup and
// blocks until the index is ready for use.
func (p *PartitionedMemoryIdx) Init() error {
	for _, m := range p.Partition {
		err := m.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

// Stop shuts down the index.
func (p *PartitionedMemoryIdx) Stop() {
	for _, m := range p.Partition {
		m.Stop()
	}
}

// Update updates an existing archive, if found.
// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
func (p *PartitionedMemoryIdx) Update(point schema.MetricPoint, partition int32) (idx.Archive, int32, bool) {
	return p.Partition[partition].Update(point, partition)
}

// AddOrUpdate makes sure a metric is known in the index,
// and should be called for every received metric.
func (p *PartitionedMemoryIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (idx.Archive, int32, bool) {
	return p.Partition[partition].AddOrUpdate(mkey, data, partition)
}

// UpdateArchive updates the archive information
func (p *PartitionedMemoryIdx) UpdateArchiveLastSave(id schema.MKey, partition int32, lastSave uint32) {
	p.Partition[partition].UpdateArchiveLastSave(id, partition, lastSave)
}

// Get returns the archive for the requested id.
func (p *PartitionedMemoryIdx) Get(key schema.MKey) (idx.Archive, bool) {
	g, _ := errgroup.WithContext(context.Background())
	resultChan := make(chan *idx.Archive, len(p.Partition))
	for _, m := range p.Partition {
		m := m
		g.Go(func() error {
			if a, ok := m.Get(key); ok {
				resultChan <- &a
			}
			return nil
		})
	}

	var err atomic.Value
	go func() {
		if e := g.Wait(); e != nil {
			err.Store(e)
		}
		close(resultChan)
	}()

	result, ok := <-resultChan
	if !ok {
		e := err.Load()
		if e != nil {
			log.Errorf("memory-idx: failed to get Archive with key %v. %s", key, e)
		}
		return idx.Archive{}, false
	}

	return *result, true
}

// GetPath returns the archives under the given path.
func (p *PartitionedMemoryIdx) GetPath(orgId uint32, path string) []idx.Archive {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.GetPath(orgId, path)
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to getPath: orgId=%d path=%s. %s", orgId, path, err)
		return nil
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response
}

// Delete deletes items from the index
// If the pattern matches a branch node, then
// all leaf nodes on that branch are deleted. So if the pattern is
// "*", all items in the index are deleted.
// It returns a copy of all of the Archives deleted.
func (p *PartitionedMemoryIdx) Delete(orgId uint32, pattern string) ([]idx.Archive, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			deleted, err := m.Delete(orgId, pattern)
			if err != nil {
				return err
			}
			result[pos] = deleted
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to Delete: orgId=%d pattern=%s. %s", orgId, pattern, err)
		return nil, err
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response, nil
}

// Find searches the index for matching nodes.
// * orgId describes the org to search in (public data in orgIdPublic is automatically included)
// * pattern is handled like graphite does. see https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
func (p *PartitionedMemoryIdx) Find(orgId uint32, pattern string, from, limit int64) ([]idx.Node, error) {
	g, _ := errgroup.WithContext(context.Background())
	resultChan := make(chan []idx.Node)
	for _, m := range p.Partition {
		m := m
		g.Go(func() error {
			found, err := m.Find(orgId, pattern, from, limit/int64(len(p.Partition)))
			if err != nil {
				return err
			}
			resultChan <- found
			return nil
		})
	}

	// consume from the ResultChan and merge the results, de-duping branches
	byPath := make(map[string]*idx.Node)
	done := make(chan struct{})
	go func() {
		for found := range resultChan {
			for i, node := range found {
				n, ok := byPath[node.Path]
				if !ok {
					byPath[node.Path] = &found[i]
				} else {
					if node.HasChildren {
						n.HasChildren = true
					}
					if node.Leaf {
						n.Defs = append(n.Defs, node.Defs...)
						n.Leaf = true
					}
				}
			}
		}
		close(done)
	}()
	if err := g.Wait(); err != nil {
		close(resultChan)
		log.Errorf("memory-idx: failed to Find: orgId=%d pattern=%s from=%d. %s", orgId, pattern, from, err)
		return nil, err
	}
	close(resultChan)
	<-done
	response := make([]idx.Node, 0, len(byPath))
	for _, node := range byPath {
		response = append(response, *node)
	}
	return response, nil
}

// List returns all Archives for the passed OrgId and the public orgId
func (p *PartitionedMemoryIdx) List(orgId uint32) []idx.Archive {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found := m.List(orgId)
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to List: orgId=%d. %s", orgId, err)
		return nil
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response
}

// Prune deletes all metrics that haven't been seen since the given timestamp.
// It returns all Archives deleted and any error encountered.
func (p *PartitionedMemoryIdx) Prune(oldest time.Time) ([]idx.Archive, error) {
	// Prune each partition sequentially.
	result := []idx.Archive{}
	for _, m := range p.Partition {
		found, err := m.Prune(oldest)
		if err != nil {
			return result, err
		}
		result = append(result, found...)
	}
	return result, nil
}

// FindByTag takes a list of expressions in the format key<operator>value.
// The allowed operators are: =, !=, =~, !=~.
// It returns a slice of Node structs that match the given conditions, the
// conditions are logically AND-ed.
// If the third argument is > 0 then the results will be filtered and only those
// where the LastUpdate time is >= from will be returned as results.
// The returned results are not deduplicated and in certain cases it is possible
// that duplicate entries will be returned.
func (p *PartitionedMemoryIdx) FindByTag(orgId uint32, query tagquery.Query) []idx.Node {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Node, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.FindByTag(orgId, query)
			return nil
		})
		i++
	}
	g.Wait()

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Node, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response
}

func (p *PartitionedMemoryIdx) FindTerms(orgID uint32, tags []string, query tagquery.Query) (uint32, map[string]map[string]uint32) {
	g, _ := errgroup.WithContext(context.Background())
	var total uint32
	results := make([]map[string]map[string]uint32, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			numSeries, terms := m.FindTerms(orgID, tags, query)
			atomic.AddUint32(&total, numSeries)
			results[pos] = terms
			return nil
		})
		i++
	}
	g.Wait()

	response := make(map[string]map[string]uint32)
	for _, terms := range results {
		for tag, term := range terms {
			if _, ok := response[tag]; !ok {
				response[tag] = make(map[string]uint32)
			}
			for val, count := range term {
				response[tag][val] += count
			}
		}
	}

	return total, response
}

// Tags returns a list of all tag keys associated with the metrics of a given
// organization. The return values are filtered by the regex in the second parameter.
func (p *PartitionedMemoryIdx) Tags(orgId uint32, filter *regexp.Regexp) []string {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.Tags(orgId, filter)
			return nil
		})
		i++
	}
	g.Wait()

	return mergePartitionStringResults(result)
}

// TagDetails returns a list of all values associated with a given tag key in the
// given org. The occurrences of each value is counted and the count is referred to by
// the metric names in the returned map.
// If the third parameter is not "" it will be used as a regular expression to filter
// the values before accounting for them.
func (p *PartitionedMemoryIdx) TagDetails(orgId uint32, key string, filter *regexp.Regexp) map[string]uint64 {
	g, _ := errgroup.WithContext(context.Background())
	result := make([]map[string]uint64, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.TagDetails(orgId, key, filter)
			return nil
		})
		i++
	}
	g.Wait()

	// merge our results into the unique set of tags
	merged := map[string]uint64{}
	for _, tagCounts := range result {
		for tag, count := range tagCounts {
			merged[tag] = merged[tag] + count
		}
	}

	return merged
}

// FindTags returns tags matching the specified conditions
// prefix:      prefix match
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (p *PartitionedMemoryIdx) FindTags(orgId uint32, prefix string, limit uint) []string {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.FindTags(orgId, prefix, limit)
			return nil
		})
		i++
	}
	g.Wait()

	response := mergePartitionStringResults(result)
	if uint(len(response)) > limit {
		return response[:limit]
	}
	return response
}

// FindTagsWithQuery returns tags matching the specified conditions
// query:       tagdb query to run on the index
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (p *PartitionedMemoryIdx) FindTagsWithQuery(orgId uint32, prefix string, query tagquery.Query, limit uint) []string {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.FindTagsWithQuery(orgId, prefix, query, limit)
			return nil
		})
		i++
	}
	g.Wait()

	response := mergePartitionStringResults(result)
	if uint(len(response)) > limit {
		return response[:limit]
	}
	return response
}

// FindTagValues generates a list of possible values that could
// complete a given value prefix. It requires a tag to be specified and only values
// of the given tag will be returned. It also accepts additional conditions to
// further narrow down the result set in the format of graphite's tag queries
func (p *PartitionedMemoryIdx) FindTagValues(orgId uint32, tag, prefix string, limit uint) []string {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.FindTagValues(orgId, tag, prefix, limit)
			return nil
		})
		i++
	}
	g.Wait()

	response := mergePartitionStringResults(result)
	if uint(len(response)) > limit {
		return response[:limit]
	}
	return response
}

func (p *PartitionedMemoryIdx) FindTagValuesWithQuery(orgId uint32, tag, prefix string, query tagquery.Query, limit uint) []string {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.FindTagValuesWithQuery(orgId, tag, prefix, query, limit)
			return nil
		})
		i++
	}
	g.Wait()

	response := mergePartitionStringResults(result)
	if uint(len(response)) > limit {
		return response[:limit]
	}
	return response
}

// DeleteTagged deletes the specified series from the tag index and also the
// DefById index.
func (p *PartitionedMemoryIdx) DeleteTagged(orgId uint32, query tagquery.Query) ([]idx.Archive, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			var err error
			result[pos], err = m.DeleteTagged(orgId, query)
			if err != nil {
				return err
			}
			return nil
		})
		i++
	}
	err := g.Wait()
	if err != nil {
		return nil, err
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}

	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}

	return response, nil
}

// Used to rebuild the index from an existing set of metricDefinitions.
func (p *PartitionedMemoryIdx) LoadPartition(partition int32, defs []schema.MetricDefinition) int {
	return p.Partition[partition].Load(defs)
}

func (p *PartitionedMemoryIdx) add(archive *idx.Archive) {
	p.Partition[archive.Partition].add(archive)
}

func (p *PartitionedMemoryIdx) MetaTagRecordList(orgId uint32) []tagquery.MetaTagRecord {
	for _, m := range p.Partition {
		// all partitions should have all meta records
		return m.MetaTagRecordList(orgId)
	}
	return nil
}

func (p *PartitionedMemoryIdx) MetaTagRecordUpsert(orgId uint32, rawRecord tagquery.MetaTagRecord) error {
	g, _ := errgroup.WithContext(context.Background())

	for _, m := range p.Partition {
		m := m
		g.Go(func() error {
			return m.MetaTagRecordUpsert(orgId, rawRecord)
		})
	}

	err := g.Wait()
	if err != nil {
		log.Errorf("memory-idx: failed to upsert meta tag record in at least one partition: %s", err)
	}

	return err
}

func (p *PartitionedMemoryIdx) MetaTagRecordSwap(orgId uint32, records []tagquery.MetaTagRecord) error {
	g, _ := errgroup.WithContext(context.Background())

	for _, m := range p.Partition {
		m := m
		g.Go(func() error {
			return m.MetaTagRecordSwap(orgId, records)
		})
	}

	err := g.Wait()
	if err != nil {
		log.Errorf("memory-idx: failed to swap meta tag records in at least one partition: %s", err)
	}

	return err
}

func mergePartitionStringResults(partitionResults [][]string) []string {
	// merge our results into the unique set of strings
	merged := map[string]struct{}{}
	for _, tags := range partitionResults {
		for _, t := range tags {
			merged[t] = struct{}{}
		}
	}

	// convert the map into a slice
	response := make([]string, len(merged))
	i := 0
	for tag := range merged {
		response[i] = tag
		i++
	}

	sort.Strings(response)

	return response
}

// AddDef adds def to the index.
func (p *PartitionedMemoryIdx) AddDefs(defs []schema.MetricDefinition) {
	// TODO - make batches by partition
	for _, def := range defs {
		p.Partition[def.Partition].AddDefs([]schema.MetricDefinition{def})
	}
}

// DeleteDefs deletes the matching defs.
func (p *PartitionedMemoryIdx) DeleteDefs(defs []schema.MetricDefinition, archive bool) {
	// TODO - make batches by partition
	for _, def := range defs {
		p.Partition[def.Partition].DeleteDefs([]schema.MetricDefinition{def}, archive)
	}
}
