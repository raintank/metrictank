# Overview of metrics
(only shows metrics that are documented. generated with [metrics2docs](github.com/Dieterbe/metrics2docs))

* `api.cluster.speculative.attempts`:  
how many peer queries resulted in speculation
* `api.cluster.speculative.requests`:  
how many speculative http requests made to peers
* `api.cluster.speculative.wins`:  
how many peer queries were improved due to speculation
* `api.get_target`:  
how long it takes to get a target
* `api.iters_to_points`:  
how long it takes to decode points from a chunk iterator
* `api.request.%s`:  
the latency of each request by request path.
* `api.request.%s.size`:  
the size of each response by request path
* `api.request.%s.status.%d`:  
the count of the number of responses for each request path, status code combination.
eg. `api.requests.metrics_find.status.200` and `api.request.render.status.503`
* `api.request.render.chosen_archive`:  
the archive chosen for the request.
0 means original data, 1 means first agg level, 2 means 2nd
* `api.request.render.points_fetched`:  
the number of points that need to be fetched for a /render request.
* `api.request.render.points_returned`:  
the number of points the request will return
best effort: not aware of summarize(), aggregation functions, runtime normalization. but does account for runtime consolidation
* `api.request.render.series`:  
the number of series a /render request is handling.  This is the number
of metrics after all of the targets in the request have expanded by searching the index.
* `api.request.render.targets`:  
the number of targets a /render request is handling.
* `api.requests_span.mem`:  
the timerange of requests hitting only the ringbuffer
* `api.requests_span.mem_and_cassandra`:  
the timerange of requests hitting both in-memory and cassandra
* `cache.ops.chunk.add`:  
how many chunks were added to the cache
* `cache.ops.chunk.evict`:  
how many chunks were evicted from the cache
* `cache.ops.chunk.hit`:  
how many chunks were hit
* `cache.ops.chunk.push-hot`:  
how many chunks have been pushed into the cache because their metric is hot
* `cache.ops.metric.add`:  
how many metrics were added to the cache
* `cache.ops.metric.evict`:  
how many metrics were evicted from the cache
* `cache.ops.metric.hit-full`:  
how many metrics were hit fully (all needed chunks in cache)
* `cache.ops.metric.hit-partial`:  
how many metrics were hit partially (some of the needed chunks in cache, but not all)
* `cache.ops.metric.miss`:  
how many metrics were missed fully (no needed chunks in cache)
* `cache.overhead.chunk`:  
an approximation of the overhead used to store chunks in the cache
* `cache.overhead.flat`:  
an approximation of the overhead used by flat accounting
* `cache.overhead.lru`:  
an approximation of the overhead used by the LRU
* `cache.size.max`:  
the maximum size of the cache (overhead does not count towards this limit)
* `cache.size.used`:  
how much of the cache is used (sum of the chunk data without overhead)
* `cluster.decode_err.join`:  
a counter of json unmarshal errors
* `cluster.decode_err.update`:  
a counter of json unmarshal errors
* `cluster.events.join`:  
how many node join events were received
* `cluster.events.leave`:  
how many node leave events were received
* `cluster.events.update`:  
how many node update events were received
* `cluster.notifier.all.messages-received`:  
a counter of messages received from cluster notifiers
* `cluster.notifier.kafka.message_size`:  
the sizes seen of messages through the kafka cluster notifier
* `cluster.notifier.kafka.messages-published`:  
a counter of messages published to the kafka cluster notifier
* `cluster.notifier.kafka.partition.%d.lag`:  
how many messages (mechunkWriteRequestsrics) there are in the kafka
partition (%d) that we have not yet consumed.
* `cluster.notifier.kafka.partition.%d.log_size`:  
the size of the kafka partition (%d), aka the newest available offset.
* `cluster.notifier.kafka.partition.%d.offset`:  
the current offset for the partition (%d) that we have consumed
* `cluster.self.partitions`:  
the number of partitions this instance consumes
* `cluster.self.priority`:  
the priority of the node. A lower number gives higher priority
* `cluster.self.promotion_wait`:  
how long a candidate (secondary node) has to wait until it can become a primary
When the timer becomes 0 it means the in-memory buffer has been able to fully populate so that if you stop a primary
and it was able to save its complete chunks, this node will be able to take over without dataloss.
You can upgrade a candidate to primary while the timer is not 0 yet, it just means it may have missing data in the chunks that it will save.
* `cluster.self.state.primary`:  
whether this instance is a primary
* `cluster.self.state.ready`:  
whether this instance is ready
* `cluster.total.partitions`:  
the number of partitions in the cluster that we know of
* `cluster.total.state.primary-not-ready`:  
the number of nodes we know to be primary but not ready (total should only be in this state very temporarily)
* `cluster.total.state.primary-ready`:  
the number of nodes we know to be primary and ready
* `cluster.total.state.query-not-ready`:  
the number of nodes we know to be query nodes and not ready
* `cluster.total.state.query-ready`:  
the number of nodes we know to be query nodes and ready
* `cluster.total.state.secondary-not-ready`:  
the number of nodes we know to be secondary and not ready
* `cluster.total.state.secondary-ready`:  
the number of nodes we know to be secondary and ready
* `idx.bigtable.add`:  
the duration of an add of one metric to the bigtable idx, including the add to the in-memory index, excluding the insert query
* `idx.bigtable.control.add`:  
the duration of add control messages processed
* `idx.bigtable.control.delete`:  
the duration of delete control messages processed
* `idx.bigtable.delete`:  
the duration of a delete of one or more metrics from the bigtable idx, including the delete from the in-memory index and the delete query
* `idx.bigtable.prune`:  
the duration of a prune of the bigtable idx, including the prune of the in-memory index and all needed delete queries
* `idx.bigtable.query-delete.exec`:  
time spent executing deletes (possibly repeatedly until success)
* `idx.bigtable.query-delete.fail`:  
how many delete queries for a metric failed (triggered by an update or a delete)
* `idx.bigtable.query-delete.ok`:  
how many delete queries for a metric completed successfully (triggered by an update or a delete)
* `idx.bigtable.query-insert.exec`:  
time spent executing inserts (possibly repeatedly until success)
* `idx.bigtable.query-insert.fail`:  
how many insert queries for a metric failed (triggered by an add or an update)
* `idx.bigtable.query-insert.ok`:  
how many insert queries for a metric completed successfully (triggered by an add or an update)
* `idx.bigtable.query-insert.wait`:  
time inserts spent in queue before being executed
* `idx.bigtable.save.bytes-per-request`:  
the number of bytes written to bigtable in each request.
* `idx.bigtable.save.skipped`:  
how many saves have been skipped due to the writeQueue being full
* `idx.bigtable.update`:  
the duration of an update of one metric to the bigtable idx, including the update to the in-memory index, excluding any insert/delete queries
* `idx.cassadra.query-delete.ok`:  
how many delete queries for a metric completed successfully (triggered by an update or a delete)
* `idx.cassadra.query-insert.ok`:  
how many insert queries for a metric completed successfully (triggered by an add or an update)
* `idx.cassandra.add`:  
the duration of an add of one metric to the cassandra idx, including the add to the in-memory index, excluding the insert query
* `idx.cassandra.control.add`:  
the duration of add control messages processed
* `idx.cassandra.control.delete`:  
the duration of delete control messages processed
* `idx.cassandra.delete`:  
the duration of a delete of one or more metrics from the cassandra idx, including the delete from the in-memory index and the delete query
* `idx.cassandra.error.cannot-achieve-consistency`:  
a counter of the cassandra idx not being able to achieve consistency for a given query
* `idx.cassandra.error.conn-closed`:  
a counter of how many times we saw a connection closed to the cassandra idx
* `idx.cassandra.error.no-connections`:  
a counter of how many times we had no connections remaining to the cassandra idx
* `idx.cassandra.error.other`:  
a counter of other errors talking to the cassandra idx
* `idx.cassandra.error.timeout`:  
a counter of timeouts seen to the cassandra idx
* `idx.cassandra.error.too-many-timeouts`:  
a counter of how many times we saw to many timeouts and closed the connection to the cassandra idx
* `idx.cassandra.error.unavailable`:  
a counter of how many times the cassandra idx was unavailable
* `idx.cassandra.prune`:  
the duration of a prune of the cassandra idx, including the prune of the in-memory index and all needed delete queries
* `idx.cassandra.query-delete.exec`:  
time spent executing deletes (possibly repeatedly until success)
* `idx.cassandra.query-delete.fail`:  
how many delete queries for a metric failed (triggered by an update or a delete)
* `idx.cassandra.query-insert.exec`:  
time spent executing inserts (possibly repeatedly until success)
* `idx.cassandra.query-insert.fail`:  
how many insert queries for a metric failed (triggered by an add or an update)
* `idx.cassandra.query-insert.wait`:  
time inserts spent in queue before being executed
* `idx.cassandra.save.skipped`:  
how many saves have been skipped due to the writeQueue being full
* `idx.cassandra.update`:  
the duration of an update of one metric to the cassandra idx, including the update to the in-memory index, excluding any insert/delete queries
* `idx.memory.add`:  
the duration of a (successful) add of a metric to the memory idx
* `idx.memory.delete`:  
the duration of a delete of one or more metrics from the memory idx
* `idx.memory.filtered`:  
number of series that have been excluded from responses due to their lastUpdate property
* `idx.memory.find`:  
the duration of memory idx find
* `idx.memory.find-cache.backoff`:  
the number of find caches in backoff mode
* `idx.memory.find-cache.entries`:  
the number of entries in the cache
* `idx.memory.find-cache.invalidation.drop`:  
the number of dropped invalidation requests
* `idx.memory.find-cache.invalidation.exec`:  
the number of executed invalidation requests
* `idx.memory.find-cache.invalidation.recv`:  
the number of received invalidation requests
* `idx.memory.find-cache.ops.hit`:  
a counter of findCache hits
* `idx.memory.find-cache.ops.miss`:  
a counter of findCache misses
* `idx.memory.get`:  
the duration of a get of one metric in the memory idx
* `idx.memory.list`:  
the duration of memory idx listings
* `idx.memory.meta-tags.enricher.ops.known-meta-records`:  
a counter of meta records known to the enricher
* `idx.memory.meta-tags.enricher.ops.metrics-with-meta-records`:  
a counter of metrics with at least one associated meta record
* `idx.memory.ops.add`:  
the number of additions to the memory idx
* `idx.memory.ops.update`:  
the number of updates to the memory idx
* `idx.memory.prune`:  
the duration of successful memory idx prunes
* `idx.memory.update`:  
the duration of (successful) update of a metric to the memory idx
* `idx.metrics_active`:  
the number of currently known metrics in the index
* `input.%s.metricdata.discarded.invalid`:  
a count of times a metricdata was invalid by input plugin
* `input.%s.metricdata.discarded.invalid_input`:  
a count of times a metricdata was considered invalid due to
invalid input data in the metric definition. all rejected metrics counted here are also counted in the above "invalid" counter
* `input.%s.metricdata.received`:  
the count of metricdata datapoints received by input plugin
* `input.%s.metricpoint.discarded.invalid`:  
a count of times a metricpoint was invalid by input plugin
* `input.%s.metricpoint.discarded.unknown`:  
the count of times the ID of a received metricpoint was not in the index, by input plugin
* `input.%s.metricpoint.received`:  
the count of metricpoint datapoints received by input plugin
* `input.%s.metricpoint_no_org.received`:  
the count of metricpoint_no_org datapoints received by input plugin
* `input.carbon.metrics_decode_err`:  
a count of times an input message (MetricData, MetricDataArray or carbon line) failed to parse
* `input.carbon.metrics_per_message`:  
how many metrics per message were seen. in carbon's case this is always 1.
* `input.kafka-mdm.metrics_decode_err`:  
a count of times an input message failed to parse
* `input.kafka-mdm.metrics_per_message`:  
how many metrics per message were seen.
* `input.kafka-mdm.partition.%d.lag`:  
how many messages (metrics) there are in the kafka partition (%d) that we have not yet consumed.
* `input.kafka-mdm.partition.%d.log_size`:  
the current size of the kafka partition (%d), aka the newest available offset.
* `input.kafka-mdm.partition.%d.offset`:  
the current offset for the partition (%d) that we have consumed.
* `mem.to_iter`:  
how long it takes to transform in-memory chunks to iterators
* `memory.bytes.obtained_from_sys`:  
the number of bytes currently obtained from the system by the process.  This is what the profiletrigger looks at.
* `memory.bytes_allocated_on_heap`:  
a gauge of currently allocated (within the runtime) memory.
* `memory.gc.cpu_fraction`:  
how much cpu is consumed by the GC across process lifetime, in pro-mille
* `memory.gc.gogc`:  
the current GOGC value (derived from the GOGC environment variable)
* `memory.gc.heap_objects`:  
how many objects are allocated on the heap, it's a key indicator for GC workload
* `memory.gc.last_duration`:  
the duration of the last GC STW pause in nanoseconds
* `memory.total_bytes_allocated`:  
a counter of total number of bytes allocated during process lifetime
* `memory.total_gc_cycles`:  
a counter of the number of GC cycles since process start
* `plan.run`:  
the time spent running the plan for a request (function processing of all targets and runtime consolidation)
* `pointslicepool.ops.get-candidate.hit`:  
how many times we could satisfy a get with a pointslice from the pool
* `pointslicepool.ops.get-candidate.miss`:  
how many times there was nothing in the pool to satisfy a get
* `pointslicepool.ops.get-candidate.unfit`:  
how many times a pointslice from the pool was not large enough to satisfy a get
* `pointslicepool.ops.get-make.large`:  
how many times a pointslice is allocated that is larger or equal to the default size
* `pointslicepool.ops.get-make.small`:  
how many times a pointslice is allocated that is smaller than the default size
* `pointslicepool.ops.put.large`:  
how many times a pointslice is added to the pool that is the same size or larger than the default
* `pointslicepool.ops.put.small`:  
how many times a pointslice is added to the pool that is smaller than the default
* `process.major_page_faults.counter64`:  
the number of major faults the process has made which have required loading a memory page from disk
* `process.minor_page_faults.counter64`:  
the number of minor faults the process has made which have not required loading a memory page from disk
* `process.resident_memory_bytes.gauge64`:  
a gauge of the process RSS from /proc/pid/stat
* `process.virtual_memory_bytes.gauge64`:  
a gauge of the process VSZ from /proc/pid/stat
* `recovered_errors.aggmetric.getaggregated.bad-aggspan`:  
how many times we detected an GetAggregated call
with an incorrect aggspan specified
* `recovered_errors.aggmetric.getaggregated.bad-consolidator`:  
how many times we detected an GetAggregated call
with an incorrect consolidator specified
* `recovered_errors.idx.memory.corrupt-index`:  
how many times
a corruption has been detected in one of the internal index structures
each time this happens, an error is logged with more details.
* `recovered_errors.idx.memory.invalid-tag`:  
how many times
an invalid tag for a metric is encountered.
each time this happens, an error is logged with more details.
* `runtime.goroutines.total`:  
how many goroutines there are
* `stats.generate_message`:  
how long it takes to generate the stats
* `store.bigtable.chunk_operations.save_fail`:  
counter of failed saves
* `store.bigtable.chunk_operations.save_ok`:  
counter of successful saves
* `store.bigtable.chunk_size.at_load`:  
the sizes of chunks seen when loading them
* `store.bigtable.chunk_size.at_save`:  
the sizes of chunks seen when saving them
* `store.bigtable.chunks_per_row`:  
how many chunks are retrieved per row in get queries
* `store.bigtable.get.error`:  
the count of reads that failed
* `store.bigtable.get.exec`:  
the duration of getting from bigtable store
* `store.bigtable.get.wait`:  
the duration of the get spent in the queue
* `store.bigtable.put.bytes`:  
the number of chunk bytes saved in each bulkApply
* `store.bigtable.put.exec`:  
the duration of putting in bigtable store
* `store.bigtable.put.wait`:  
the duration of a put in the wait queue
* `store.bigtable.rows_per_response`:  
how many rows come per get response
* `store.cassandra.chunk_operations.save_fail`:  
counter of failed saves
* `store.cassandra.chunk_operations.save_ok`:  
counter of successful saves
* `store.cassandra.chunk_size.at_load`:  
the sizes of chunks seen when loading them
* `store.cassandra.chunk_size.at_save`:  
the sizes of chunks seen when saving them
* `store.cassandra.chunks_per_response`:  
how many chunks are retrieved per response in get queries
* `store.cassandra.error.cannot-achieve-consistency`:  
a counter of the cassandra store not being able to achieve consistency for a given query
* `store.cassandra.error.conn-closed`:  
a counter of how many times we saw a connection closed to the cassandra store
* `store.cassandra.error.no-connections`:  
a counter of how many times we had no connections remaining to the cassandra store
* `store.cassandra.error.other`:  
a counter of other errors talking to the cassandra store
* `store.cassandra.error.timeout`:  
a counter of timeouts seen to the cassandra store
* `store.cassandra.error.too-many-timeouts`:  
a counter of how many times we saw to many timeouts and closed the connection to the cassandra store
* `store.cassandra.error.unavailable`:  
a counter of how many times the cassandra store was unavailable
* `store.cassandra.get.exec`:  
the duration of getting from cassandra store
* `store.cassandra.get.wait`:  
the duration of the get spent in the queue
* `store.cassandra.get_chunks`:  
the duration of how long it takes to get chunks
* `store.cassandra.put.exec`:  
the duration of putting in cassandra store
* `store.cassandra.put.wait`:  
the duration of a put in the wait queue
* `store.cassandra.rows_per_response`:  
how many rows come per get response
* `store.cassandra.to_iter`:  
the duration of converting chunks to iterators
* `tank.chunk_operations.clear`:  
a counter of how many chunks are cleared (replaced by new chunks)
* `tank.chunk_operations.create`:  
a counter of how many chunks are created
* `tank.discarded.new-value-for-timestamp`:  
points that have timestamps for which we already have data points.
these points are discarded.
data points can be incorrectly classified as metric tank.discarded.sample-out-of-order even when the timestamp
has already been used. This happens in two cases:
- when the reorder buffer is enabled, if the point is older than the reorder buffer retention window
- when the reorder buffer is disabled, if the point is older than the last data point
* `tank.discarded.received-too-late`:  
points received for the most recent chunk
when that chunk is already being "closed", ie the end-of-stream marker has been written to the chunk.
this indicates that your GC is actively sealing chunks and saving them before you have the chance to send
your (infrequent) updates.  Any points revcieved for a chunk that has already been closed are discarded.
* `tank.discarded.sample-out-of-order`:  
points that go back in time beyond the scope of the optional reorder window.
these points will end up being dropped and lost.
* `tank.discarded.sample-too-far-ahead`:  
count of points which got discareded because their timestamp
is too far in the future, beyond the limitation of the future tolerance window defined via the
retention.future-tolerance-ratio parameter.
* `tank.discarded.unknown`:  
points that have been discarded for unknown reasons.
* `tank.gc_metric`:  
the number of times the metrics GC is about to inspect a metric (series)
* `tank.metrics_active`:  
the number of currently known metrics (excl rollup series), measured every second
* `tank.metrics_reordered`:  
the number of points received that are going back in time, but are still
within the reorder window. in such a case they will be inserted in the correct order.
E.g. if the reorder window is 60 (datapoints) then points may be inserted at random order as long as their
ts is not older than the 60th datapoint counting from the newest.
* `tank.persist`:  
how long it takes to persist a chunk (and chunks preceding it)
this is subject to backpressure from the store when the store's queue runs full
* `tank.sample-too-far-ahead`:  
count of points with a timestamp which is too far in the future,
beyond the limitation of the future tolerance window defined via the retention.future-tolerance-ratio
parameter. it also gets increased if the enforcement of the future tolerance is disabled, this is
useful for predicting whether data points would get rejected once enforcement gets turned on.
* `tank.total_points`:  
the number of points currently held in the in-memory ringbuffer
* `version.%s`:  
the version of metrictank running.  The metric value is always 1
