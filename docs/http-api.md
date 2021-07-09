# HTTP api

This documents endpoints aimed to be used by users. For internal clustering http endpoints, which may change, refer to the source.

- Note that some of the endpoints rely on being a fed a proper Org-Id.  See [Multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md).

- For GET requests, any parameters not specified as a header can be passed as an HTTP query string parameter.

## Get app status

```
GET /
POST /
```

returns:

* `200 OK` if the node is [ready](clustering.md#priority-and-ready-state)
* `503 Service not ready` otherwise.

#### Example


```bash
curl "http://localhost:6060"
```


## Walk the metrics tree and return every metric found that is visible to the org as a sorted JSON array

```
GET /metrics/index.json
POST /metrics/index.json
```

* header `X-Org-Id` required

Returns metrics stored under the given org, as well as public data (see [multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md))

#### Example


```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/metrics/index.json"
```

## Find all metrics visible to the given org

```
GET /metrics/find
POST /metrics/find
```

* header `X-Org-Id` required
* query (required): can be an id, and use all graphite glob patterns (`*`, `{}`, `[]`, `?`)
* format: json, treejson, completer, pickle, or msgpack. (defaults to json)
* jsonp

Returns metrics which match the query and are stored under the given org or are public data (see [multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md))
the completer format is for completion UI's such as graphite-web.
json and treejson are the same.

#### Example

```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/metrics/find?query=statsd.fakesite.counters.session_start.*.count"
```

## Expand metric name patterns into all names matching it

```
GET /metrics/expand
POST /metrics/expand
```

Returns metrics which match the given query/queries and are stored under the given org or are public data (see [multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md)).
Very similar to `/metrics/find`, the main difference is that it does not deduplicate the results by the last name node and it returns them in a more simple format.

##### Parameters

* header `X-Org-Id` required
* query (required): a metric name, and can use all graphite glob patterns (`*`, `{}`, `[]`, `?`). can be specified multiple times
* groupByExpr: if true, then the results get grouped by the query which yielded them, otherwise all results are in a flat list. (defaults to false)
* leavesOnly: if true, only leaf nodes get returned, if false branch nodes also get returned. (defaults to false)
* jsonp

#### Example

```bash
curl 'http://localhost:6061/metrics/expand?groupByExpr=true&query=some.id.of.a.metric.*&query=other.metric.*.*'
```

## Find tagged metrics

```
GET /tags/findSeries
POST /tags/findSeries
```

Returns metrics which match tag queries and have received an update since `from`.
Note: the returned results are not deduplicated and in certain cases it is possible
that duplicate entries will be returned.

##### Parameters

* expr (required): a list of [tag expressions](#tag-expressions)
* from: epoch time, only return results last seen after this (optional. defaults to 0 meaning no limit)
* to: epoch time, only return results last seen before this (optional. defaults to 0 meaning no limit)
* format: series-json, lastts-json. (defaults to series-json)
* limit: max number to return. (default: 0)
  Note: the resultset is also subjected to the `http.max-series-per-req` config setting.
  if the result set is larger than `http.max-series-per-req`, an error is returned. If it breaches the provided limit, the result is truncated.
* meta: If false and format is `series-json` then return series names as array (graphite compatibility). If true, include meta information like warnings.  (defaults to false)

##### Example

```sh
curl "http://localhost:6060/tags/findSeries?expr=datacenter=dc1&expr=server=web01"

[
  "disk.used;datacenter=dc1;rack=a1;server=web01"
]
```

```sh
curl "http://localhost:6060/tags/findSeries?expr=datacenter=dc1&expr=server=web01&format=lastts-json"

{
    "series": [
        {
            "lastTs": 1576683990,
            "val": "disk.used;datacenter=dc1;rack=a1;server=web01"
        }
    ]
}
```

### Tag Exploration

#### Count tag values With `/tags/terms`

Returns count of series for each tag value which matches tag queries for a given set of tag keys.

* Method: GET or POST
* API key type: any (including MetricsPublisher)

##### Parameters

* expr (required): a list of [tag expressions](#tag-expressions)
* tags: a list of tag keys for which to count values series

##### Example

```sh
curl "http://localhost:6060/tags/terms?expr=datacenter=dc1&expr=server=web01&tags=rack"

{
  "totalSeries": 5892,
  "terms": {
    "rack": {
      "a1": 2480,
      "a2": 465,
      "b1": 2480,
      "b2": 467
    }
  }
}
```

## Deleting metrics

This will delete any metrics (technically metricdefinitions) matching the query from the index.
Note that unlike find and render patterns, these queries are recursive.
So if the delete query matches a branch, every series under that branch will be deleted.
Note that the data stays in the datastore until it expires.
Should the metrics enter the system again with the same metadata, the data will show up again.

```
POST /metrics/delete
```

* header `X-Org-Id` required
* query (required): can be a metric key, and use all graphite glob patterns (`*`, `{}`, `[]`, `?`)

#### Example

```bash
curl -H "X-Org-Id: 12345" --data query=statsd.fakesite.counters.session_start.*.count "http://localhost:6060/metrics/delete"
```

## Deleting tagged metrics (Preferred Method)

This will delete (if `execute` is true) metrics matching all of the specified `expressions` parameters and that have not been seen since before the `olderThan` parameter.
Note that the data stays in the datastore until it expires.
Should the metrics enter the system again with the same metadata, the data will show up again.

```
POST /tags/delByQuery
```

* header `X-Org-Id` required
* expr (required, multiple allowed): A tag filter expression (e.g. `name=reqs.count`)
* olderThan (optional): A unix epoch time. Series seen more recently than this will not be deleted.
* execute (optional, default false): Execute this deletion. If false, just returns the count that *would* have been deleted
* method (optional, default `delete`): Method of deletion to use.
  - `delete` - delete from index table
  - `archive` - move from index table to index archive. Currently only supported by cassandra index.

#### Example

The following request will delete series with tag `cluster=decommed`.
```bash
curl -H "X-Org-Id: 12345" -d "expr=cluster=decommed" -d "execute=true" "http://localhost:6060/tags/delByQuery"

{
    "count": 120,
    "peers": {
        "m1": 120
    }
}
```


## Deleting tagged metrics (Method 2)

This will delete the metrics (technically metricdefinitions) matching the `path` parameter(s).
Note that the data stays in the datastore until it expires.
Should the metrics enter the system again with the same metadata, the data will show up again.

```
POST /tags/delSeries
```

* header `X-Org-Id` required
* path (required, multiple allowed): A single Graphite series

#### Example

```bash
curl -H "X-Org-Id: 12345" -d "path=some.series;key=value" -d "path=another.series;tag=value" "http://localhost:6060/tags/delSeries"
```

## Graphite query api

Graphite-web-like api. It can return JSON, pickle or messagepack output

```
GET /render
POST /render
```

* header `X-Org-Id` required
* maxDataPoints: int (default: 800)
* target: mandatory. one or more metric names or patterns, like graphite.
* from: see [timespec format](#tspec) (default: 24h ago) (exclusive)
* to/until : see [timespec format](#tspec)(default: now) (inclusive)
* format: json, msgp, pickle, csv or msgpack (default: json). (note: msgp and msgpack are similar, but msgpack is for use with graphite)
* meta: use 'meta=true' to enable metadata in response (see below). Only supported for json responses.
* process: all, stable, none (default: stable). Controls metrictank's eagerness of fulfilling the request with its built-in processing functions
  (as opposed to proxying to the fallback graphite).
  - all: process request without fallback if we have all the needed functions, even if they are marked unstable (under development)
  - stable: process request without fallback if we have all the needed functions and they are marked as stable.
  - none: always defer to graphite for processing.

  If metrictank doesn't have a requested function, it always proxies to graphite, irrespective of this setting.
* optimizations: can override http.pre-normalization and http.mdp-optimization options. empty (default) : no override. either "none" to force no optimizations, or a csv list with either of both of "pn", "mdp" to enable those options.

Data queried for must be stored under the given org or be public data (see [multi-tenancy](https://github.com/grafana/metrictank/blob/master/docs/multi-tenancy.md))

#### Example

```bash
curl -H "X-Org-Id: 12345" "http://localhost:6060/render?target=statsd.fakesite.counters.session_start.*.count&from=3h&to=2h"
```

#### Metadata

The metadata of a render response (provided when `meta=true` is passed and format is json), includes:

* response global performance measurements
* series-specific lineage information describing storage-schemas, read archive, archive interval and any consolidation and normalization applied.
  note that explicit function calls like summarize are *not* considered runtime consolidation for this purpose.

##### Response-global performance measurements

| Key                                 | Description                                                                |
| ----------------------------------- | -------------------------------------------------------------------------- |
| executeplan.resolve-series.ms       | Time spent doing the (distributed) index query                             |
| executeplan.get-targets.ms          | Time spent doing the (distributed) fetching of data                        |
| executeplan.prepare-series.ms       | Time spent preparing for plan run. so merging, sorting of fetched series   |
| executeplan.plan-run.ms             | Time spent executing all processing functions                              |
| executeplan.series-fetch.count      | Number of series fetched                                                   |
| executeplan.points-fetch.count      | Number of points fetched                                                   |
| executeplan.points-return.count     | Number of points returned                                                  |
| executeplan.cache-miss.count        | Number of cache misses (series with no useful chunks in cache)             |
| executeplan.cache-hit-partial.count | Number of partial cache hits (series with some useful chunks in cache)     |
| executeplan.cache-hit.count         | Number of full cache hits (series with all needed chunks in cache)         |
| executeplan.chunks-from-tank.count  | Number of chunks loaded from tank                                          |
| executeplan.chunks-from-cache.count | Number of chunks loaded from chunk cache                                   |
| executeplan.chunks-from-store.count | Number of chunks loaded from data storage                                  |

##### Series-specific lineage information

Every output series comes with lineage information. The lineage information is one or more lineage sections.
Each section describes a certain lineage, and the number of input series matching that lineage that were part
of the corresponding output series.
Each lineage section has these fields:

| Key                    | Description                                                                                                    |
| ---------------------- | -------------------------------------------------------------------------------------------------------------- |
| schema-name            | Name of the section in storage-schemas.conf                                                                    |
| schema-retentions      | Retentions defined in storage-schemas.conf                                                                     |
| archive-read           | Which archive was read as defined in the retentions. (0 means raw, 1 first rollup, etc)                        |
| archive-interval       | The native interval of the archive that was read                                                               |
| aggnum-norm            | If >1, number of points aggregated together per point, as part of normalization                                |
| aggnum-rc              | If >1, number of points aggregated together per output point, as part of runtime consolidation (MaxDataPoints) |
| consolidator-normfetch | Consolidator used for normalization (if aggnum-norm > 1) and which rollup was read (if archive-read > 0)       |
| consolidator-rc        | Consolidator used for runtime consolidation (MaxDataPoints) (if aggnum-rc > 1)                                 |
| count                  | Number of input series matching this lineage that were part of this output series                              |


## Get Cluster Status

```
GET /node
```

returns a json document with the following fields:

* "name": the node name
* "primary": whether the node is a primary node or not
* "primaryChange": timestamp of when the primary state last changed
* "version": metrictank version
* "state": whether the node is [ready](clustering.md#priority-and-ready-state) to handle requests or not
* "stateChange": timestamp of when the state last changed
* "started": timestamp of when the node started up

#### Example

```bash
curl "http://localhost:6060/node"
```

## Set Cluster Status

```
POST /node
```

parameter values :

* `primary true|false`

Sets the primary status to this node to true or false.

#### Example

```bash
curl --data primary=true "http://localhost:6060/node"
```

## Analyze instance priority

```
GET /priority
```

Gets all enabled plugins to declare how they compute their
priority scores.

#### Example

```bash
curl -s http://localhost:6060/priority | jsonpp
[
    "carbon-in: priority=0 (always in sync)",
    {
        "Title": "kafka-mdm:",
        "Explanation": {
            "Explanation": {
                "Status": {
                    "0": {
                        "Lag": 1,
                        "Rate": 26494,
                        "Priority": 0
                    },
                    "1": {
                        "Lag": 2,
                        "Rate": 24989,
                        "Priority": 0
                    }
                },
                "Priority": 0,
                "Updated": "2018-06-06T11:56:24.399840121Z"
            },
            "Now": "2018-06-06T11:56:25.016645631Z"
        }
    }
]
```

## Cache delete

```
GET /ccache/delete
POST /ccache/delete
```

* `X-Org-Id`: required
* patterns: one or more query (glob) patterns. Use `**` to mean "all data" (full reset)
* expr: tag expressions
* propagate: whether to propagate to other cluster nodes. true/false

Remove chunks from the cache for matching series, or wipe the entire cache

#### Example

```bash
curl -v -X POST -d '{"propagate": true, "orgId": 1, "patterns": ["**"]}' -H 'Content-Type: application/json' http://localhost:6060/ccache/delete
```

## Get Meta Records

```
GET /metaTags
```

Returns the list of meta tag records that currently exist. At the moment these records
have no effect, but in the future they will be used to associated "virtual" tags with
metrics based on defined rules.

## Adding, Updating, Deleting Meta Records

```
POST /metaTags/upsert
```

This route can be used to create, update and delete meta tag rules (meta records).
Each meta record is identified by its set of query expressions, if a record gets
posted to this URL which has a set of expressions that doesn't exist yet, a new
meta record gets created. If its set of query expressions already exists, then the
existing meta record gets updated. If the new record has no meta tags associated
with it, then an existing record with the same set of expressions gets deleted.
If the following calls are made to a Metrictank that uses the Cassandra persistent
index and which has index updating enabled, all modifications also get persisted
into the Cassandra index, from where they can be loaded by other Metrictanks.

## Example

```
# we start with no meta records defined
~$ curl -s \
    http://localhost:6063/metaTags \
    | jq
[]

# we insert a simple meta record which assigns the meta tag "meta=tag" to all
# metrics that match the query expression "real=tag"
~$ curl -s \
    'http://localhost:6063/metaTags/upsert' \
    -H 'Content-Type: application/json' \
    -d '{"metaTags": ["meta=tag"], "expressions": ["real=tag"]}' \
    | jq
{
  "Status": "OK"
}

# we retrieve the list of meta records to confirm that the one we just
# inserted has been created
~$ curl -s \
    http://localhost:6063/metaTags \
    | jq
[
  {
    "metaTags": [
      "meta=tag"
    ],
    "expressions": [
      "real=tag"
    ]
  }
]

# we modify the existing meta record. note that the set of expressions is the ID
# of the record, because there is already a record with the same ID the existing
# record gets replaced
~$ curl -s \
    'http://localhost:6063/metaTags/upsert' \
    -H 'Content-Type: application/json' \
    -d '{"metaTags": ["meta=tag2"], "expressions": ["real=tag"]}' \
    | jq
{
  "Status": "OK"
}

# we retrieve the list one more time to see the updated meta record
~$ curl -s \
    http://localhost:6063/metaTags \
    | jq
[
  {
    "metaTags": [
      "meta=tag2"
    ],
    "expressions": [
      "real=tag"
    ]
  }
]

# we delete the record by assigning its ID (its expressions) an empty
# list of meta tags
~$ curl -s \
    http://localhost:6063/metaTags/upsert \
    -H 'Content-Type: application/json' \
    -d '{"metaTags": [], "expressions": ["real=tag"]}' \
    | jq
{
  "Status": "OK"
}

# we retrieve the list one more time to see that it has been deleted
~$ curl -s \
    http://localhost:6063/metaTags \
    | jq
[]
```

## Batch updating all Meta Tag Records

```
POST /metaTags/swap
```

This endpoint is an alternative to the above "upsert". It accepts a list of meta tag rules
(meta records) and replaces all existing records with the new ones. This is useful for users
who first generate all of their meta records and then want to swap the present set of records
out by replacing it with the new one.
Just like in the case of the above upsert call, if these calls are made to a Metrictank that
uses the Cassandra persistent index and which has index updating enabled, all modifications
also get persisted into the Cassandra index, from where they can be loaded by other Metrictanks.

## Example

```
# we start with no meta records defined
~$ curl -s \
    http://localhost:6063/metaTags \
    | jq
[]

# we call swap to replace the current empty set of records with the ones we want
~$ curl -s \
    'http://localhost:6063/metaTags/swap' \
    -H 'Content-Type: application/json' \
    -d '{"records": [{"metaTags": ["my=tag"], "expressions": ["some=tag"]}]}' \
    | jq
{
  "Status": "OK"
}

# we retrieve the list of records to verify that the new record is there
~$ curl -s \
    http://localhost:6063/metaTags \
    | jq
[
  {
    "metaTags": [
      "my=tag"
    ],
    "expressions": [
      "some=tag"
    ]
  }
]

# we do another swap to delete the existing of records and replace them with a completely new ones
~$ curl -s \
    'http://localhost:6063/metaTags/swap' \
    -H 'Content-Type: application/json' \
    -d '{"records": [{"metaTags": ["some=metatag"], "expressions": ["a=b"]}, {"metaTags": ["a=b"], "expressions": ["c=d"]}]}' \
    | jq
{
  "Status": "OK"
}

# we retrieve the list of records one more time to verify that the new records have
# replaced the old ones
~$ curl -s \
    http://localhost:6063/metaTags \
    | jq
[
  {
    "metaTags": [
      "some=metatag"
    ],
    "expressions": [
      "a=b"
    ]
  },
  {
    "metaTags": [
      "a=b"
    ],
    "expressions": [
      "c=d"
    ]
  }
]
```

## Misc

### Tspec

The time specification is used throughout the http api and it can be any of these forms:

* now: current time on server
* any integer: unix timestamp
* `-offset` or `offset` gets interpreted as current time minus offset.
  Where `offset` is a sequence of one or more `<num><unit>` where unit is one of:

	- ``, `s`, `sec`, `secs`, `second`, `seconds`
	- `m`, `min`, `mins`, `minute`, `minutes`
	- `h`, `hour`, `hours`
	- `d`, `day`, `days`
	- `w`, `week`, `weeks`
	- `mon`, `month`, `months`
	- `y`, `year`, `years`

* datetime in any of the following formats: `15:04 20060102`, `20060102`, `01/02/06`
