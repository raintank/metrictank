package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	traceLog "github.com/opentracing/opentracing-go/log"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/tracing"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
)

var NotFoundErr = errors.New("not found")

var (

	// metric api.cluster.speculative.attempts is how many peer queries resulted in speculation
	speculativeAttempts = stats.NewCounter32("api.cluster.speculative.attempts")

	// metric api.cluster.speculative.wins is how many peer queries were improved due to speculation
	speculativeWins = stats.NewCounter32("api.cluster.speculative.wins")

	// metric api.cluster.speculative.requests is how many speculative http requests made to peers
	speculativeRequests = stats.NewCounter32("api.cluster.speculative.requests")
)

func (s *Server) explainPriority(ctx *middleware.Context) {
	var data []interface{}
	for _, p := range s.prioritySetters {
		data = append(data, p.ExplainPriority())
	}
	response.Write(ctx, response.NewJson(200, data, ""))
}

func (s *Server) getNodeStatus(ctx *middleware.Context) {
	response.Write(ctx, response.NewJson(200, cluster.Manager.ThisNode(), ""))
}

func (s *Server) setNodeStatus(ctx *middleware.Context, status models.NodeStatus) {
	primary, err := strconv.ParseBool(status.Primary)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf(
			"could not parse status to bool. %s",
			err.Error())),
		)
		return
	}
	cluster.Manager.SetPrimary(primary)
	ctx.PlainText(200, []byte("OK"))
}

func (s *Server) appStatus(ctx *middleware.Context) {
	if cluster.Manager.IsReady() {
		ctx.PlainText(200, []byte("OK"))
		return
	}

	response.Write(ctx, response.NewError(http.StatusServiceUnavailable, "node not ready"))
}

func (s *Server) getClusterStatus(ctx *middleware.Context) {
	status := models.ClusterStatus{
		ClusterName: cluster.ClusterName,
		NodeName:    cluster.Manager.ThisNode().GetName(),
		Members:     cluster.Manager.MemberList(false, false),
	}
	response.Write(ctx, response.NewJson(200, status, ""))
}

func (s *Server) postClusterMembers(ctx *middleware.Context, req models.ClusterMembers) {
	memberNames := make(map[string]struct{})
	var toJoin []string

	for _, memberNode := range cluster.Manager.MemberList(false, false) {
		memberNames[memberNode.GetName()] = struct{}{}
	}

	for _, peerName := range req.Members {
		if _, ok := memberNames[peerName]; !ok {
			toJoin = append(toJoin, peerName)
		}
	}

	resp := models.ClusterMembersResp{
		Status:       "ok",
		MembersAdded: 0,
	}

	if len(toJoin) == 0 {
		response.Write(ctx, response.NewJson(200, resp, ""))
		return
	}

	n, err := cluster.Manager.Join(toJoin)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf(
			"error when joining cluster members: %s", err.Error())),
		)
		return
	}
	resp.MembersAdded = n
	response.Write(ctx, response.NewJson(200, resp, ""))
}

// IndexFind returns a sequence of msgp encoded idx.Node's
func (s *Server) indexFind(ctx *middleware.Context, req models.IndexFind) {
	resp := models.NewIndexFindResp()

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, resp))
		return
	}

	for _, pattern := range req.Patterns {
		nodes, err := s.MetricIndex.Find(req.OrgId, pattern, req.From, req.Limit)
		if err != nil {
			response.Write(ctx, response.WrapError(err))
			return
		}
		resp.Nodes[pattern] = nodes
	}
	response.Write(ctx, response.NewMsgp(200, resp))
}

func (s *Server) indexTagDetails(ctx *middleware.Context, req models.IndexTagDetails) {

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.IndexTagDetailsResp{}))
		return
	}

	re, err := compileRegexAnchored(req.Filter)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	values := s.MetricIndex.TagDetails(req.OrgId, req.Tag, re)
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagDetailsResp{Values: values}))
}

func (s *Server) indexTags(ctx *middleware.Context, req models.IndexTags) {

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.IndexTagsResp{}))
		return
	}

	re, err := compileRegexAnchored(req.Filter)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	tags := s.MetricIndex.Tags(req.OrgId, re)
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagsResp{Tags: tags}))
}

func compileRegexAnchored(pattern string) (*regexp.Regexp, error) {
	if len(pattern) == 0 {
		return nil, nil
	}

	if pattern == "^.+" {
		return nil, nil
	}

	if pattern[0] != '^' {
		pattern = "^(?:" + pattern + ")"
	}
	return regexp.Compile(pattern)
}

func (s *Server) indexAutoCompleteTags(ctx *middleware.Context, req models.IndexAutoCompleteTags) {

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, models.StringList(nil)))
		return
	}

	// if there are no expressions given, we can shortcut the evaluation by not using a query
	if len(req.Expr) == 0 {
		tags := s.MetricIndex.FindTags(req.OrgId, req.Prefix, req.Limit)
		response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
		return
	}

	expressions := req.Expr
	if len(req.Prefix) > 0 {
		expressions = append(expressions, "__tag^="+req.Prefix)
	}

	query, err := tagquery.NewQueryFromStrings(expressions, 0, 0)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	tags := s.MetricIndex.FindTagsWithQuery(req.OrgId, req.Prefix, query, req.Limit)
	response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
	return
}

func (s *Server) indexAutoCompleteTagValues(ctx *middleware.Context, req models.IndexAutoCompleteTagValues) {

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, models.StringList(nil)))
		return
	}

	// if there are no expressions given, we can shortcut the evaluation by not using a query
	if len(req.Expr) == 0 {
		values := s.MetricIndex.FindTagValues(req.OrgId, req.Tag, req.Prefix, req.Limit)
		response.Write(ctx, response.NewMsgp(200, models.StringList(values)))
		return
	}

	expressions := req.Expr
	if len(req.Prefix) > 0 {
		expressions = append(expressions, req.Tag+"^="+req.Prefix)
	}

	query, err := tagquery.NewQueryFromStrings(expressions, 0, 0)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	tags := s.MetricIndex.FindTagValuesWithQuery(req.OrgId, req.Tag, req.Prefix, query, req.Limit)
	response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
}

func (s *Server) indexTagDelSeries(ctx *middleware.Context, request models.IndexTagDelSeries) {

	res := models.IndexTagDelSeriesResp{}

	// nothing to do on query nodes.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, res))
		return
	}

	for _, path := range request.Paths {
		tags, err := tagquery.ParseTagsFromMetricName(path)
		if err != nil {
			response.Write(ctx, response.WrapErrorForTagDB(err))
			return
		}

		expressions := make(tagquery.Expressions, len(tags))
		builder := strings.Builder{}
		for i := range tags {
			tags[i].StringIntoWriter(&builder)

			expressions[i], err = tagquery.ParseExpression(builder.String())
			if err != nil {
				response.Write(ctx, response.WrapErrorForTagDB(err))
				return
			}
			builder.Reset()
		}

		query, err := tagquery.NewQuery(expressions, 0, 0)
		if err != nil {
			response.Write(ctx, response.WrapErrorForTagDB(err))
			return
		}

		deleted, err := s.MetricIndex.DeleteTagged(request.OrgId, query)
		if err != nil {
			response.Write(ctx, response.WrapErrorForTagDB(err))
			return
		}
		res.Count += len(deleted)
	}

	response.Write(ctx, response.NewMsgp(200, res))
}

func (s *Server) localTagDelByQuery(request models.IndexTagDelByQuery) (int, error) {
	query, err := tagquery.NewQueryFromStrings(request.Expr, 0, request.OlderThan)
	if err != nil {
		return 0, err
	}

	if !request.Execute {
		// Use FindTerms to count what would be deleted
		matched, _ := s.MetricIndex.FindTerms(request.OrgId, []string{}, query)
		return int(matched), nil
	}

	if request.Method == "archive" {
		casidx, ok := s.MetricIndex.(*cassandra.CasIdx)
		if !ok {
			return 0, errors.New("Only cassandra index supports 'archive' method")
		}
		deleted, err := casidx.ArchiveTagged(request.OrgId, query)
		return len(deleted), err
	} else if request.Method == "delete" {
		deleted, err := s.MetricIndex.DeleteTagged(request.OrgId, query)
		return len(deleted), err
	}
	return 0, fmt.Errorf("Unknown delete method '%s'", request.Method)
}

func (s *Server) IndexTagDelByQuery(ctx *middleware.Context, request models.IndexTagDelByQuery) {

	res := models.IndexTagDelByQueryResp{}

	// nothing to do on query nodes.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, res))
		return
	}

	var err error
	res.Count, err = s.localTagDelByQuery(request)
	if err != nil {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	response.Write(ctx, response.NewMsgp(200, res))
}

func (s *Server) IndexTagTerms(ctx *middleware.Context, req models.IndexTagTerms) {
	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.GraphiteTagTermsResp{}))
		return
	}

	query, err := tagquery.NewQueryFromStrings(req.Expr, req.From, req.To)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	total, terms := s.MetricIndex.FindTerms(req.OrgId, req.Tags, query)

	response.Write(ctx, response.NewMsgp(200, &models.GraphiteTagTermsResp{TotalSeries: total, Terms: terms}))
}

func (s *Server) indexFindByTag(ctx *middleware.Context, req models.IndexFindByTag) {

	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.IndexFindByTagResp{}))
		return
	}

	query, err := tagquery.NewQueryFromStrings(req.Expr, req.From, req.To)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	metrics := s.MetricIndex.FindByTag(req.OrgId, query)
	response.Write(ctx, response.NewMsgp(200, &models.IndexFindByTagResp{Metrics: metrics}))
}

// IndexGet returns a msgp encoded schema.MetricDefinition
func (s *Server) indexGet(ctx *middleware.Context, req models.IndexGet) {

	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(404, nil))
		return
	}

	def, ok := s.MetricIndex.Get(req.MKey)
	if !ok {
		response.Write(ctx, response.NewError(http.StatusNotFound, "Not Found"))
		return
	}

	response.Write(ctx, response.NewMsgp(200, &def))
}

// IndexList returns msgp encoded schema.MetricDefinition's
func (s *Server) indexList(ctx *middleware.Context, req models.IndexList) {

	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgpArray(200, nil))
		return
	}

	defs := s.MetricIndex.List(req.OrgId)
	resp := make([]msgp.Marshaler, len(defs))
	for i := range defs {
		d := defs[i]
		resp[i] = &d
	}
	response.Write(ctx, response.NewMsgpArray(200, resp))
}

func (s *Server) getData(ctx *middleware.Context, request models.GetData) {
	var ss models.StorageStats
	series, err := s.getTargetsLocal(ctx.Req.Context(), &ss, request.Requests)
	if err != nil {
		// the only errors returned are from us catching panics, so we should treat them
		// all as internalServerErrors

		for _, s := range series {
			pointSlicePool.PutMaybeNil(s.Datapoints)
		}

		log.Errorf("HTTP getData() %s", err.Error())
		response.Write(ctx, response.WrapError(err))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.GetDataRespV1{Stats: ss, Series: series}))

	for _, s := range series {
		pointSlicePool.PutMaybeNil(s.Datapoints)
	}
}

func (s *Server) indexDelete(ctx *middleware.Context, req models.IndexDelete) {

	// nothing to do on query nodes.
	if s.MetricIndex == nil {
		return
	}

	defs, err := s.MetricIndex.Delete(req.OrgId, req.Query)
	if err != nil {
		// errors can only be caused by bad request.
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	resp := models.MetricsDeleteResp{
		DeletedDefs: len(defs),
	}
	response.Write(ctx, response.NewMsgp(200, &resp))
}

type PeerResponse struct {
	peer cluster.Node
	buf  []byte
}

type GenericPeerResponse struct {
	peer cluster.Node
	resp interface{}
}

// queryAllPeers takes a request and the path to request it on, then fans it out
// across the cluster, except to the local peer or peers without data.
// Note: unlike the other queryAllPeers methods, we include peers that are not ready
// If any peer request fails, requests to other peers are aborted.
// ctx:          request context
// data:         request to be submitted
// name:         name to be used in logging & tracing
// path:         path to request on
func (s *Server) queryAllPeers(ctx context.Context, data cluster.Traceable, name, path string) (map[string]PeerResponse, map[string]error) {

	peers := cluster.Manager.MemberList(false, true)
	log.Debugf("HTTP %s across %d instances", name, len(peers)-1)

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	responses := make(chan struct {
		data PeerResponse
		err  error
	}, 1)
	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer.IsLocal() {
			continue
		}
		wg.Add(1)
		go func(peer cluster.Node) {
			defer wg.Done()
			log.Debugf("HTTP Render querying %s%s", peer.GetName(), path)
			buf, err := peer.Post(reqCtx, name, path, data)
			if err != nil {
				cancel()
				log.Errorf("HTTP Render error querying %s%s: %q", peer.GetName(), path, err.Error())
			}
			responses <- struct {
				data PeerResponse
				err  error
			}{PeerResponse{peer, buf}, err}
		}(peer)
	}
	// wait for all list goroutines to end, then close our responses channel
	go func() {
		wg.Wait()
		close(responses)
	}()

	result := make(map[string]PeerResponse)
	errors := make(map[string]error)
	for resp := range responses {
		if resp.err != nil {
			errors[resp.data.peer.GetName()] = resp.err
		} else {
			result[resp.data.peer.GetName()] = resp.data
		}
	}

	return result, errors
}

// queryAllShards takes a function and calls it for one peer in each shard
// across the cluster. If any peer fails, we try another replica. If enough
// peers have been heard from (based on speculation-threshold configuration), and we
// are missing the others, try to speculatively query other members of the shard group.
// all responses are collected and returned at once.
// ctx:          request context
// name:         name to be used in logging & tracing
// fetchFunc:    function to call to fetch the data from a peer
func (s *Server) queryAllShards(ctx context.Context, name string, fetchFn fetchFunc) (map[string]PeerResponse, error) {
	result := make(map[string]PeerResponse)

	responseChan, errorChan := s.queryAllShardsGeneric(ctx, name, fetchFn)

	for resp := range responseChan {
		result[resp.peer.GetName()] = PeerResponse{
			peer: resp.peer,
			buf:  resp.resp.([]byte),
		}
	}

	err := <-errorChan
	return result, err
}

// queryAllShardsGeneric takes a function and calls it for one peer in each shard
// across the cluster. If any peer fails, we try another replica. If enough
// peers have been heard from (based on speculation-threshold configuration), and we
// are missing the others, try to speculatively query other members of the shard group.
// all responses and errors are streamed through the returned channels
// ctx:          request context
// name:         name to be used in logging & tracing
// fetchFunc:    function to call to fetch the data from a peer
func (s *Server) queryAllShardsGeneric(ctx context.Context, name string, fetchFn fetchFunc) (<-chan GenericPeerResponse, <-chan error) {
	peerGroups, err := cluster.MembersForSpeculativeQuery()
	if err != nil {
		log.Errorf("HTTP peerQuery unable to get peers, %s", err.Error())
		resultChan := make(chan GenericPeerResponse)
		errorChan := make(chan error, 1)
		errorChan <- err
		return resultChan, errorChan
	}

	return queryPeers(ctx, peerGroups, name, fetchFn)
}

// fetchFunc is a function to query the given cluster.Node
// the list of all nodes in the cluster is passed as well for additional context
// Example: fetchFunc can use this to determine the ratio of how much data the target peer owns
// compared to the cluster as a whole.  Caveat: this is based on live cluster state. If shardgroups
// go completely down it'll look like the target peer owns more of the cluster than it actually does.
// if query limits are set based on this, the limits would loosen up as shards leave the cluster.
type fetchFunc func(context.Context, cluster.Node, map[int32][]cluster.Node) (interface{}, error)

func fetchFuncPost(data cluster.Traceable, name, path string) fetchFunc {
	return func(reqCtx context.Context, peer cluster.Node, peerGroups map[int32][]cluster.Node) (interface{}, error) {
		return peer.Post(reqCtx, name, path, data)
	}
}

type shardResponse struct {
	shardGroup int32
	data       GenericPeerResponse
	err        error
}

// shardState represents the state of a shard wrt speculative execution of a query
type shardState struct {
	shard          int32          // shard ID
	remainingPeers []cluster.Node // peers that we have not sent a query to yet
	inflight       int            // number of requests in flight
}

// AskPeer issues the query on the next peer, if available, and returns it
func (state *shardState) AskPeer(ctx context.Context, peerGroups map[int32][]cluster.Node, fn fetchFunc, responses chan shardResponse) (cluster.Node, bool) {
	if len(state.remainingPeers) == 0 {
		return nil, false
	}
	peer := state.remainingPeers[0]
	state.remainingPeers = state.remainingPeers[1:]
	state.inflight++
	go state.askPeer(ctx, peerGroups, peer, fn, responses)
	return peer, true
}

func (state *shardState) askPeer(ctx context.Context, peerGroups map[int32][]cluster.Node, peer cluster.Node, fetchFn fetchFunc, responses chan shardResponse) {
	//log.Debugf("HTTP Render querying %s%s", peer.GetName(), path)
	resp, err := fetchFn(ctx, peer, peerGroups)
	select {
	case <-ctx.Done():
		return
	case responses <- shardResponse{state.shard, GenericPeerResponse{peer, resp}, err}:
	}
	// Only print if the context isn't done
	if err != nil {
		log.Errorf("Peer %s responded with error = %q", peer.GetName(), err.Error())
	}
}

// queryPeers takes a function and peers grouped by shard. The function
// is called it for one peer of each shard. If any peer fails, we try another replica.
// If enough peers have been heard from (based on speculation-threshold configuration),
// and we are missing the others, try to speculatively query other members of the shard group.
// ctx:          request context
// peerGroups:   peers grouped by shard
// fetchFn:        function to call to fetch the data from a peer
func queryPeers(ctx context.Context, peerGroups map[int32][]cluster.Node, name string, fetchFn fetchFunc) (<-chan GenericPeerResponse, <-chan error) {
	resultChan := make(chan GenericPeerResponse)
	errorChan := make(chan error, 1)

	go func() {
		defer close(errorChan)
		defer close(resultChan)

		span := opentracing.SpanFromContext(ctx)
		OpCtx, opSpan := tracing.NewSpan(ctx, span.Tracer(), name)
		reqCtx, cancel := context.WithCancel(OpCtx)
		defer cancel()
		defer opSpan.Finish()

		responses := make(chan shardResponse)
		originalPeers := make(map[string]struct{}, len(peerGroups))    // track the first peers we query for each shard
		receivedResponses := make(map[int32]struct{}, len(peerGroups)) // non-error responses per shard
		states := make(map[int32]*shardState, len(peerGroups))         // query state per shard

		for shard, peers := range peerGroups {
			if len(peers) == 0 {
				log.Warningf("HTTP Peer group for shard %d has no peers", shard)
				delete(peerGroups, shard)
				continue
			}
			state := &shardState{
				shard:          shard,
				remainingPeers: peers,
			}
			peer, _ := state.AskPeer(reqCtx, peerGroups, fetchFn, responses) // thanks to the above check we always know there was a peer available
			originalPeers[peer.GetName()] = struct{}{}
			states[shard] = state
		}

		var specSpan opentracing.Span
		var ticker *time.Ticker
		var tickChan <-chan time.Time
		if speculationThreshold != 1 {
			ticker = time.NewTicker(5 * time.Millisecond)
			tickChan = ticker.C
			defer ticker.Stop()
		}

		for len(receivedResponses) < len(peerGroups) {
			select {
			case <-reqCtx.Done():
				//request canceled
				return
			case resp := <-responses:
				states[resp.shardGroup].inflight--

				if _, ok := receivedResponses[resp.shardGroup]; ok {
					// already received this response (possibly speculatively)
					continue
				}

				if resp.err != nil {
					if resp.err.Error() == "400 Bad Request" {
						// if we got bad request, then retrying it on a different replica will result in the same
						// Cancel the reqCtx, which will cancel all in-flight requests.
						cancel()
						errorChan <- resp.err
						return
					}

					// if we can try another peer for this shardGroup, do it
					_, ok := states[resp.shardGroup].AskPeer(reqCtx, peerGroups, fetchFn, responses)
					if ok {
						speculativeRequests.Inc()
						continue
					}
					// if there is another request in-flight for this shardGroup, then we can wait for that
					if states[resp.shardGroup].inflight > 0 {
						continue
					}
					// we're out of options. Cancel the reqCtx, which will cancel all in-flight requests.
					cancel()
					errorChan <- resp.err
					return
				}

				resultChan <- resp.data
				receivedResponses[resp.shardGroup] = struct{}{}
				delete(originalPeers, resp.data.peer.GetName())

			case <-tickChan:
				// Check if it's time to speculate!
				ratioReceived := float64(len(receivedResponses)) / float64(len(peerGroups))
				if ratioReceived >= speculationThreshold {
					// kick off speculative queries to other members now
					ticker.Stop()
					speculativeAttempts.Inc()
					var specCtx context.Context
					specCtx, specSpan = tracing.NewSpan(OpCtx, span.Tracer(), "speculative-queries")
					defer specSpan.Finish()
					for shardGroup := range peerGroups {
						if _, ok := receivedResponses[shardGroup]; ok {
							continue
						}

						if _, ok := states[shardGroup].AskPeer(specCtx, peerGroups, fetchFn, responses); ok {
							speculativeRequests.Inc()
						}
					}
				}
			}
		}

		if len(originalPeers) > 0 {
			speculativeWins.Inc()
			if specSpan != nil {
				specSpan.LogFields(traceLog.Int32("spec-wins", int32(len(originalPeers))))
			} else {
				log.Warnf("Something weird: %v", originalPeers)
			}
		}
	}()

	return resultChan, errorChan
}
