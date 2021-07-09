package models

import (
	"fmt"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/schema"
	opentracing "github.com/opentracing/opentracing-go"
	traceLog "github.com/opentracing/opentracing-go/log"
)

type NodeStatus struct {
	Primary string `json:"primary" form:"primary" binding:"Required"`
}

type ClusterStatus struct {
	ClusterName string         `json:"clusterName"`
	NodeName    string         `json:"nodeName"`
	Members     []cluster.Node `json:"members"`
}

type ClusterMembers struct {
	Members []string `json:"members"`
}

type ClusterMembersResp struct {
	Status       string `json:"status"`
	MembersAdded int    `json:"membersAdded"`
}

type IndexList struct {
	OrgId uint32 `json:"orgId" form:"orgId" binding:"Required"`
}

func (i IndexList) Trace(span opentracing.Span) {
	span.SetTag("orgId", i.OrgId)
}

func (i IndexList) TraceDebug(span opentracing.Span) {
}

type IndexFindByTag struct {
	OrgId uint32   `json:"orgId" binding:"Required"`
	Expr  []string `json:"expressions"`
	From  int64    `json:"from"`
	To    int64    `json:"to"`
}

func (t IndexFindByTag) Trace(span opentracing.Span) {
	span.SetTag("orgId", t.OrgId)
	span.LogFields(
		traceLog.Int64("from", t.From),
		traceLog.Int64("to", t.To),
		traceLog.String("expressions", fmt.Sprintf("%q", t.Expr)),
	)
}

func (i IndexFindByTag) TraceDebug(span opentracing.Span) {
}

type IndexTagDetails struct {
	OrgId  uint32 `json:"orgId" binding:"Required"`
	Filter string `json:"filter"`
	Tag    string `json:"tag" binding:"Required"`
}

func (t IndexTagDetails) Trace(span opentracing.Span) {
	span.SetTag("orgId", t.OrgId)
	span.LogFields(
		traceLog.String("filter", t.Filter),
		traceLog.String("tag", t.Tag),
	)
}

func (i IndexTagDetails) TraceDebug(span opentracing.Span) {
}

type IndexTags struct {
	OrgId  uint32 `json:"orgId" binding:"Required"`
	Filter string `json:"filter"`
}

func (t IndexTags) Trace(span opentracing.Span) {
	span.SetTag("orgId", t.OrgId)
	span.LogFields(
		traceLog.String("filter", t.Filter),
	)
}

func (i IndexTags) TraceDebug(span opentracing.Span) {
}

type IndexAutoCompleteTags struct {
	OrgId  uint32   `json:"orgId" binding:"Required"`
	Prefix string   `json:"Prefix"`
	Expr   []string `json:"expressions"`
	Limit  uint     `json:"limit"`
}

func (t IndexAutoCompleteTags) Trace(span opentracing.Span) {
	span.SetTag("orgId", t.OrgId)
	span.LogFields(
		traceLog.String("prefix", t.Prefix),
		traceLog.String("expressions", fmt.Sprintf("%q", t.Expr)),
		traceLog.Int("limit", int(t.Limit)),
	)
}

func (i IndexAutoCompleteTags) TraceDebug(span opentracing.Span) {
}

type IndexAutoCompleteTagValues struct {
	OrgId  uint32   `json:"orgId" binding:"Required"`
	Tag    string   `json:"tag"`
	Prefix string   `json:"prefix"`
	Expr   []string `json:"expressions"`
	Limit  uint     `json:"limit"`
}

func (t IndexAutoCompleteTagValues) Trace(span opentracing.Span) {
	span.SetTag("orgId", t.OrgId)
	span.LogFields(
		traceLog.String("prefix", t.Prefix),
		traceLog.String("tag", t.Tag),
		traceLog.String("expressions", fmt.Sprintf("%q", t.Expr)),
		traceLog.Int("limit", int(t.Limit)),
	)
}

func (i IndexAutoCompleteTagValues) TraceDebug(span opentracing.Span) {
}

type IndexTagTerms struct {
	OrgId uint32   `json:"orgId" binding:"Required"`
	Tags  []string `json:"tags"`
	Expr  []string `json:"expressions"`
	From  int64    `json:"from" form:"from"`
	To    int64    `json:"to" form:"to"`
}

func (t IndexTagTerms) Trace(span opentracing.Span) {
	span.SetTag("orgId", t.OrgId)
	span.LogFields(
		traceLog.String("tags", fmt.Sprintf("%q", t.Expr)),
		traceLog.String("expressions", fmt.Sprintf("%q", t.Expr)),
		traceLog.String("from", fmt.Sprintf("%d", t.From)),
		traceLog.String("to", fmt.Sprintf("%d", t.To)),
	)
}

func (i IndexTagTerms) TraceDebug(span opentracing.Span) {
}

type IndexTagDelSeries struct {
	OrgId uint32   `json:"orgId" binding:"Required"`
	Paths []string `json:"path" form:"path"`
}

func (t IndexTagDelSeries) Trace(span opentracing.Span) {
	span.SetTag("orgId", t.OrgId)
	span.LogFields(traceLog.String("paths", fmt.Sprintf("%q", t.Paths)))
}

func (i IndexTagDelSeries) TraceDebug(span opentracing.Span) {
}

type IndexTagDelByQuery struct {
	OrgId     uint32   `json:"orgId" binding:"Required"`
	Expr      []string `json:"expressions" binding:"Required"`
	OlderThan int64    `json:"olderThan" form:"olderThan"`
	Execute   bool     `json:"execute" binding:"Default(false)"`
	Method    string   `json:"method" binding:"Default(delete);OmitEmpty;In(delete,archive)"`
}

func (t IndexTagDelByQuery) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.String("expressions", fmt.Sprintf("%q", t.Expr)),
		traceLog.String("olderThan", fmt.Sprintf("%d", t.OlderThan)),
		traceLog.String("execute", fmt.Sprintf("%t", t.Execute)),
		traceLog.String("method", fmt.Sprintf("%s", t.Method)),
	)
}

func (t IndexTagDelByQuery) TraceDebug(span opentracing.Span) {
}

type IndexGet struct {
	MKey schema.MKey `json:"id" form:"id" binding:"Required"`
}

type IndexFind struct {
	Patterns []string `json:"patterns" form:"patterns" binding:"Required"`
	OrgId    uint32   `json:"orgId" form:"orgId" binding:"Required"`
	From     int64    `json:"from" form:"from"`
	Limit    int64    `json:"limit"`
}

func (i IndexFind) Trace(span opentracing.Span) {
	span.SetTag("orgId", i.OrgId)
	span.LogFields(
		traceLog.Int64("from", i.From),
		traceLog.Int64("limit", i.Limit),
		traceLog.String("q", fmt.Sprintf("%q", i.Patterns)),
	)
}

func (i IndexFind) TraceDebug(span opentracing.Span) {
}

type GetData struct {
	Requests []Req `json:"requests" binding:"Required"`
}

func (g GetData) Trace(span opentracing.Span) {
	span.LogFields(traceLog.Int("num_reqs", len(g.Requests)))
}

func (g GetData) TraceDebug(span opentracing.Span) {
	// max span size is 64kB. anything higher will be discarded
	tolog := g.Requests
	if len(g.Requests) > 45 {
		span.SetTag("udp_cutoff", true)
		tolog = g.Requests[:45]
	}
	for _, r := range tolog {
		r.TraceLog(span)
	}
}

type IndexDelete struct {
	Query string `json:"query" form:"query" binding:"Required"`
	OrgId uint32 `json:"orgId" form:"orgId" binding:"Required"`
}

func (i IndexDelete) Trace(span opentracing.Span) {
	span.SetTag("orgId", i.OrgId)
	span.LogFields(traceLog.String("q", i.Query))
}

func (i IndexDelete) TraceDebug(span opentracing.Span) {
}
