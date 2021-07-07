package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/stats"
)

var proxyStats graphiteProxyStats

func init() {
	proxyStats = graphiteProxyStats{
		funcMiss: make(map[string]*stats.Counter32),
	}

}

type graphiteProxyStats struct {
	sync.Mutex
	funcMiss map[string]*stats.Counter32
}

func (s *graphiteProxyStats) Miss(fun string) {
	s.Lock()
	counter, ok := s.funcMiss[fun]
	if !ok {
		counter = stats.NewCounter32(fmt.Sprintf("api.request.render.proxy-due-to.%s", fun))
		s.funcMiss[fun] = counter
	}
	s.Unlock()
	counter.Inc()
}

func NewGraphiteProxy(u *url.URL) *httputil.ReverseProxy {
	graphiteProxy := httputil.NewSingleHostReverseProxy(u)

	// workaround for net/http/httputil issue https://github.com/golang/go/issues/28168
	originalDirector := graphiteProxy.Director
	graphiteProxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = req.URL.Host
	}

	// remove these headers from upstream. we will set our own correct ones
	graphiteProxy.ModifyResponse = func(resp *http.Response) error {
		// if kept, would be duplicated. and duplicated headers are illegal)
		resp.Header.Del("access-control-allow-credentials")
		resp.Header.Del("access-control-allow-origin")
		// if kept, would errorously stick around and be invalid because we gzip responses
		resp.Header.Del("content-length")
		return nil
	}

	graphiteProxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		if graphiteProxy.ErrorLog != nil {
			graphiteProxy.ErrorLog.Printf("http: proxy error: %v", err)
		} else {
			log.Printf("http: proxy error: %v", err)
		}

		if req.Context().Err() == context.Canceled {
			// if the client disconnected before the query was fully processed
			rw.WriteHeader(response.HttpClientClosedRequest)
		} else {
			rw.WriteHeader(http.StatusBadGateway)
		}
	}
	return graphiteProxy
}
