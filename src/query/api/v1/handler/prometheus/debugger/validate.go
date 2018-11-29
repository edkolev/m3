// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/debug"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// PromDebugURL is the url for the Prom and m3query debugging tool
	PromDebugURL = handler.RoutePrefixV1 + "/debug/validate_query"

	// PromDebugHTTPMethod is the HTTP method used with this resource.
	PromDebugHTTPMethod = http.MethodPost
)

// PromDebugHandler represents a handler for prometheus debug endpoint.
type PromDebugHandler struct {
	engine      *executor.Engine
	scope       tally.Scope
	readHandler *native.PromReadHandler
}

// NewPromDebugHandler returns a new instance of handler.
func NewPromDebugHandler(
	h *native.PromReadHandler,
	scope tally.Scope,
) *PromDebugHandler {
	return &PromDebugHandler{
		scope:       scope,
		readHandler: h,
	}
}

func (h *PromDebugHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Error("unable to read data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	var promDebug prometheus.PromDebug
	if err := json.Unmarshal(data, &promDebug); err != nil {
		logger.Error("unable to unmarshal data", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	// fmt.Println(promDebug.Input)
	s := debug.NewStorage(promDebug.Input)
	engine := executor.NewEngine(s, h.scope.SubScope("debug_engine"))
	h.engine = engine
	results, _, err := h.readHandler.ServeHTTPWithEngine(w, r, h.engine)
	if err != nil {
		fmt.Println("error: ", err)
	}

	promResults := debug.DpToTS(promDebug.Results, models.NewTagOptions())

	for _, res := range results {
		fmt.Println(res.Values().Datapoints())
	}

	equal := validate(promResults, results)

	fmt.Println("equal: ", equal)
}

func validate(prom, m3 []*ts.Series) bool {
	// check to make sure number of series returned is the same
	if len(prom) != len(m3) {
		return false
	}

	for i, promSeries := range prom {
		promdps := promSeries.Values().Datapoints()
		m3dps := m3[i].Values().Datapoints()

		m3idx := 0
		for _, promdp := range promdps {
			if math.IsNaN(promdp.Value) && !math.IsNaN(m3dps[m3idx].Value) {
				return false
			}

			for {
				if !math.IsNaN(m3dps[m3idx].Value) {
					break
				}

				m3idx++
			}

			if promdp.Value != m3dps[m3idx].Value {
				fmt.Println(promdp.Value, promdp.Timestamp, m3dps[m3idx].Value, m3dps[m3idx].Timestamp)
				return false
			}
		}
	}

	return true
}
