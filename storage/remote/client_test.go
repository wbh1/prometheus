// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/util/testutil"
)

var longErrMessage = strings.Repeat("error message", maxErrMsgLen)

func TestStoreHTTPErrorHandling(t *testing.T) {
	tests := []struct {
		code int
		err  error
	}{
		{
			code: 200,
			err:  nil,
		},
		{
			code: 300,
			err:  errors.New("server returned HTTP status 300 Multiple Choices: " + longErrMessage[:maxErrMsgLen]),
		},
		{
			code: 404,
			err:  errors.New("server returned HTTP status 404 Not Found: " + longErrMessage[:maxErrMsgLen]),
		},
		{
			code: 500,
			err:  recoverableError{errors.New("server returned HTTP status 500 Internal Server Error: " + longErrMessage[:maxErrMsgLen])},
		},
	}

	for i, test := range tests {
		server := httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, longErrMessage, test.code)
			}),
		)

		serverURL, err := url.Parse(server.URL)
		testutil.Ok(t, err)

		conf := &ClientConfig{
			URL:     &config_util.URL{URL: serverURL},
			Timeout: model.Duration(time.Second),
		}

		hash, err := toHash(conf)
		testutil.Ok(t, err)
		c, err := NewStorageClient(hash, conf)
		testutil.Ok(t, err)

		err = c.Store(context.Background(), []byte{})
		testutil.ErrorEqual(t, err, test.err, "unexpected error in test %d", i)

		server.Close()
	}
}

func TestReadQueryMetric(t *testing.T) {
	var (
		i      = int64(0)
		wait   = make(chan struct{})
		q      = &prompb.Query{}
		server = httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch atomic.AddInt64(&i, 1) {
				case 1:
					w.WriteHeader(http.StatusOK)
				case 2:
					http.Error(w, "error!", http.StatusInternalServerError)
				case 3:
					w.WriteHeader(http.StatusOK)
				default:
					<-wait
				}

			}),
		)
	)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	testutil.Ok(t, err)

	conf := &ClientConfig{
		URL:     &config_util.URL{URL: serverURL},
		Timeout: model.Duration(time.Second),
	}
	reg := prometheus.NewRegistry()
	c1, err := newReadClient(reg, "foo", conf)
	testutil.Ok(t, err)
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c1.(*client).readQueries))

	c2, err := newReadClient(reg, "bar", conf)
	testutil.Ok(t, err)
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c2.(*client).readQueries))

	_, err = c1.Read(context.Background(), q)
	testutil.ErrorEqual(t, errors.New("error reading response: snappy: corrupt input"), err)
	_, err = c1.Read(context.Background(), q)
	testutil.ErrorEqual(t, errors.Errorf("remote server %s returned HTTP status 500 Internal Server Error: error!", serverURL.String()), err)
	_, err = c2.Read(context.Background(), q)
	testutil.ErrorEqual(t, errors.New("error reading response: snappy: corrupt input"), err)

	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c1.(*client).readQueries))
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c2.(*client).readQueries))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	defer wg.Wait()
	defer close(wait)

	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err = c1.Read(ctx, q)
		testutil.ErrorEqual(t, errors.New("error reading response: snappy: corrupt input"), err)
	}()
	for ctx.Err() == nil && atomic.LoadInt64(&i) < 4 {
	}

	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c1.(*client).readQueries))
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c2.(*client).readQueries))

	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err = c2.Read(ctx, q)
		testutil.ErrorEqual(t, errors.New("error reading response: snappy: corrupt input"), err)
	}()
	for ctx.Err() == nil && atomic.LoadInt64(&i) < 5 {
	}

	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c1.(*client).readQueries))
	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c2.(*client).readQueries))

	// Add third client which will cause already exist metric err which should be handled and allow reusing same metric.
	sameAsC2, err := newReadClient(reg, "bar", conf)
	testutil.Ok(t, err)
	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(sameAsC2.(*client).readQueries))

	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err = sameAsC2.Read(ctx, q)
		testutil.ErrorEqual(t, errors.New("error reading response: snappy: corrupt input"), err)
	}()
	for ctx.Err() == nil && atomic.LoadInt64(&i) < 6 {
	}

	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c1.(*client).readQueries))
	testutil.Equals(t, 2.0, prom_testutil.ToFloat64(c2.(*client).readQueries))
	testutil.Equals(t, 2.0, prom_testutil.ToFloat64(sameAsC2.(*client).readQueries))
}
