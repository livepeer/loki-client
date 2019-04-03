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
package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/livepeer/loki-client/logproto"
	"github.com/livepeer/loki-client/model"
)

const contentType = "application/x-protobuf"
const maxErrMsgLen = 1024

// Config describes configuration for a HTTP pusher client.
type Config struct {
	URL       string
	BatchWait time.Duration
	BatchSize int

	BackoffConfig  BackoffConfig  `yaml:"backoff_config"`
	ExternalLabels model.LabelSet `yaml:"external_labels,omitempty"`
	Timeout        time.Duration  `yaml:"timeout"`
}

// Logger function to send logs of errors to
type Logger func(...interface{})

// Client for pushing logs in snappy-compressed protos over HTTP.
type Client struct {
	logger         Logger
	cfg            Config
	quit           chan struct{}
	entries        chan entry
	wg             sync.WaitGroup
	externalLabels model.LabelSet
	stopLock       sync.Mutex
	stopped        bool
}

type entry struct {
	labels model.LabelSet
	logproto.Entry
}

// NewWithDefaults makes a new Client with default config.
func NewWithDefaults(url string, externalLabels model.LabelSet, logger Logger) (*Client, error) {
	cfg := Config{
		URL:       url,
		BatchWait: time.Second,
		BatchSize: 100 * 1024,
		Timeout:   10 * time.Second,
		BackoffConfig: BackoffConfig{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: 10 * time.Second,
			MaxRetries: 10,
		},
		ExternalLabels: externalLabels,
	}
	return New(cfg, logger)
}

// New makes a new Client.
func New(cfg Config, logger Logger) (*Client, error) {
	c := &Client{
		logger:  logger,
		cfg:     cfg,
		quit:    make(chan struct{}),
		entries: make(chan entry),

		externalLabels: cfg.ExternalLabels,
	}
	c.wg.Add(1)
	go c.run()
	return c, nil
}

func (c *Client) run() {
	batch := map[string]*logproto.Stream{}
	batchSize := 0
	maxWait := time.NewTimer(c.cfg.BatchWait)

	defer func() {
		c.sendBatch(batch)
		c.wg.Done()
	}()

	for {
		maxWait.Reset(c.cfg.BatchWait)
		select {
		case <-c.quit:
			return

		case e := <-c.entries:
			if batchSize+len(e.Line) > c.cfg.BatchSize {
				c.sendBatch(batch)
				batchSize = 0
				batch = map[string]*logproto.Stream{}
			}

			batchSize += len(e.Line)
			fp := e.labels.String()
			c.log("info:", "Labels for stream:", fp)
			stream, ok := batch[fp]
			if !ok {
				stream = &logproto.Stream{
					Labels: fp,
				}
				batch[fp] = stream
			}
			c.log("info:", "Appended entry:", e.Entry.Timestamp, e.Entry.Line)
			stream.Entries = append(stream.Entries, e.Entry)

		case <-maxWait.C:
			if len(batch) > 0 {
				c.sendBatch(batch)
				batchSize = 0
				batch = map[string]*logproto.Stream{}
			}
		}
	}
}

func (c *Client) sendBatch(batch map[string]*logproto.Stream) {
	buf, err := encodeBatch(batch)
	if err != nil {
		c.logError("msg", "error encoding batch", "error", err)
		return
	}

	ctx := context.Background()
	backoff := NewBackoff(ctx, c.cfg.BackoffConfig)
	var status int
	for backoff.Ongoing() {
		status, err = c.send(ctx, buf)
		c.log("info:", "Batch sent with status:", status, "Bytes sent", len(buf))

		if err == nil {
			return
		}

		// Only retry 500s and connection-level errors.
		if status > 0 && status/100 != 5 {
			break
		}

		c.logWarn("msg", "error sending batch, will retry", "status", status, "error", err)
		backoff.Wait()
	}

	if err != nil {
		c.logError("msg", "final error sending batch", "status", status, "error", err)
	}
}

func encodeBatch(batch map[string]*logproto.Stream) ([]byte, error) {
	req := logproto.PushRequest{
		Streams: make([]*logproto.Stream, 0, len(batch)),
	}
	for _, stream := range batch {
		req.Streams = append(req.Streams, stream)
	}
	buf, err := proto.Marshal(&req)
	if err != nil {
		return nil, err
	}
	buf = snappy.Encode(nil, buf)
	return buf, nil
}

func (c *Client) send(ctx context.Context, buf []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequest("POST", c.cfg.URL, bytes.NewReader(buf))
	if err != nil {
		return -1, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", contentType)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s (%d): %s", resp.Status, resp.StatusCode, line)
	}
	return resp.StatusCode, err
}

// Stop the client.
func (c *Client) Stop() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	if c.stopped {
		return
	}
	c.log("info: ", "Loki client waiting for stop")
	c.stopped = true
	close(c.quit)
	c.wg.Wait()
}

// Handle implement EntryHandler; adds a new line to the next batch; send is async.
func (c *Client) Handle(ls model.LabelSet, t time.Time, s string) error {
	if len(c.externalLabels) > 0 {
		ls = c.externalLabels.Merge(ls)
	}

	c.entries <- entry{ls, logproto.Entry{
		Timestamp: t,
		Line:      s,
	}}
	return nil
}

func (c *Client) logError(v ...interface{}) {
	c.log("error: ", v...)
}

func (c *Client) logWarn(v ...interface{}) {
	c.log("warning: ", v...)
}

func (c *Client) log(level string, v ...interface{}) {
	if c.logger != nil {
		v2 := make([]interface{}, 0, len(v)+1)
		v2 = append(v2, level)
		v2 = append(v2, v...)
		c.logger(v2)
	}
}
