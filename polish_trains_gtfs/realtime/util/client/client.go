// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package client

import (
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
)

const PoolClientBackoff = 30 * time.Minute

type Client struct {
	Key       string
	Closer    func()
	Doer      http2.Doer
	RateLimit time.Duration
	nextRun   time.Time
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if c.RateLimit != 0 {
		sleep := time.Until(c.nextRun)
		if sleep > 0 {
			time.Sleep(sleep)
		}
		c.nextRun = time.Now().Add(c.RateLimit)
	}

	return c.Doer.Do(req)
}

func (c *Client) Close() {
	if c.Closer != nil {
		c.Closer()
	}
}

type Pool struct {
	clients []*Client
	backoff []time.Time
	last    int
}

func NewPool(clients ...*Client) *Pool {
	if len(clients) == 0 {
		panic("client.NewPool: no clients provided")
	}

	return &Pool{
		clients: clients,
		backoff: make([]time.Time, len(clients)),
	}
}

func (p *Pool) Close() {
	for _, c := range p.clients {
		c.Close()
	}
}

func (p *Pool) Select() *Client {
	// Short-circuit when there's only one client
	if len(p.clients) <= 1 {
		return p.clients[0]
	}

	// Try a couple of times to select a non-backoffed client
	now := time.Now()
	for try := 0; try < len(p.clients); try++ {
		idx := rand.IntN(len(p.clients))
		if now.After(p.backoff[idx]) {
			p.last = idx
			return p.clients[idx]
		}
	}

	// Failed to do so - pick a random one
	slog.Warn("Failed to select a non-backoffed client for the request")
	idx := rand.IntN(len(p.clients))
	p.last = idx
	return p.clients[idx]
}

func (p *Pool) BackoffLast() {
	p.backoff[p.last] = time.Now().Add(PoolClientBackoff)
}
