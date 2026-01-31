// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package http2

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"time"
)

// Doer abstracts any object which can "Do" a [http.Request], such as a [http.Client].
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// RateLimitedDoer limits another [Doer] to only run requests at most once every Period.
type RateLimitedDoer struct {
	Parent  Doer
	Period  time.Duration
	NextRun time.Time
}

func NewRateLimitedDoer(parent Doer, period time.Duration) *RateLimitedDoer {
	if parent == nil {
		parent = http.DefaultClient
	}
	return &RateLimitedDoer{Parent: parent, Period: period}
}

func (d *RateLimitedDoer) Do(req *http.Request) (*http.Response, error) {
	sleepDuration := time.Until(d.NextRun)
	if sleepDuration > 0 {
		time.Sleep(sleepDuration)
	}

	d.NextRun = time.Now().Add(d.Period)
	return d.Parent.Do(req)
}

// RandomDoer runs a request by pseud-randomly choosing another [Doer].
type RandomDoer []Doer

func (d RandomDoer) Do(req *http.Request) (*http.Response, error) {
	idx := rand.IntN(len(d))
	return d[idx].Do(req)
}

type Error struct {
	URL, Status string
	StatusCode  int
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.URL, e.Status)
}

func Check(r *http.Response) error {
	if r.StatusCode >= 400 && r.StatusCode < 600 {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		return &Error{
			URL:        r.Request.URL.Redacted(),
			Status:     r.Status,
			StatusCode: r.StatusCode,
		}
	}
	return nil
}

func GetJSON[T any](client Doer, req *http.Request) (content *T, err error) {
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	} else if err = Check(resp); err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&content)
	return
}
