// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package source

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
)

const DefaultPageSize = 5000
const DefaultMaxPages = 10
const DefaultFetchSpacing = 100 * time.Millisecond

var ErrTooManyPages = errors.New("fetching operations takes too many pages")

type Operations struct {
	Timestamp time.Time         `json:"ts"`
	Pages     Pagination        `json:"pg"`
	Trains    []*OperationTrain `json:"tr"`
}

type OperationTrain struct {
	TrainID
	Status string                `json:"s"`
	Stops  []*OperationTrainStop `json:"st"`
}

type OperationTrainStop struct {
	StopID          int       `json:"id"`
	PlannedSequence int       `json:"psn"`
	ActualSequence  int       `json:"asn"`
	LiveArrival     time.Time `json:"aa"`
	LiveDeparture   time.Time `json:"ad"`
	Confirmed       bool      `json:"cf"`
	Cancelled       bool      `json:"cn"`
}

type PageFetchOptions struct {
	PageSize     int
	MaxPages     int
	FetchSpacing time.Duration
}

func NewPageFetchOptions() PageFetchOptions {
	return PageFetchOptions{
		PageSize:     DefaultPageSize,
		MaxPages:     DefaultMaxPages,
		FetchSpacing: DefaultFetchSpacing,
	}
}

func FetchOperations(ctx context.Context, apikey string, client *http.Client, options PageFetchOptions) (*Operations, error) {
	var all *Operations
	var nextFetch time.Time

	for page := 1; page <= options.MaxPages; page++ {
		waitFor(ctx, nextFetch)
		o, err := FetchOperationsPage(ctx, apikey, client, page, options.PageSize)
		if err != nil {
			return nil, err
		}

		if all == nil {
			all = o
			all.Pages = Pagination{
				PageSize:     all.Pages.PageSize,
				TotalPages:   all.Pages.TotalPages,
				TotalEntries: all.Pages.TotalEntries,
			}
		} else {
			all.Trains = append(all.Trains, o.Trains...)
		}

		if !o.Pages.HasNext {
			return all, nil
		}
	}
	return nil, ErrTooManyPages
}

func FetchOperationsPage(ctx context.Context, apikey string, client *http.Client, page, pageSize int) (o *Operations, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://pdp-api.plk-sa.pl/api/v1/operations/shortened", nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Api-Key", apikey)
	req.URL.RawQuery = url.Values{
		"page":       {strconv.Itoa(page)},
		"pageSize":   {strconv.Itoa(pageSize)},
		"fullRoutes": {"true"},
	}.Encode()

	return http2.GetJSON[Operations](client, req)
}

func waitFor(ctx context.Context, t time.Time) error {
	duration := time.Until(t)
	if duration <= 0 {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}
