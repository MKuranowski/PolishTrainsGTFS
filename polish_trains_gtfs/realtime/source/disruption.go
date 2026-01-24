// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package source

import (
	"context"
	"net/http"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
)

type Disruptions struct {
	Timestamp   time.Time     `json:"ts"`
	Disruptions []*Disruption `json:"ds"`
}

type Disruption struct {
	ID             int              `json:"id"`
	Type           string           `json:"tc"`
	Title          string           `json:"tt"`
	Message        string           `json:"msg"`
	AffectedTrains []*AffectedTrain `json:"ar"`
}

type AffectedTrain struct {
	TrainID
	StationID int `json:"sid"`
	Sequence  int `json:"seq"`
}

func FetchDisruptions(ctx context.Context, apikey string, client *http.Client) (d *Disruptions, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://pdp-api.plk-sa.pl/api/v1/disruptions/shortened", nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Api-Key", apikey)

	return http2.GetJSON[Disruptions](client, req)
}
