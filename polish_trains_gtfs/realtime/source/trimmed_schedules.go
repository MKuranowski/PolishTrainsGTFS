// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package source

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
)

type TrimmedSchedules struct {
	Timestamp time.Time       `json:"ts"`
	Trains    []*TrimmedRoute `json:"rt"`
}

type TrimmedRoute struct {
	ScheduleID             int          `json:"sid"`
	OrderID                int          `json:"oid"`
	TrainOrderID           int          `json:"toid"`
	OperatingDates         []time2.Date `json:"od"`
	CarrierCode            string       `json:"cc"`
	Number                 string       `json:"nn"`
	InternationalDepNumber string       `json:"idn"`
	InternationalArrNumber string       `json:"ian"`
}

func (r *TrimmedRoute) TrainIDs() []TrainID {
	ids := make([]TrainID, len(r.OperatingDates))
	for i, od := range r.OperatingDates {
		ids[i] = TrainID{r.ScheduleID, r.OrderID, r.TrainOrderID, od}
	}
	return ids
}

func (r *TrimmedRoute) GetNumber() string {
	if r.Number != "" {
		return r.Number
	} else if r.InternationalDepNumber != "" {
		return r.InternationalDepNumber
	}
	return r.InternationalArrNumber
}

func FetchTrimmedSchedules(ctx context.Context, apikey string, client http2.Doer, startDate, endDate time2.Date) (*TrimmedSchedules, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://pdp-api.plk-sa.pl/api/v1/schedules/shortened", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Api-Key", apikey)
	req.URL.RawQuery = url.Values{
		"dateFrom":        {startDate.String()},
		"dateTo":          {endDate.String()},
		"carriersExclude": {"WKD"},
	}.Encode()

	return http2.GetJSON[TrimmedSchedules](client, req)
}
