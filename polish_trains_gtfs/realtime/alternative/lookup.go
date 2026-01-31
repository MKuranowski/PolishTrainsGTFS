// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package alternative

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
)

type LookupReloader interface {
	Reload(context.Context, *schedules.Package, string, http2.Doer) error
}

type NopLookupReloader struct{}

func (NopLookupReloader) Reload(context.Context, *schedules.Package, string, http2.Doer) error {
	return nil
}

type UnconditionalLookupReloader struct{}

func (UnconditionalLookupReloader) Reload(ctx context.Context, static *schedules.Package, apikey string, client http2.Doer) error {
	slog.Info("Reloading alternative trip lookup table")

	today := time2.Today()
	startDate := today.Previous()
	endDate := today.Next()

	trains, err := source.FetchTrimmedSchedules(ctx, apikey, client, startDate, endDate)
	if err != nil {
		return err
	}

	if static.AlternativeTripLookup == nil {
		static.AlternativeTripLookup = make(map[schedules.TripID]schedules.NumberID)
	} else {
		clear(static.AlternativeTripLookup)
	}

	for _, train := range trains.Trains {
		agencyID := CarrierCodeToAgencyID(train.CarrierCode)

		number := train.GetNumber()
		if number == "" {
			continue
		}

		for _, plkStartDate := range train.OperatingDates {
			tripID := schedules.TripID{
				ScheduleID:   train.ScheduleID,
				OrderID:      train.OrderID,
				PLKStartDate: plkStartDate,
			}
			numberID := schedules.NumberID{
				AgencyID:     agencyID,
				Number:       number,
				PLKStartDate: plkStartDate,
			}
			static.AlternativeTripLookup[tripID] = numberID
		}
	}

	return nil
}

type TimeLimitedLookupReloader struct {
	Wrapped LookupReloader
	Period  time.Duration
	lastRun time.Time
}

func (r *TimeLimitedLookupReloader) Reload(ctx context.Context, static *schedules.Package, apikey string, client http2.Doer) error {
	if time.Since(r.lastRun) < r.Period {
		return nil
	}
	startTime := time.Now()
	err := r.Wrapped.Reload(ctx, static, apikey, client)
	if err == nil {
		r.lastRun = startTime
	}
	return err
}

func CarrierCodeToAgencyID(cc string) string {
	cc = strings.TrimSpace(cc)
	switch cc {
	case "KMŁ":
		return "KML"
	case "Leo Express":
		return "LEO"
	case "ŁKA":
		return "LKA"
	default:
		return cc
	}
}
