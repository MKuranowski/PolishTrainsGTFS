// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package match

import (
	"fmt"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
)

func Alerts(real *source.Disruptions, static *schedules.Package, stats *Stats) *fact.Container {
	c := &fact.Container{
		Timestamp: real.Timestamp,
		Alerts:    make([]*fact.Alert, 0, len(real.Disruptions)),
	}
	for _, d := range real.Disruptions {
		if a := Alert(d, static, stats); a != nil {
			c.Alerts = append(c.Alerts, a)
		}
	}
	return c
}

func Alert(real *source.Disruption, static *schedules.Package, stats *Stats) *fact.Alert {
	// Try to match the trains
	trips := make([]fact.TripSelector, 0, len(real.AffectedTrains))
	for _, train := range real.AffectedTrains {
		selectors := TripSelectors(train.TrainID, static)
		trips = append(trips, selectors...)

		if stats != nil {
			if len(selectors) > 0 {
				stats.Matched++
			} else if !static.Dates.Contains(train.OperatingDate) {
				stats.OutsideFeedDates++
			} else {
				stats.Unmatched++
			}
		}
	}

	// Bail out when no trains match
	if len(trips) == 0 {
		return nil
	}

	// Convert the alert
	return &fact.Alert{
		ID:      fmt.Sprintf("A_%d", real.ID),
		Title:   real.Title,
		Message: real.Message,
		Trips:   trips,
	}
}
