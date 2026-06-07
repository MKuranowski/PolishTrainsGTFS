// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package match

import (
	"fmt"
	"strings"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
)

func Alerts(real *source.Disruptions, static *schedules.Package, stats *Stats) *fact.Container {
	c := &fact.Container{
		Schema:    "https://mkuran.pl/gtfs/polish_trains/live.schema.json",
		Timestamp: real.Timestamp.In(time2.PolishTimezone),
		Alerts:    make([]*fact.Alert, 0, len(real.Disruptions)),
	}
	for _, d := range real.Disruptions {
		if a := Alert(d, static, stats, real.DisruptionTypes); a != nil {
			c.Alerts = append(c.Alerts, a)
		}
	}
	return c
}

func Alert(real *source.Disruption, static *schedules.Package, stats *Stats, disruptionTypes map[string]string) *fact.Alert {
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

	// PDP-API uses codes like 'utr_1' in message field when message has no placeholders. Replace them with actual messages.
	if strings.Contains(real.Message, "utr_") {
		real.Message = disruptionTypes[real.Message]
	}

	// Convert the alert
	return &fact.Alert{
		ID:      fmt.Sprintf("A_%d", real.ID),
		Title:   real.Title,
		Message: real.Message,
		Trips:   trips,
	}
}
