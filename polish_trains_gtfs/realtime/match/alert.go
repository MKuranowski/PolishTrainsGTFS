// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package match

import (
	"fmt"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
)

func Alerts(real *source.Disruptions, static *schedules.Package) *fact.Container {
	c := &fact.Container{
		Timestamp: real.Timestamp,
		Alerts:    make([]*fact.Alert, 0, len(real.Disruptions)),
	}
	for _, d := range real.Disruptions {
		if a := Alert(d, static); a != nil {
			c.Alerts = append(c.Alerts, a)
		}
	}
	return c
}

func Alert(real *source.Disruption, static *schedules.Package) *fact.Alert {
	// Try to match the trains
	trips := make([]fact.TripSelector, 0, len(real.AffectedTrains))
	for _, train := range real.AffectedTrains {
		trips = append(trips, TripSelectors(train.TrainID, static)...)
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
