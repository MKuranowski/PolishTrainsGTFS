// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package match

import (
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
)

func Trip(real source.TrainID, static *schedules.Package) *schedules.Trip {
	id := schedules.TripID{
		ScheduleID:   real.ScheduleID,
		OrderID:      real.OrderID,
		PLKStartDate: real.OperatingDate,
	}
	return static.Trips[id]
}

func TripSelectors(real source.TrainID, static *schedules.Package) []fact.TripSelector {
	t := Trip(real, static)
	if t == nil {
		return nil
	}

	tripIDs := t.GetTripIDs()
	selectors := make([]fact.TripSelector, len(tripIDs))
	for i, tripID := range tripIDs {
		selectors[i] = fact.TripSelector{TripID: tripID, GTFSStartDate: t.GTFSStartDate}
	}
	return selectors
}
