// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package match

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
)

func TripUpdates(real *source.Operations, static *schedules.Package) *fact.Container {
	c := &fact.Container{
		Timestamp:   real.Timestamp,
		TripUpdates: make([]*fact.TripUpdate, 0, len(real.Trains)),
	}
	for _, t := range real.Trains {
		if u := TripUpdate(t, static); u != nil {
			c.TripUpdates = append(c.TripUpdates, u...)
		}
	}
	return c
}

func TripUpdate(real *source.OperationTrain, static *schedules.Package) []*fact.TripUpdate {
	trip := Trip(real.TrainID, static)
	if trip == nil {
		return nil
	}
	tripIDs := trip.GetTripIDs()

	if isEntireTripCancelled(real) {
		updates := make([]*fact.TripUpdate, len(tripIDs))
		for i, tripID := range tripIDs {
			updates[i] = &fact.TripUpdate{
				ID:           fmt.Sprintf("U_%s_%s", trip.GTFSStartDate, tripID),
				TripSelector: fact.TripSelector{TripID: tripID, GTFSStartDate: trip.GTFSStartDate},
				Cancelled:    true,
			}
		}
		return updates
	}

	slices.SortFunc(real.Stops, func(a, b *source.OperationTrainStop) int { return cmp.Compare(a.ActualSequence, b.ActualSequence) })

	if isOnDetour(real, trip, static.Stops) {
		updates := make([]*fact.TripUpdate, len(tripIDs))
		for i, tripID := range tripIDs {
			updates[i] = new(fact.TripUpdate)
			updates[i].ID = fmt.Sprintf("U_%s_%s", trip.GTFSStartDate, tripID)
			updates[i].TripSelector = fact.TripSelector{TripID: tripID, GTFSStartDate: trip.GTFSStartDate}

			if i == 0 {
				updates[i].Detour = true
				updates[i].StopTimes = getDetourStopTimeUpdates(real.Stops, static.Stops)
			} else {
				updates[i].Cancelled = true
			}
		}

		return updates
	}

	// Build lookup tables for updates
	realStopByPLKSequence := make(map[int]*source.OperationTrainStop, len(real.Stops))
	for _, stop := range real.Stops {
		realStopByPLKSequence[stop.PlannedSequence] = stop
	}
	updateIndexByTripID := make(map[string]int, len(tripIDs))
	updates := make([]*fact.TripUpdate, len(tripIDs))
	for i, tripID := range tripIDs {
		updateIndexByTripID[tripID] = i
		updates[i] = &fact.TripUpdate{
			ID:           fmt.Sprintf("U_%s_%s", trip.GTFSStartDate, tripID),
			TripSelector: fact.TripSelector{TripID: tripID, GTFSStartDate: trip.GTFSStartDate},
		}
	}

	// Generate stop-time updates
	for _, st := range trip.StopTimes {
		i, ok := updateIndexByTripID[st.GTFSTripID]
		if !ok {
			panic("schedules.Trip.GetTripIDs did not return all trip ids")
		}

		realUpdate := realStopByPLKSequence[st.PLKSequence]
		if realUpdate == nil {
			continue
		}

		update := &fact.StopTimeUpdate{Sequence: st.GTFSSequence}
		if realUpdate.Cancelled {
			update.Cancelled = true
		} else {
			update.Confirmed = realUpdate.Confirmed
			update.Arrival = time.Time(realUpdate.LiveArrival)
			update.Departure = time.Time(realUpdate.LiveDeparture)
		}
		updates[i].StopTimes = append(updates[i].StopTimes, update)
	}

	return updates
}

func isEntireTripCancelled(real *source.OperationTrain) bool {
	if len(real.Stops) == 0 {
		return real.Status == "X"
	}
	for _, s := range real.Stops {
		if !s.Cancelled {
			return false
		}
	}
	return true
}

func isOnDetour(real *source.OperationTrain, trip *schedules.Trip, canonicalStops map[string]string) bool {
	// Train is on detour if one of its real stops is not on the trip.
	// We permit opposite (scheduled stop not in real), as that indicates lack of realtime data.
	scheduledStops := trip.GetStopIDs()
	for stopID := range getAllRealStopIDs(real.Stops, canonicalStops) {
		if _, isScheduled := scheduledStops[stopID]; !isScheduled {
			return true
		}
	}
	return false
}

func getAllRealStopIDs(stops []*source.OperationTrainStop, canonicalStops map[string]string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, stop := range stops {
			stopID := canonicalStops[strconv.Itoa(stop.StopID)]
			if stopID == "" {
				continue
			}
			if !yield(stopID) {
				return
			}
		}
	}
}

func getDetourStopTimeUpdates(stops []*source.OperationTrainStop, canonicalStops map[string]string) []*fact.StopTimeUpdate {
	updates := make([]*fact.StopTimeUpdate, 0, len(stops))
	idx := 0

	for _, stop := range stops {
		stopID := canonicalStops[strconv.Itoa(stop.StopID)]
		if stopID != "" && !stop.Cancelled {
			updates = append(updates, &fact.StopTimeUpdate{
				Sequence:  idx,
				StopID:    stopID,
				Arrival:   time.Time(stop.LiveArrival),
				Departure: time.Time(stop.LiveDeparture),
				Confirmed: stop.Confirmed,
			})
			idx++
		}
	}

	return updates
}
