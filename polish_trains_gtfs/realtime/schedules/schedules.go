// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package schedules

import (
	"iter"
	"slices"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/set"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
)

type FeedDates struct {
	Start, End time2.Date
}

func (fd FeedDates) Contains(d time2.Date) bool {
	return (fd.Start == d || d.After(fd.Start)) && (fd.End == d || d.Before(fd.End))
}

type TripID struct {
	ScheduleID   int
	OrderID      int
	PLKStartDate time2.Date
}

type NumberID struct {
	AgencyID     string
	Number       string
	PLKStartDate time2.Date
}

type Trip struct {
	AgencyID      string
	GTFSStartDate time2.Date
	PLKStartDate  time2.Date
	Numbers       []string
	StopTimes     []StopTime
}

func (t *Trip) GetTripIDs() (ids []string) {
	for _, st := range t.StopTimes {
		if !slices.Contains(ids, st.GTFSTripID) {
			ids = append(ids, st.GTFSTripID)
		}
	}
	return
}

func (t *Trip) GetNumberIDs() iter.Seq[NumberID] {
	return func(yield func(NumberID) bool) {
		for _, n := range t.Numbers {
			id := NumberID{t.AgencyID, n, t.PLKStartDate}
			if !yield(id) {
				return
			}
		}
	}
}

func (t *Trip) GetStopIDs() (ids set.Set[string]) {
	ids = make(set.Set[string], len(t.StopTimes))
	for _, st := range t.StopTimes {
		ids.Add(st.StopID)
	}
	return
}

type StopTime struct {
	GTFSTripID   string
	StopID       string
	GTFSSequence int
	PLKSequence  int
}

type Package struct {
	Dates                 FeedDates
	Stops                 map[string]string
	Trips                 map[TripID]*Trip
	TripsByNumber         map[NumberID]*Trip
	AlternativeTripLookup map[TripID]NumberID
}

func (p *Package) RebuidNumberIndex() {
	if p.TripsByNumber == nil {
		p.TripsByNumber = make(map[NumberID]*Trip)
	} else {
		clear(p.TripsByNumber)
	}

	for _, trip := range p.Trips {
		for number := range trip.GetNumberIDs() {
			_, exists := p.TripsByNumber[number]
			if exists {
				// If `number` is not unique, set its value to `nil` in the lookup table.
				// This prevents the key from being used during matching, but also
				// makes further duplicates are also not remembered (which would happen with `delete`).
				p.TripsByNumber[number] = nil
			} else {
				p.TripsByNumber[number] = trip
			}
		}
	}
}
