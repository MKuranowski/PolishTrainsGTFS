// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package schedules

import (
	"slices"

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

type Trip struct {
	GTFSStartDate time2.Date
	AgencyID      string
	Number        string
	StopTimes     []StopTime
}

func (t Trip) GetTripIDs() (ids []string) {
	for _, st := range t.StopTimes {
		if !slices.Contains(ids, st.GTFSTripID) {
			ids = append(ids, st.GTFSTripID)
		}
	}
	return
}

func (t Trip) GetStopIDs() (ids []string) {
	ids = make([]string, len(t.StopTimes))
	for i, st := range t.StopTimes {
		ids[i] = st.StopID
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
	Dates FeedDates
	Stops map[string]string
	Trips map[TripID]*Trip
}
