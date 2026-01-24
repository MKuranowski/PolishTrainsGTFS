// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package schedules

import (
	"archive/zip"
	"cmp"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/mcsv"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
)

var (
	startDateOffsetRegex = regexp.MustCompile(`([+-][0-9]+)D$`)
	plkTripIdRegex       = regexp.MustCompile(`^PLK_([A-Za-z0-9]+)_([0-9]+)_([0-9]+)`)
	trainNumberRegex     = regexp.MustCompile(`^[0-9]{3,6}(?:/[0-9])?`)
)

type DatePair struct {
	GTFSDate time2.Date
	PLKDate  time2.Date
}

type ErrGTFSInvalidValue struct {
	File, Column string
	Line         int
	Reason       error
}

func (e ErrGTFSInvalidValue) Error() string {
	if e.Reason == nil {
		return fmt.Sprintf("%s:%d: invalid %s", e.File, e.Line, e.Column)
	}
	return fmt.Sprintf("%s:%d: invalid %s: %s", e.File, e.Line, e.Column, e.Reason)
}

func (e ErrGTFSInvalidValue) Unwrap() error {
	return e.Reason
}

type ErrGTFSInvalidPLKTripID string

func (e ErrGTFSInvalidPLKTripID) Error() string {
	return fmt.Sprintf("failed to extract agency, scheduleId and orderId from trip_id %q", string(e))
}

func LoadGTFSFromPath(path string) (*Package, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		return LoadGTFS(os.DirFS(path))
	}

	arch, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}

	return LoadGTFS(arch)
}

func LoadGTFS(gtfs fs.FS) (p *Package, err error) {
	p = new(Package)

	// 1. Load feed_info.txt
	{
		var f fs.File
		f, err = gtfs.Open("feed_info.txt")
		if err != nil {
			return nil, err
		}
		defer f.Close()
		p.Dates, err = LoadGTFSFeedDates(f)
		if err != nil {
			return nil, err
		}
	}

	// 2. Load stops.txt
	{
		var f fs.File
		f, err = gtfs.Open("stops.txt")
		if err != nil {
			return nil, err
		}
		defer f.Close()
		p.Stops, err = LoadGTFSStops(f)
		if err != nil {
			return nil, err
		}
	}

	// 3. Load calendar_dates.txt
	var services map[string][]DatePair
	{
		var f fs.File
		f, err = gtfs.Open("calendar_dates.txt")
		if err != nil {
			return nil, err
		}
		defer f.Close()
		services, err = LoadGTFSServices(f, p.Dates)
		if err != nil {
			return nil, err
		}
	}

	// 4. Load trips.txt
	var tripIDs map[string][]TripID
	{
		var f fs.File
		f, err = gtfs.Open("trips.txt")
		if err != nil {
			return nil, err
		}
		defer f.Close()
		tripIDs, p.Trips, err = LoadGTFSTrips(f, services)
		if err != nil {
			return nil, err
		}
	}

	// 5. Load stop_times.txt
	{
		var f fs.File
		f, err = gtfs.Open("stop_times.txt")
		if err != nil {
			return nil, err
		}
		defer f.Close()
		err = LoadGTFSStopTimes(f, tripIDs, p.Trips, p.Stops)
		if err != nil {
			return nil, err
		}
	}

	return
}

func LoadGTFSFeedDates(feedInfo io.Reader) (d FeedDates, err error) {
	r := mcsv.NewReader(feedInfo)
	row, err := r.Read()
	if errors.Is(err, io.EOF) {
		return FeedDates{}, ErrGTFSInvalidValue{"feed_info.txt", "feed_start_date", 1, nil}
	} else if err != nil {
		return FeedDates{}, fmt.Errorf("feed_info.txt: %w", err)
	}

	err = d.Start.UnmarshalText([]byte(row["feed_start_date"]))
	if err != nil {
		return FeedDates{}, ErrGTFSInvalidValue{"feed_info.txt", "feed_start_date", 2, err}
	}
	// feed_start_date is the first "full" date, but we also want to match trips
	// starting on the previous day. They are included in the GTFS.
	d.Start = d.Start.Previous()

	err = d.Start.UnmarshalText([]byte(row["feed_end_date"]))
	if err != nil {
		return FeedDates{}, ErrGTFSInvalidValue{"feed_info.txt", "feed_end_date", 2, err}
	}

	return
}

func LoadGTFSStops(stops io.Reader) (map[string]string, error) {
	// First, load stops.txt and load viable ids
	allIDs := make(map[string][]string)
	secondaryIDs := make(map[string]string)

	r := mcsv.NewReader(stops)
	for row := range r.Iter() {
		stopID := row["stop_id"]
		if stopID == "" {
			return nil, ErrGTFSInvalidValue{"stops.txt", "stop_id", r.Line(), nil}
		}

		stationID, _, _ := strings.Cut(stopID, "_")
		allIDs[stationID] = append(allIDs[stationID], stopID)

		secondaryID := row["plk_secondary_id"]
		if secondaryID != "" {
			secondaryIDs[secondaryID] = stationID
		}
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("stops.txt: %w", err)
	}

	// Second, for each station, pick a canonical GTFS stop_id.
	// The canonical stop_id will be used for comparing if stops are the same,
	// and will be used by diverted trains.
	canonicalIDs := make(map[string]string, len(allIDs)+len(secondaryIDs))
	for stationID, usedIDs := range allIDs {
		canonicalID := pickCanonicalStopID(usedIDs)

		canonicalIDs[stationID] = canonicalID
		for _, alternativeID := range usedIDs {
			canonicalIDs[alternativeID] = canonicalID
		}
	}

	// Third, map secondary IDs to canonical IDs
	for secondaryID, stationID := range secondaryIDs {
		canonicalIDs[secondaryID] = canonicalIDs[stationID]
	}

	return canonicalIDs, nil
}

func pickCanonicalStopID(used []string) string {
	return slices.MaxFunc(used, func(a, b string) int { return cmp.Compare(rankStopID(a), rankStopID(b)) })
}

func rankStopID(id string) int {
	if strings.HasSuffix(id, "_RAIL") {
		return 3
	} else if strings.HasSuffix(id, "_BUS") {
		return 2
	} else if strings.Contains(id, "_BUS_") {
		return 1
	}
	return 0
}

func LoadGTFSServices(calendarDates io.Reader, period FeedDates) (map[string][]DatePair, error) {
	d := make(map[string][]DatePair)

	r := mcsv.NewReader(calendarDates)
	for row := range r.Iter() {
		if row["exception_type"] != "1" {
			panic("GTFS calendar_dates.txt removes dates. This indicates usage of calendar.txt, which is unsupported.")
		}

		id := row["service_id"]
		if id == "" {
			return nil, ErrGTFSInvalidValue{"calendar_dates.txt", "service_id", r.Line(), nil}
		}

		var gtfsDate time2.Date
		err := gtfsDate.UnmarshalText([]byte(row["date"]))
		if err != nil {
			return nil, ErrGTFSInvalidValue{"calendar_dates.txt", "date", r.Line(), err}
		}

		if period.Contains(gtfsDate) {
			gtfsOffset := extractStartDateOffset(id)
			plkDate := gtfsDate.Shifted(-gtfsOffset)
			d[id] = append(d[id], DatePair{gtfsDate, plkDate})
		}
	}

	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("calendar_dates.txt: %w", err)
	}
	return d, nil
}

func extractStartDateOffset(id string) int {
	m := startDateOffsetRegex.FindStringSubmatch(id)
	if m == nil {
		return 0
	}

	offset, _ := strconv.Atoi(m[1])
	return offset
}

func LoadGTFSTrips(trips io.Reader, services map[string][]DatePair) (map[string][]TripID, map[TripID]*Trip, error) {
	tripIDs := make(map[string][]TripID)
	tripObjects := make(map[TripID]*Trip)

	r := mcsv.NewReader(trips)
	for row := range r.Iter() {
		gtfsID := row["trip_id"]
		if gtfsID == "" {
			return nil, nil, ErrGTFSInvalidValue{"trips.txt", "trip_id", r.Line(), nil}
		} else if !strings.HasPrefix(gtfsID, "PLK_") {
			continue
		}

		serviceID := row["service_id"]
		if serviceID == "" {
			return nil, nil, ErrGTFSInvalidValue{"trips.txt", "service_id", r.Line(), nil}
		}

		agencyID, scheduleID, orderID, err := parseTripID(gtfsID)
		if err != nil {
			return nil, nil, ErrGTFSInvalidValue{"trips.txt", "trip_id", r.Line(), err}
		}

		for _, dp := range services[serviceID] {
			tripID := TripID{scheduleID, orderID, dp.PLKDate}
			tripIDs[gtfsID] = append(tripIDs[gtfsID], tripID)

			tripObjects[tripID] = &Trip{
				GTFSStartDate: dp.GTFSDate,
				AgencyID:      agencyID,
				Number:        extractTrainNumber(row),
			}
		}
	}

	if err := r.Err(); err != nil {
		return nil, nil, fmt.Errorf("trips.txt: %w", err)
	}
	return tripIDs, tripObjects, nil
}

func parseTripID(gtfsID string) (agencyID string, scheduleID int, orderID int, err error) {
	m := plkTripIdRegex.FindStringSubmatch(gtfsID)
	if m == nil {
		err = ErrGTFSInvalidPLKTripID(gtfsID)
		return
	}

	agencyID = m[1]
	scheduleID, _ = strconv.Atoi(m[2])
	orderID, _ = strconv.Atoi(m[3])
	return
}

func extractTrainNumber(tripsRow map[string]string) string {
	m := trainNumberRegex.FindString(tripsRow["trip_short_name"])
	if m == "" {
		return tripsRow["plk_train_number"]
	}
	return m
}

func LoadGTFSStopTimes(stopTimes io.Reader, tripIDs map[string][]TripID, tripObjects map[TripID]*Trip, canonicalStops map[string]string) error {
	r := mcsv.NewReader(stopTimes)
	for row := range r.Iter() {
		var st StopTime

		st.GTFSTripID = row["trip_id"]
		if st.GTFSTripID == "" {
			return ErrGTFSInvalidValue{"stop_times.txt", "trip_id", r.Line(), nil}
		}

		// Ignore non-PLK trips
		if len(tripIDs[st.GTFSTripID]) == 0 {
			continue
		}

		// Get a canonical stop_id
		st.StopID = row["stop_id"]
		if st.StopID == "" {
			return ErrGTFSInvalidValue{"stop_times.txt", "stop_id", r.Line(), nil}
		}
		if override := canonicalStops[st.StopID]; override != "" {
			st.StopID = override
		}

		// Parse gtfs sequence
		var err error
		st.GTFSSequence, err = strconv.Atoi(row["stop_sequence"])
		if err != nil {
			return ErrGTFSInvalidValue{"stop_times.txt", "stop_sequence", r.Line(), err}
		}

		// Parse PLK sequence
		st.PLKSequence, err = strconv.Atoi(row["plk_sequence"])
		if err != nil {
			return ErrGTFSInvalidValue{"stop_times.txt", "plk_sequence", r.Line(), err}
		}

		// Save the stop_time to all possible trips
		for _, tripID := range tripIDs[st.GTFSTripID] {
			tripObjects[tripID].StopTimes = append(tripObjects[tripID].StopTimes, st)
		}
	}

	if err := r.Err(); err != nil {
		return fmt.Errorf("stop_times.txt: %w", err)
	}

	// Ensure all Trip.StopTimes are sorted by sequence
	for _, o := range tripObjects {
		slices.SortFunc(o.StopTimes, func(a, b StopTime) int { return cmp.Compare(a.GTFSSequence, b.GTFSSequence) })
	}

	return nil
}
