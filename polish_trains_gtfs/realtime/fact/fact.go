// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package fact

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

const (
	Binary        = false
	HumanReadable = true
)

type Container struct {
	Timestamp   time.Time     `json:"timestamp"`
	Alerts      []*Alert      `json:"alerts,omitempty"`
	TripUpdates []*TripUpdate `json:"trip_updates,omitempty"`
}

func (c *Container) AsGTFS() *gtfs.FeedMessage {
	g := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{
			GtfsRealtimeVersion: ptr("2.0"),
			Timestamp:           ptr(uint64(c.Timestamp.Unix())),
		},
	}

	g.Entity = make([]*gtfs.FeedEntity, 0, len(c.Alerts)+len(c.TripUpdates))
	for _, a := range c.Alerts {
		g.Entity = append(g.Entity, a.AsGTFS())
	}
	for _, u := range c.TripUpdates {
		g.Entity = append(g.Entity, u.AsGTFS())
	}

	return g
}

func (c *Container) DumpJSON(w io.Writer, humanReadable bool) error {
	e := json.NewEncoder(w)
	if humanReadable {
		e.SetIndent("", "\t")
	}
	return e.Encode(c)
}

func (c *Container) DumpJSONFile(path string, humanReadable bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	b := bufio.NewWriter(f)
	err = c.DumpJSON(b, humanReadable)
	if err != nil {
		return err
	}

	return b.Flush()
}

func (c *Container) DumpGTFS(w io.Writer, humanReadable bool) error {
	var data []byte
	var err error

	if humanReadable {
		data, err = prototext.Marshal(c.AsGTFS())
	} else {
		data, err = proto.Marshal(c.AsGTFS())
	}

	if err != nil {
		return err
	}

	_, err = io.Copy(w, bytes.NewReader(data))
	return err
}

func (c *Container) DumpGTFSFile(path string, humanReadable bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	b := bufio.NewWriter(f)
	err = c.DumpGTFS(b, humanReadable)
	if err != nil {
		return err
	}

	return b.Flush()
}

type Alert struct {
	ID      string         `json:"id"`
	Title   string         `json:"title"`
	Message string         `json:"message"`
	Trips   []TripSelector `json:"trips"`
}

func (a *Alert) AsGTFS() *gtfs.FeedEntity {
	g := new(gtfs.FeedEntity)
	g.Id = ptr(a.ID)
	g.Alert = new(gtfs.Alert)

	if a.Title != "" {
		g.Alert.HeaderText = translatedString(a.Title)
	}

	if a.Message != "" {
		g.Alert.DescriptionText = translatedString(a.Message)
	}

	g.Alert.InformedEntity = make([]*gtfs.EntitySelector, len(a.Trips))
	for i, ts := range a.Trips {
		g.Alert.InformedEntity[i] = &gtfs.EntitySelector{Trip: ts.AsGTFS()}
	}

	return g
}

type TripUpdate struct {
	ID string `json:"id"`
	TripSelector
	StopTimes []*StopTimeUpdate `json:"stop_times,omitempty"`
	Cancelled bool              `json:"cancelled"`
	Detour    bool              `json:"detour"`
}

func (t *TripUpdate) AsGTFS() *gtfs.FeedEntity {
	g := new(gtfs.FeedEntity)
	g.Id = ptr(t.ID)
	g.TripUpdate = new(gtfs.TripUpdate)

	g.TripUpdate.Trip = t.TripSelector.AsGTFS()
	if t.Cancelled {
		g.TripUpdate.Trip.ScheduleRelationship = ptr(gtfs.TripDescriptor_CANCELED)
	} else {
		if t.Detour {
			g.TripUpdate.Trip.ScheduleRelationship = ptr(gtfs.TripDescriptor_REPLACEMENT)
		} else {
			g.TripUpdate.Trip.ScheduleRelationship = ptr(gtfs.TripDescriptor_SCHEDULED)
		}

		g.TripUpdate.StopTimeUpdate = make([]*gtfs.TripUpdate_StopTimeUpdate, len(t.StopTimes))
		for i, st := range t.StopTimes {
			g.TripUpdate.StopTimeUpdate[i] = st.AsGTFS()
		}
	}
	return g
}

type StopTimeUpdate struct {
	Sequence  int       `json:"stop_sequence"`
	StopID    string    `json:"stop_id,omitempty"`
	Arrival   time.Time `json:"arrival,omitzero"`
	Departure time.Time `json:"departure,omitzero"`
	Cancelled bool      `json:"cancelled"`
	Confirmed bool      `json:"confirmed"`
}

func (s *StopTimeUpdate) AsGTFS() *gtfs.TripUpdate_StopTimeUpdate {
	g := new(gtfs.TripUpdate_StopTimeUpdate)
	g.StopSequence = ptr(uint32(s.Sequence))
	if s.StopID != "" {
		g.StopId = ptr(s.StopID)
	}

	if s.Cancelled {
		g.ScheduleRelationship = ptr(gtfs.TripUpdate_StopTimeUpdate_SKIPPED)
	} else {
		uncertainty := int32(1)
		if s.Confirmed {
			uncertainty = 0
		}

		if !s.Arrival.IsZero() {
			g.Arrival = &gtfs.TripUpdate_StopTimeEvent{
				Time:        ptr(s.Arrival.Unix()),
				Uncertainty: ptr(uncertainty),
			}
		}

		if !s.Departure.IsZero() {
			g.Departure = &gtfs.TripUpdate_StopTimeEvent{
				Time:        ptr(s.Departure.Unix()),
				Uncertainty: ptr(uncertainty),
			}
		}
	}

	return g
}

type TripSelector struct {
	TripID        string     `json:"trip_id"`
	GTFSStartDate time2.Date `json:"start_date"`
}

func (s TripSelector) AsGTFS() *gtfs.TripDescriptor {
	return &gtfs.TripDescriptor{
		TripId:    ptr(s.TripID),
		StartDate: ptr(s.GTFSStartDate.StringSeparator("")),
	}
}

func ptr[T any](thing T) *T {
	return &thing
}

func translatedString(s string) *gtfs.TranslatedString {
	return &gtfs.TranslatedString{
		Translation: []*gtfs.TranslatedString_Translation{
			{
				Text:     ptr(s),
				Language: ptr("pl"),
			},
		},
	}
}
