// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package source

import (
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
)

type Pagination struct {
	Page         int  `json:"p"`
	PageSize     int  `json:"ps"`
	TotalPages   int  `json:"tp"`
	TotalEntries int  `json:"tc"`
	HasNext      bool `json:"hn"`
	HasPrevious  bool `json:"hp"`
}

type TrainID struct {
	ScheduleID    int        `json:"sid"`
	OrderID       int        `json:"oid"`
	TrainOrderID  int        `json:"toid"`
	OperatingDate time2.Date `json:"od"`
}

type Period struct {
	From time2.Date `json:"f"`
	To   time2.Date `json:"t"`
}
