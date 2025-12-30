# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests

from . import gtfs_realtime_pb2
from .fact import Fact, FactContainer
from .tools import TripDate


@dataclass
class StopDelay:
    stop_id: int
    stop_sequence: int
    cancelled: bool
    confirmed: bool
    live_arrival: datetime
    live_departure: datetime

    def as_json(self) -> Mapping[str, Any]:
        if self.cancelled:
            return {
                "stop_id": str(self.stop_id),
                "stop_sequence": self.stop_sequence,
                "confirmed": self.confirmed,
                "cancelled": True,
            }
        return {
            "stop_id": str(self.stop_id),
            "stop_sequence": self.stop_sequence,
            "confirmed": self.confirmed,
            "cancelled": False,
            "arrival": self.live_arrival.isoformat(),
            "departure": self.live_departure.isoformat(),
        }

    def as_gtfs_rt(self) -> gtfs_realtime_pb2.TripUpdate.StopTimeUpdate:
        u = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate(
            stop_sequence=self.stop_sequence,
            stop_id=str(self.stop_id),
        )
        if self.cancelled:
            u.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        else:
            uncertainty = 0 if self.confirmed else 1
            u.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
            u.arrival = gtfs_realtime_pb2.TripUpdate.StopTimeEvent(
                time=round(self.live_arrival.timestamp()),
                uncertainty=uncertainty,
            )
            u.departure = gtfs_realtime_pb2.TripUpdate.StopTimeEvent(
                time=round(self.live_departure.timestamp()),
                uncertainty=uncertainty,
            )
        return u


@dataclass
class TripDelay(Fact):
    trip: TripDate
    stops: list[StopDelay]

    def as_json(self) -> Mapping[str, Any]:
        return {
            "type": "delay",
            "id": f"D_{self.trip.start_date.isoformat()}_{self.trip.trip_id}",
            "trip": self.trip.as_json(),
            "stops": [i.as_json() for i in self.stops],
        }

    def as_gtfs_rt(self) -> gtfs_realtime_pb2.FeedEntity:
        return gtfs_realtime_pb2.FeedEntity(
            id=f"D_{self.trip.start_date.isoformat()}_{self.trip.trip_id}",
            trip_update=gtfs_realtime_pb2.TripUpdate(
                trip=self.trip.as_gtfs_rt(),
                stop_time_update=(i.as_gtfs_rt() for i in self.stops),
            ),
        )


def fetch_delays(apikey: str) -> FactContainer[TripDelay]:
    with requests.get(
        "https://pdp-api.plk-sa.pl/api/v1/operations/shortened",
        params={"pageSize": "99999", "fullRoutes": "true"},
        headers={"X-Api-Key": apikey},
    ) as r:
        r.raise_for_status()
        data = r.json()

    if data["pg"]["hn"]:
        raise ValueError("operations endpoint overflowed over multiple pages")

    return FactContainer(
        timestamp=datetime.fromisoformat(data["ts"]),
        facts=[parse_trip_delay(i) for i in data["tr"]],
    )


def parse_trip_delay(d: Mapping[str, Any]) -> TripDelay:
    return TripDelay(
        trip=TripDate.parse(d["sid"], d["od"]),
        stops=[parse_stop_delay(i) for i in d["st"]],
    )


def parse_stop_delay(s: Mapping[str, Any]) -> StopDelay:
    return StopDelay(
        stop_id=s["id"],
        stop_sequence=s["psn"],
        cancelled=s["cn"] or False,
        confirmed=s["cf"] or False,
        live_arrival=datetime.fromisoformat(s["aa"]),
        live_departure=datetime.fromisoformat(s["ad"]),
    )
