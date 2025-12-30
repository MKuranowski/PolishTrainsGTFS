# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests

from . import gtfs_realtime_pb2
from .fact import Fact, FactContainer
from .tools import TripDate, as_translation


@dataclass
class Alert(Fact):
    id: int
    title: str
    description: str
    trips: list[TripDate]

    def as_json(self) -> Mapping[str, Any]:
        return {
            "type": "alert",
            "id": f"A_{self.id}",
            "title": self.title,
            "description": self.description,
            "trips": [i.as_json() for i in self.trips],
        }

    def as_gtfs_rt(self) -> gtfs_realtime_pb2.FeedEntity:
        return gtfs_realtime_pb2.FeedEntity(
            id=f"A_{self.id}",
            alert=gtfs_realtime_pb2.Alert(
                header_text=as_translation(self.title),
                description_text=as_translation(self.description),
                informed_entity=(
                    gtfs_realtime_pb2.EntitySelector(trip=i.as_gtfs_rt()) for i in self.trips
                ),
            ),
        )


def fetch_alerts(apikey: str) -> FactContainer[Alert]:
    with requests.get(
        "https://pdp-api.plk-sa.pl/api/v1/disruptions/shortened",
        headers={"X-Api-Key": apikey},
    ) as r:
        r.raise_for_status()
        data = r.json()

    return FactContainer(
        timestamp=datetime.fromisoformat(data["ts"]),
        facts=[parse_alert(i) for i in data["ds"]],
    )


def parse_alert(d: Mapping[str, Any]) -> Alert:
    return Alert(
        id=d["id"],
        title=d["tt"] or "",
        description=d["msg"] or "",
        trips=[TripDate.parse(i["sid"], i["od"]) for i in d["ar"]],
    )
