# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from .gtfs_realtime_pb2 import FeedEntity, FeedHeader, FeedMessage


class Fact(Protocol):
    def as_json(self) -> Mapping[str, Any]: ...
    def as_gtfs_rt(self) -> FeedEntity: ...


@dataclass
class FactContainer[FactT: Fact]:
    timestamp: datetime
    facts: list[FactT] = field(default_factory=list[FactT])

    def merge[FactU: Fact](self, other: "FactContainer[FactU]") -> "FactContainer[FactT | FactU]":
        return FactContainer(
            timestamp=max(self.timestamp, other.timestamp),
            facts=self.facts + other.facts,
        )

    def as_json(self) -> Mapping[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(timespec="seconds"),
            "facts": [i.as_json() for i in self.facts],
        }

    def as_gtfs_rt(self) -> FeedMessage:
        return FeedMessage(
            header=FeedHeader(
                gtfs_realtime_version="2.0",
                incrementality=FeedHeader.Incrementality.FULL_DATASET,
                timestamp=round(self.timestamp.timestamp()),
            ),
            entity=(i.as_gtfs_rt() for i in self.facts),
        )
