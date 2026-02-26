# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Sequence
from dataclasses import dataclass, field
from math import nan

from impuls import DBConnection
from routx import earth_distance


@dataclass(frozen=True)
class MatchedNode:
    node_id: int
    stop_sequence: int | None = None


@dataclass(frozen=True)
class MatchedTrip:
    trip_id: str
    nodes: tuple[MatchedNode, ...]


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float
    distance: float = nan

    def distance_to_km(self, lat: float, lon: float) -> float:
        return earth_distance(self.lat, self.lon, lat, lon)


@dataclass
class GeneratedShape:
    shape_id: str
    points: list[Point] = field(default_factory=list[Point])
    stop_distances: dict[int, float] = field(default_factory=dict[int, float])

    def insert_into(self, db: DBConnection) -> None:
        db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (self.shape_id,))
        db.raw_execute_many(
            "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                (self.shape_id, i, round(pt.lat, 6), round(pt.lon, 6), round(pt.distance, 3))
                for i, pt in enumerate(self.points)
            ),
        )

    def apply_for(self, db: DBConnection, trip_id: str) -> None:
        self.apply_for_many(db, [trip_id])

    def apply_for_many(self, db: DBConnection, trip_ids: Sequence[str]) -> None:
        db.raw_execute_many(
            "UPDATE trips SET shape_id = ? WHERE trip_id = ?",
            ((self.shape_id, trip_id) for trip_id in trip_ids),
        )
        db.raw_execute_many(
            "UPDATE stop_times SET shape_dist_traveled = ? WHERE trip_id = ? AND stop_sequence = ?",
            (
                (round(dist, 3), trip_id, stop_seq)
                for trip_id in trip_ids
                for stop_seq, dist in self.stop_distances.items()
            ),
        )
