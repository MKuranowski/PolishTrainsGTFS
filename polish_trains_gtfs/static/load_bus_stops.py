# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import groupby
from math import inf
from operator import itemgetter
from statistics import mean
from typing import Self, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Stop
from impuls.tools.geo import earth_distance_m, initial_bearing

from ..geo import BusStop, load_stations
from .util import json

BEARING_CODE_TO_DEGREES = {
    "N": 0,
    "NE": 45,
    "E": 90,
    "SE": 135,
    "S": 180,
    "SW": 225,
    "W": 270,
    "NW": 315,
}


@dataclass
class StopTime:
    seq: int
    id: str


@dataclass
class Trip:
    id: str
    stop_times: list[StopTime]


@dataclass
class StopUpdate:
    trip_id: str
    stop_sequence: int
    old_stop_id: str
    new_stop_id: str

    @classmethod
    def for_trips(cls, trips: Iterable[tuple[int, Trip]], new_stop_id: str) -> list[Self]:
        return [cls.for_trip(trip, offset, new_stop_id) for offset, trip in trips]

    @classmethod
    def for_trip(cls, trip: Trip, stop_time_offset: int, new_stop_id: str) -> Self:
        st = trip.stop_times[stop_time_offset]
        return cls(
            trip_id=trip.id,
            stop_sequence=st.seq,
            old_stop_id=st.id,
            new_stop_id=new_stop_id,
        )


class LoadBusStops(Task):
    def __init__(self) -> None:
        super().__init__()
        self.stop_locations = dict[str, tuple[float, float]]()

    def execute(self, r: TaskRuntime) -> None:
        self.stop_locations = self.load_stop_locations(r.db)
        stations_data = load_stations(r.resources["geo.osm"].stored_at)
        bus_trips_by_station = self.group_bus_trips(self.load_bus_trips(r.db))
        uncurated_stations = list[str]()

        for station_id, trips in bus_trips_by_station.items():
            if (station := stations_data.get(station_id)) and station.bus_stops:
                with r.db.transaction():
                    self.curate_bus_stops(r.db, station_id, station.bus_stops, trips)
            else:
                uncurated_stations.append(station_id)

        self.warn_about_uncurated_stations(r.db, uncurated_stations)

    def load_stop_locations(self, db: DBConnection) -> dict[str, tuple[float, float]]:
        return {
            cast(str, i[0]): (cast(float, i[1]), cast(float, i[2]))
            for i in db.raw_execute("SELECT stop_id, lat, lon FROM stops")
        }

    def load_bus_trips(self, db: DBConnection) -> Iterable[Trip]:
        q = cast(
            Iterable[tuple[str, int, str]],
            db.raw_execute(
                "SELECT trip_id, stop_sequence, coalesce(parent_station, stop_id) "
                "FROM stop_times "
                "LEFT JOIN stops USING (stop_id) "
                "LEFT JOIN trips USING (trip_id) "
                "LEFT JOIN routes USING (route_id) "
                "WHERE routes.type = 3 "
                "ORDER BY trip_id, stop_sequence ASC"
            ),
        )
        for trip_id, rows in groupby(q, itemgetter(0)):
            yield Trip(id=trip_id, stop_times=[StopTime(r[1], r[2]) for r in rows])

    def group_bus_trips(self, trips: Iterable[Trip]) -> dict[str, list[tuple[int, Trip]]]:
        by_stop = dict[str, list[tuple[int, Trip]]]()
        for trip in trips:
            for offset, stop_time in enumerate(trip.stop_times):
                by_stop.setdefault(stop_time.id, []).append((offset, trip))
        return by_stop

    def warn_about_uncurated_stations(self, db: DBConnection, ids: list[str]) -> None:
        if not ids:
            return
        self.logger.warning(
            "%d stations don't have curated bus stop locations:\n\t%s",
            len(ids),
            "\n\t".join(f"{id} {get_stop_name(db, id)}" for id in ids),
        )

    def curate_bus_stops(
        self,
        db: DBConnection,
        station_id: str,
        stops: Iterable[BusStop],
        trips: Iterable[tuple[int, Trip]],
    ) -> None:
        stops_by_id = {get_stop_gtfs_id(station_id, stop): stop for stop in stops}
        if len(stops_by_id) == 1:
            gtfs_id = next(iter(stops_by_id))
            stop_updates = StopUpdate.for_trips(trips, gtfs_id)
            new_stops = stops_by_id
        else:
            matcher = GeoTripMatcher(stops_by_id, self.stop_locations)
            stop_updates = [matcher.match(trip, offset) for offset, trip in trips]
            new_stops = {id: stop for id, stop in stops_by_id.items() if id in matcher.used_ids}

        self.logger.debug(
            "Creating %d stops for %d bus trips at %s",
            len(new_stops),
            len(stop_updates),
            station_id,
        )
        self.apply_changes(db, station_id, new_stops, stop_updates)

    def apply_changes(
        self,
        db: DBConnection,
        station_id: str,
        new_stops: Mapping[str, BusStop],
        stop_updates: Sequence[StopUpdate],
    ) -> None:
        self.apply_stops(db, station_id, new_stops)
        db.raw_execute_many(
            "UPDATE stop_times SET stop_id = ? WHERE trip_id = ? AND stop_sequence = ?",
            ((i.new_stop_id, i.trip_id, i.stop_sequence) for i in stop_updates),
        )
        db.raw_execute_many(
            "UPDATE transfers SET to_stop_id = ? WHERE to_trip_id = ? AND to_stop_id = ?",
            ((i.new_stop_id, i.trip_id, i.old_stop_id) for i in stop_updates),
        )
        db.raw_execute_many(
            "UPDATE transfers SET from_stop_id = ? WHERE from_trip_id = ? AND from_stop_id = ?",
            ((i.new_stop_id, i.trip_id, i.old_stop_id) for i in stop_updates),
        )

    def apply_stops(
        self,
        db: DBConnection,
        station_id: str,
        new_stops: Mapping[str, BusStop],
    ) -> None:
        station = db.retrieve_must(Stop, station_id)
        country = station.get_extra_field("country") or ""

        # Move the station location if there are only bus departures,
        # and the bus stops spread apart geographically.
        if (
            not has_train_departures(db, station_id)
            and bbox_diagonal_dist(new_stops.values()) <= 500
        ):
            station.lat = round(mean(i.lat for i in new_stops.values()), 6)
            station.lon = round(mean(i.lon for i in new_stops.values()), 6)
            db.raw_execute(
                "UPDATE stops SET lat = ?, lon = ? WHERE stop_id = ? OR stop_id = ?",
                (station.lat, station.lon, station_id, f"{station_id}_FALLBACK"),
            )

        db.create_many(
            Stop,
            (
                Stop(
                    id=gtfs_id,
                    name=station.name,
                    lat=bus_stop.lat,
                    lon=bus_stop.lon,
                    wheelchair_boarding=None,
                    location_type=Stop.LocationType.STOP,
                    parent_station=station_id,
                    extra_fields_json=json.dumps({"country": country, "stop_access": "1"}),
                )
                for gtfs_id, bus_stop in new_stops.items()
            ),
        )


class GeoTripMatcher:
    def __init__(
        self,
        bus_stops: Mapping[str, BusStop],
        stop_locations: Mapping[str, tuple[float, float]],
    ) -> None:
        self.stop_locations = stop_locations
        self.stop_id_by_hint = {
            hint: gtfs_id for gtfs_id, stop in bus_stops.items() for hint in stop.direction
        }
        self.stop_id_by_towards = {
            towards: gtfs_id for gtfs_id, stop in bus_stops.items() for towards in stop.towards
        }
        self.match_cache = dict[tuple[str | None, str, str | None], str]()
        self.used_ids = set[str]()

    def match(self, trip: Trip, stop_time_offset: int) -> StopUpdate:
        prev_id = st.id if (st := list_get(trip.stop_times, stop_time_offset - 1)) else None
        curr_id = trip.stop_times[stop_time_offset].id
        next_id = st.id if (st := list_get(trip.stop_times, stop_time_offset + 1)) else None

        if (replacement_id := self.match_cache.get((prev_id, curr_id, next_id))) is None:
            replacement_id = self.match_inner(prev_id, curr_id, next_id)
            self.match_cache[(prev_id, curr_id, next_id)] = replacement_id
            self.used_ids.add(replacement_id)
        return StopUpdate.for_trip(trip, stop_time_offset, replacement_id)

    def match_inner(self, prev_id: str | None, curr_id: str, next_id: str | None) -> str:
        if next_id and (towards_id := self.stop_id_by_towards.get(next_id)):
            return towards_id

        terminates = not prev_id or not next_id
        if terminates and (terminus_id := self.stop_id_by_hint.get("T")):
            return terminus_id

        if star_id := self.stop_id_by_hint.get("*"):
            return star_id

        trip_bearing = self.calc_bearing(prev_id, curr_id, next_id)
        closest_hint = min(
            (hint for hint in self.stop_id_by_hint if hint in BEARING_CODE_TO_DEGREES),
            key=lambda hint: abs(angle_diff(trip_bearing, BEARING_CODE_TO_DEGREES[hint])),
        )
        return self.stop_id_by_hint[closest_hint]

    def calc_bearing(self, prev_id: str | None, curr_id: str, next_id: str | None) -> float:
        if next_id:
            a, b = curr_id, next_id
        elif prev_id:
            a, b = prev_id, curr_id
        else:
            raise ValueError("single-stop trips are not supported")
        return initial_bearing(*self.stop_locations[a], *self.stop_locations[b]) % 360


def get_stop_gtfs_id(station_id: str, stop: BusStop) -> str:
    if stop.direction and "*" not in stop.direction:
        return f"{station_id}_BUS_{stop.direction[0]}"
    if stop.towards:
        return f"{station_id}_BUS_{stop.towards[0]}"
    return f"{station_id}_BUS"


def get_stop_name(db: DBConnection, stop_id: str) -> str:
    # fmt: off
    return cast(
        str,
        db.raw_execute("SELECT name FROM stops WHERE stop_id = ?", (stop_id,))
            .one_must("invalid stop")
            [0]
    )
    # fmt: on


def has_train_departures(db: DBConnection, station_id: str) -> bool:
    with db.raw_execute(
        "SELECT 1 FROM stop_times "
        "JOIN trips USING (trip_id) JOIN routes USING (route_id) "
        "WHERE stop_id LIKE concat(?, '%') AND routes.type = 2 "
        "LIMIT 1",
        (station_id,),
    ) as q:
        return q.one() is not None


def bbox_diagonal_dist(stops: Iterable[BusStop]) -> float:
    min_lat = inf
    min_lon = inf
    max_lat = -inf
    max_lon = -inf

    for stop in stops:
        min_lat = min(min_lat, stop.lat)
        min_lon = min(min_lon, stop.lon)
        max_lat = max(max_lat, stop.lat)
        max_lon = max(max_lon, stop.lon)

    if min_lat == inf:
        return 0  # no stops

    return earth_distance_m(min_lat, min_lon, max_lat, max_lon)


def list_get[T, U](seq: Sequence[T], idx: int, default: U = None) -> T | U:
    if idx < 0 or idx >= len(seq):
        return default
    return seq[idx]


def angle_diff(a: float, b: float) -> float:
    delta = (b - a) % 360
    if delta > 180:
        return delta - 360
    return delta
