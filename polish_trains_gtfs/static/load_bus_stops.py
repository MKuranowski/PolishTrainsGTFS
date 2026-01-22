# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import groupby
from operator import itemgetter
from statistics import mean
from typing import Self, cast
from xml.sax import ContentHandler as XmlSaxContentHandler
from xml.sax import parse as xml_sax_parse
from xml.sax.xmlreader import AttributesImpl as XmlSaxAttributes

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Stop
from impuls.tools.geo import initial_bearing
from impuls.tools.types import StrPath

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
class BusStop:
    station_id: str = ""
    lat: float = 0.0
    lon: float = 0.0
    direction_hints: list[str] = field(default_factory=list[str])

    @property
    def gtfs_id(self) -> str:
        if self.direction_hints == [] or self.direction_hints == ["*"]:
            return f"{self.station_id}_BUS"
        return f"{self.station_id}_BUS_{self.direction_hints[0]}"

    def __bool__(self) -> bool:
        return self.station_id != "" and self.lat != 0.0 and self.lon != 0.0


class PLRailMapBusStopLoader(XmlSaxContentHandler):
    def __init__(self) -> None:
        super().__init__()
        self.stops = defaultdict[str, list[BusStop]](list)
        self.current_stop = BusStop()
        self.is_stop = False

    def startElement(self, name: str, attrs: XmlSaxAttributes) -> None:
        if name == "node":
            self.current_stop = BusStop(lat=float(attrs["lat"]), lon=float(attrs["lon"]))
        elif name == "tag":
            if attrs["k"] == "highway" and attrs["v"] == "bus_stop":
                self.is_stop = True
            if attrs["k"] == "ref:station":
                self.current_stop.station_id = attrs["v"]
            elif attrs["k"] == "direction" and attrs["v"]:
                self.current_stop.direction_hints = attrs["v"].split(";")

    def endElement(self, name: str) -> None:
        if name == "node":
            if self.is_stop and self.current_stop:
                self.stops[self.current_stop.station_id].append(self.current_stop)
            self.is_stop = False

    @classmethod
    def load_from_file(cls, path: StrPath) -> defaultdict[str, list[BusStop]]:
        handler = cls()
        xml_sax_parse(path, handler)
        return handler.stops


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
        curated_bus_stops = PLRailMapBusStopLoader.load_from_file(
            r.resources["pl_rail_map.osm"].stored_at,
        )
        bus_trips_by_stops = self.group_bus_trips(self.load_bus_trips(r.db))
        uncurated_stations = list[str]()

        for station_id, trips in bus_trips_by_stops.items():
            if stops := curated_bus_stops.get(station_id):
                with r.db.transaction():
                    self.curate_bus_stops(r.db, station_id, stops, trips)
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
                "SELECT trip_id, stop_sequence, stop_id "
                "FROM stop_times "
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
        stops: list[BusStop],
        trips: Iterable[tuple[int, Trip]],
    ) -> None:
        if len(stops) == 1:
            stop_updates = StopUpdate.for_trips(trips, stops[0].gtfs_id)
            new_stops = stops
        else:
            matcher = GeoTripMatcher(stops, self.stop_locations)
            stop_updates = [matcher.match(trip, offset) for offset, trip in trips]
            new_stops = [i for i in stops if i.gtfs_id in matcher.used_ids]

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
        new_stops: Sequence[BusStop],
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

    def apply_stops(self, db: DBConnection, station_id: str, new_stops: Sequence[BusStop]) -> None:
        preserve_train = has_train_departures(db, station_id)
        existing_stop = db.retrieve_must(Stop, station_id)

        if preserve_train:
            rail_id = f"{station_id}_RAIL"
            db.raw_execute(
                "UPDATE stops SET stop_id = ? WHERE stop_id = ?",
                (rail_id, station_id),
            )
            db.raw_execute(
                "INSERT INTO stops (stop_id,name,lat,lon,location_type) VALUES (?,?,?,?,1)",
                (station_id, existing_stop.name, existing_stop.lat, existing_stop.lon),
            )
            db.raw_execute(
                "UPDATE stops SET parent_station = ? WHERE stop_id = ?",
                (station_id, rail_id),
            )
            db.raw_execute_many(
                "INSERT INTO stops (stop_id,name,lat,lon,parent_station) VALUES (?,?,?,?,?)",
                ((i.gtfs_id, existing_stop.name, i.lat, i.lon, station_id) for i in new_stops),
            )
        elif len(new_stops) > 1:
            lat = round(mean(i.lat for i in new_stops), 6)
            lon = round(mean(i.lon for i in new_stops), 6)
            db.raw_execute(
                "UPDATE stops SET lat = ?, lon = ?, location_type = 1 WHERE stop_id = ?",
                (lat, lon, station_id),
            )
            db.raw_execute_many(
                "INSERT INTO stops (stop_id,name,lat,lon,parent_station) VALUES (?,?,?,?,?)",
                ((i.gtfs_id, existing_stop.name, i.lat, i.lon, station_id) for i in new_stops),
            )
        else:
            stop = new_stops[0]
            db.raw_execute(
                "UPDATE stops SET stop_id = ?, lat = ?, lon = ? WHERE stop_id = ?",
                (stop.gtfs_id, stop.lat, stop.lon, station_id),
            )


class GeoTripMatcher:
    def __init__(
        self,
        bus_stops: Iterable[BusStop],
        stop_locations: Mapping[str, tuple[float, float]],
    ) -> None:
        self.stop_locations = stop_locations
        self.stop_id_by_hint = {
            hint: stop.gtfs_id for stop in bus_stops for hint in stop.direction_hints
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
        towards = next_id or prev_id
        assert towards, "single-stop trips are not supported"
        return initial_bearing(*self.stop_locations[curr_id], *self.stop_locations[towards]) % 360


def get_stop_name(db: DBConnection, stop_id: str) -> str:
    # fmt: off
    return cast(
        str,
        db.raw_execute("SELECT name FROM stops WHERE stop_id = ?", (stop_id,))
            .one_must("invalid stop")
            [0]
    )
    # fmt: on


def has_train_departures(db: DBConnection, stop_id: str) -> bool:
    with db.raw_execute(
        "SELECT 1 FROM stop_times LEFT JOIN trips USING (trip_id) "
        "LEFT JOIN routes USING (route_id) WHERE stop_id = ? AND type = 2 "
        "LIMIT 1",
        (stop_id,),
    ) as q:
        return q.one() is not None


def list_get[T, U](seq: Sequence[T], idx: int, default: U = None) -> T | U:
    if idx < 0 or idx >= len(seq):
        return default
    return seq[idx]


def angle_diff(a: float, b: float) -> float:
    delta = (b - a) % 360
    if delta > 180:
        return delta - 360
    return delta
