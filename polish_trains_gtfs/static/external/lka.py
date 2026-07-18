# SPDX-FileCopyrightText: 2026 Adrian Zgorzałek
# SPDX-License-Identifier: MIT

# Łódzka Kolej Aglomeracyjna runs a network of local/feeder buses (connecting
# its rail stations to surrounding towns and villages) that PKP PLK's API does
# not coordinate, so they are absent from the main schedules feed. KOLEO sells
# them as through-tickets alongside the train (e.g. train ŁKA 12379 to Bedoń,
# then bus "ŁA 1225" Bedoń - Wardzyn OSP), so end-user apps need the bus legs to
# render the full journey. ŁKA publishes these in its own static GTFS.
#
# Only the bus routes (route_type=3) are imported here; ŁKA's trains already
# arrive via PKP PLK. ŁKA offers a static feed only (no GTFS-Realtime), so
# these buses carry no live data - schedule only.

import csv
import os
import zipfile
from datetime import datetime
from typing import Iterator
from zoneinfo import ZoneInfo

from impuls import DBConnection, HTTPResource, LocalResource, Resource, TaskRuntime
from impuls.model import Agency, Attribution, Date, Route, Stop

from ..util.calendar import CalendarGenerator
from .task import LoadExternal

TZ = ZoneInfo("Europe/Warsaw")

# ŁKA's open-data GTFS lives under a stable token path, with the schedule year
# in the last segment (gtfs-2025-2026, ...). The year matches the European
# railway schedule revision, so it is derived rather than hard-coded.
GTFS_BASE = "https://kolej-lka.pl/pliki/pn0e6eg45qcl4hd5"

# All ŁKA-own ids are namespaced with this prefix so they never collide with
# the PKP-PLK-derived entities already loaded into the database.
ID_PREFIX = "LKA_"

BUS = 3  # GTFS route_type for buses


class LoadLKA(LoadExternal):
    # ŁKA's GTFS carries its own stop coordinates, so run after stop curation
    # (its village bus stops are not in PLRailMap).
    runs_after_stop_curation = True

    def __init__(self) -> None:
        super().__init__()
        self.calendars = CalendarGenerator(ID_PREFIX)

    @staticmethod
    def get_required_resources() -> dict[str, Resource]:
        # A local zip may be provided (offline builds / tests) via env var,
        # bypassing the network fetch.
        if local := os.getenv("LKA_GTFS_LOCAL"):
            return {"gtfs_lka.zip": LocalResource(local)}

        from impuls.tools.temporal import get_european_railway_schedule_revision

        revision = get_european_railway_schedule_revision()
        return {"gtfs_lka.zip": HTTPResource.get(f"{GTFS_BASE}/gtfs-{revision}/zip/")}

    def execute(self, r: TaskRuntime) -> None:
        self.calendars.clear()
        resource = r.resources["gtfs_lka.zip"]
        with zipfile.ZipFile(resource.stored_at) as zip:
            feed = _Feed.load(zip)

        self.logger.info(
            "ŁKA: %d bus routes, %d stops, %d trips",
            len(feed.routes),
            len(feed.stops),
            len(feed.trips),
        )

        with r.db.transaction():
            self.insert_static_objects(r.db, resource.fetch_time)
            self.insert_feed(r.db, feed)

    def insert_static_objects(self, db: DBConnection, fetch_time: datetime) -> None:
        # The agency usually already exists (created from PKP PLK data); only
        # create it when running in isolation (e.g. tests).
        if not db.raw_execute("SELECT 1 FROM agencies WHERE agency_id = 'LKA'").one():
            db.create(
                Agency(
                    id="LKA",
                    name="Łódzka Kolej Aglomeracyjna",
                    url="https://lka.lodzkie.pl",
                    timezone="Europe/Warsaw",
                    lang="pl",
                )
            )

        fetch_time_str = fetch_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
        db.create(
            Attribution(
                id="LKA_BUS",
                organization_name=f"Bus data: Łódzka Kolej Aglomeracyjna ({fetch_time_str})",
                url="https://lka.lodzkie.pl/pasazer/do-pobrania",
                is_operator=True,
                is_data_source=True,
            )
        )

    def insert_feed(self, db: DBConnection, feed: "_Feed") -> None:
        for route in feed.routes.values():
            db.create(
                Route(
                    id=ID_PREFIX + route.id,
                    agency_id="LKA",
                    short_name=route.short_name,
                    long_name=route.long_name,
                    type=Route.Type.BUS,
                    color=route.color,
                    text_color=route.text_color,
                )
            )

        for stop in feed.stops.values():
            db.create(Stop(id=ID_PREFIX + stop.id, name=stop.name, lat=stop.lat, lon=stop.lon))

        for trip in feed.trips:
            dates = feed.service_dates.get(trip.service_id, frozenset())
            if not dates:
                self.logger.warning("ŁKA: trip %s has no operating dates, skipping", trip.id)
                continue
            calendar_id = self.calendars.upsert(db, dates)
            db.raw_execute(
                "INSERT INTO trips (trip_id, route_id, calendar_id, headsign, short_name) "
                "VALUES (?, ?, ?, ?, ?)",
                (ID_PREFIX + trip.id, ID_PREFIX + trip.route_id, calendar_id, trip.headsign, trip.short_name),
            )
            db.raw_execute_many(
                "INSERT INTO stop_times "
                "(trip_id, stop_sequence, stop_id, arrival_time, departure_time) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    (ID_PREFIX + trip.id, st.sequence, ID_PREFIX + st.stop_id, st.arrival, st.departure)
                    for st in trip.stop_times
                ),
            )


# ─────────────────────────── parsing (pure, no DB) ───────────────────────────


class _Route:
    __slots__ = ("id", "short_name", "long_name", "color", "text_color")

    def __init__(self, id: str, short_name: str, long_name: str, color: str, text_color: str) -> None:
        self.id = id
        self.short_name = short_name
        self.long_name = long_name
        self.color = color
        self.text_color = text_color


class _Stop:
    __slots__ = ("id", "name", "lat", "lon")

    def __init__(self, id: str, name: str, lat: float, lon: float) -> None:
        self.id = id
        self.name = name
        self.lat = lat
        self.lon = lon


class _StopTime:
    __slots__ = ("sequence", "stop_id", "arrival", "departure")

    def __init__(self, sequence: int, stop_id: str, arrival: int, departure: int) -> None:
        self.sequence = sequence
        self.stop_id = stop_id
        self.arrival = arrival
        self.departure = departure


class _Trip:
    __slots__ = ("id", "route_id", "service_id", "headsign", "short_name", "stop_times")

    def __init__(self, id: str, route_id: str, service_id: str, headsign: str, short_name: str) -> None:
        self.id = id
        self.route_id = route_id
        self.service_id = service_id
        self.headsign = headsign
        self.short_name = short_name
        self.stop_times: list[_StopTime] = []


class _Feed:
    def __init__(self) -> None:
        self.routes: dict[str, _Route] = {}
        self.stops: dict[str, _Stop] = {}
        self.trips: list[_Trip] = []
        self.service_dates: dict[str, frozenset[Date]] = {}

    @classmethod
    def load(cls, zip: zipfile.ZipFile) -> "_Feed":
        feed = cls()

        # Bus routes only; ŁKA trains arrive via PKP PLK.
        for row in _csv(zip, "routes.txt"):
            if int(row["route_type"]) != BUS:
                continue
            feed.routes[row["route_id"]] = _Route(
                id=row["route_id"],
                short_name=row.get("route_short_name", ""),
                long_name=row.get("route_long_name", ""),
                color=row.get("route_color", ""),
                text_color=row.get("route_text_color", ""),
            )

        trips_by_id: dict[str, _Trip] = {}
        used_services: set[str] = set()
        for row in _csv(zip, "trips.txt"):
            if row["route_id"] not in feed.routes:
                continue
            trip = _Trip(
                id=row["trip_id"],
                route_id=row["route_id"],
                service_id=row["service_id"],
                headsign=row.get("trip_headsign", "") or "",
                short_name=row.get("trip_short_name", "") or "",
            )
            trips_by_id[trip.id] = trip
            used_services.add(trip.service_id)

        used_stops: set[str] = set()
        for row in _csv(zip, "stop_times.txt"):
            trip = trips_by_id.get(row["trip_id"])
            if trip is None:
                continue
            trip.stop_times.append(
                _StopTime(
                    sequence=int(row["stop_sequence"]),
                    stop_id=row["stop_id"],
                    arrival=_parse_time(row["arrival_time"] or row["departure_time"]),
                    departure=_parse_time(row["departure_time"] or row["arrival_time"]),
                )
            )
            used_stops.add(row["stop_id"])

        for trip in trips_by_id.values():
            trip.stop_times.sort(key=lambda st: st.sequence)
            if trip.stop_times:
                feed.trips.append(trip)

        for row in _csv(zip, "stops.txt"):
            if row["stop_id"] not in used_stops:
                continue
            feed.stops[row["stop_id"]] = _Stop(
                id=row["stop_id"],
                name=row["stop_name"],
                lat=float(row["stop_lat"]),
                lon=float(row["stop_lon"]),
            )

        feed.service_dates = _load_calendars(zip, used_services)
        return feed


def _load_calendars(zip: zipfile.ZipFile, wanted: set[str]) -> dict[str, frozenset[Date]]:
    from impuls.tools.temporal import date_range

    dates: dict[str, set[Date]] = {s: set() for s in wanted}

    names = set(zip.namelist())
    if "calendar.txt" in names:
        for row in _csv(zip, "calendar.txt"):
            service = row["service_id"]
            if service not in dates:
                continue
            weekmask = [
                row["monday"], row["tuesday"], row["wednesday"], row["thursday"],
                row["friday"], row["saturday"], row["sunday"],
            ]
            start, end = _parse_date(row["start_date"]), _parse_date(row["end_date"])
            for day in date_range(start, end):
                if weekmask[day.weekday()] == "1":
                    dates[service].add(day)

    if "calendar_dates.txt" in names:
        for row in _csv(zip, "calendar_dates.txt"):
            service = row["service_id"]
            if service not in dates:
                continue
            day = _parse_date(row["date"])
            if row["exception_type"] == "1":
                dates[service].add(day)
            elif row["exception_type"] == "2":
                dates[service].discard(day)

    return {s: frozenset(d) for s, d in dates.items()}


def _csv(zip: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    with zip.open(name) as f:
        text = f.read().decode("utf-8-sig")
    yield from csv.DictReader(text.splitlines())


def _parse_time(value: str) -> int:
    h, m, s = (int(p) for p in value.split(":"))
    return h * 3600 + m * 60 + s


def _parse_date(value: str) -> Date:
    value = value.strip()
    return Date(int(value[0:4]), int(value[4:6]), int(value[6:8]))
