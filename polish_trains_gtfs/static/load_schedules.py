# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from operator import itemgetter
from typing import IO, Any, cast
from zoneinfo import ZoneInfo

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Attribution, Date, FeedInfo

from .. import json
from ..ids import get_trip_id

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR

TZ = ZoneInfo("Europe/Warsaw")


class LoadSchedules(Task):
    def __init__(self, r: str = "schedules.json") -> None:
        super().__init__()
        self.r = r

        self.calendar_id_counter = 0
        self.calendars = dict[frozenset[Date], int]()

        self.agency_names = dict[str, str]()
        self.route_names = dict[str, str]()
        self.stop_names = dict[int, str]()

    def clear(self) -> None:
        self.calendar_id_counter = 0
        self.calendars.clear()

        self.agency_names.clear()
        self.route_names.clear()
        self.stop_names.clear()

    def execute(self, r: TaskRuntime) -> None:
        self.clear()

        with r.resources[self.r].open_binary() as f:
            self.load_agencies(f)
            self.load_routes(f)
            self.load_stops(f)
            with r.db.transaction():
                self.create_attributions(r.db)
                self.load_feed_info(r.db, f)
                self.load_schedules(r.db, f)

    def create_attributions(self, db: DBConnection) -> None:
        db.create_many(
            Attribution,
            (
                Attribution(
                    id="1",
                    organization_name="Data: PKP Polskie linie Kolejowe S.A.",
                    url="https://www.plk-sa.pl/klienci-i-kontrahenci/api-otwarte-dane",
                    is_authority=True,
                    is_data_source=True,
                ),
                Attribution(
                    id="2",
                    organization_name="GTFS: Mikołaj Kuranowski",
                    url="https://mkuran.pl/gtfs/",
                    is_producer=True,
                ),
            ),
        )

    def load_feed_info(self, db: DBConnection, f: IO[bytes]) -> None:
        timestamp = self.load_update_timestamp(f).astimezone(TZ)
        start_date, end_date = self.load_feed_dates(f)
        db.create(
            FeedInfo(
                publisher_name="Mikołaj Kuranowski",
                publisher_url="https://mkuran.pl/gtfs",
                lang="pl",
                version=timestamp.isoformat(),
                start_date=start_date,
                end_date=end_date,
            )
        )

    @staticmethod
    def load_update_timestamp(f: IO[bytes]) -> datetime:
        if ts := json.first(f, "ts"):
            return datetime.fromisoformat(ts)
        return datetime.min.replace(tzinfo=timezone.utc)

    @staticmethod
    def load_feed_dates(f: IO[bytes]) -> tuple[Date, Date]:
        if pr := json.first(f, "pr"):
            start = Date.from_ymd_str(pr["f"][:10])
            end = Date.from_ymd_str(pr["t"][:10])
        else:
            start = Date(1, 1, 1)
            end = Date(1, 1, 1)
        return start, end

    def load_stops(self, f: IO[bytes]) -> None:
        for _, stop in json.object_iter(f, "dc.st"):
            self.stop_names[stop["id"]] = stop.get("nm", "")

    def load_agencies(self, f: IO[bytes]) -> None:
        for id, name in json.object_iter(f, "dc.cr"):
            self.agency_names[id.strip()] = name

    def load_routes(self, f: IO[bytes]) -> None:
        for code, name in json.object_iter(f, "dc.cc"):
            self.route_names[code] = name

    def load_schedules(self, db: DBConnection, f: IO[bytes]) -> None:
        for route in json.list_iter(f, "rt.item"):
            self.process_route(db, route)

    def process_route(self, db: DBConnection, r: Mapping[str, Any]) -> None:
        trip_id = get_trip_id(r["sid"], r["oid"], r.get("toid"))
        route_stations = cast(list[dict[str, Any]], r["st"])

        if len(route_stations) < 2:
            self.logger.warning("Trip %s has less than 2 stops - skipping", trip_id)
            return

        agency_id = self.get_agency_id(db, r["cc"])
        route_id = self.get_route_id(db, agency_id, r["ccs"])
        calendar_id = self.get_calendar_id(db, r["od"])

        plk_number = get_fallback(r, "nn", "idn", "ian", default="")
        display_number = get_fallback(r, "idn", "ian", "nn", default="")
        long_name = get_fallback(r, "nm", default="")

        extra_fields = json.dumps(
            {
                "order_id": str(r["oid"]),
                "plk_train_number": plk_number,
                "trip_long_name": long_name,
            }
        )

        db.raw_execute(
            "INSERT INTO trips (trip_id, route_id, calendar_id, short_name, extra_fields_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (trip_id, route_id, calendar_id, display_number, extra_fields),
        )

        route_stations.sort(key=itemgetter("ord"))
        for i, route_station in enumerate(r["st"]):
            self.process_route_stop(db, trip_id, i, route_station)

    def process_route_stop(
        self,
        db: DBConnection,
        trip_id: str,
        sequence: int,
        s: Mapping[str, Any],
    ) -> None:
        stop_id = self.get_stop_id(db, s["id"])
        order = cast(int, s["ord"])

        arrival_time = s.get("atm")
        arrival_day = s.get("ady") or 0
        departure_time = s.get("dtm")
        departure_day = s.get("ddy") or 0

        if arrival_time and departure_time:
            pass  # separate arrival and departure times
        elif arrival_time:
            departure_time = arrival_time
            departure_day = arrival_day
        elif departure_time:
            arrival_time = departure_time
            arrival_day = departure_day
        else:
            self.logger.warning(
                "Trip %s has no time at stop %d (order %d)", trip_id, stop_id, order
            )
            return

        arrival = parse_time(arrival_time, arrival_day)
        departure = parse_time(departure_time, departure_day)

        platform = get_fallback(s, "dpl", "apl", default="")
        track = get_fallback(s, "dtr", "atr", default="")
        extra_fields = json.dumps({"track": track, "order": str(order)})

        db.raw_execute(
            "INSERT INTO stop_times (trip_id, stop_sequence, stop_id, arrival_time, "
            "departure_time, platform, extra_fields_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (trip_id, sequence, stop_id, arrival, departure, platform, extra_fields),
        )

    def get_agency_id(self, db: DBConnection, carrier_code: str) -> str:
        agency_id = {"Leo Express": "LEO"}.get(carrier_code, carrier_code.strip())
        db.raw_execute(
            "INSERT OR IGNORE INTO agencies (agency_id, name, url, timezone, lang) "
            "VALUES (?, ?, 'https://example.com/', 'Europe/Warsaw', 'pl')",
            (agency_id, self.agency_names.get(carrier_code, "")),
        )
        return agency_id

    def get_route_id(self, db: DBConnection, agency_id: str, route_code: str) -> str:
        route_id = f"{agency_id}_{route_code}"
        db.raw_execute(
            "INSERT OR IGNORE INTO routes (route_id, agency_id, short_name, long_name, type) "
            "VALUES (?, ?, ?, ?, 2)",
            (route_id, agency_id, route_code, self.route_names.get(route_code, "")),
        )
        return route_id

    def get_stop_id(self, db: DBConnection, stop_id: int) -> int:
        db.raw_execute(
            "INSERT OR IGNORE INTO stops (stop_id, name, lat, lon) VALUES (?, ?, 0, 0)",
            (stop_id, self.stop_names.get(stop_id, "")),
        )
        return stop_id

    def get_calendar_id(self, db: DBConnection, operating_dates: Iterable[str]) -> int:
        dates = frozenset(Date.from_ymd_str(i[:10]) for i in operating_dates)
        if calendar_id := self.calendars.get(dates):
            return calendar_id
        else:
            self.calendar_id_counter += 1
            calendar_id = self.calendar_id_counter

            db.raw_execute("INSERT INTO calendars (calendar_id) VALUES (?)", (calendar_id,))
            db.raw_execute_many(
                "INSERT INTO calendar_exceptions (calendar_id,date,exception_type) VALUES (?,?,1)",
                ((calendar_id, str(date)) for date in dates),
            )

            self.calendars[dates] = calendar_id
            return calendar_id


def parse_time(x: str, day_offset: int = 0) -> int:
    parts = x.split(":")
    if len(parts) == 2:
        h, m = map(int, parts)
        s = 0
    elif len(parts) == 3:
        h, m, s = map(int, parts)
    else:
        raise ValueError(f"invalid time value: {x!r}")

    h += 24 * day_offset
    return h * HOUR + m * MINUTE + s


def get_fallback[T](obj: Mapping[str, Any], *keys: str, default: T) -> T:
    for key in keys:
        if item := obj.get(key):
            return cast(T, item)
    return default
