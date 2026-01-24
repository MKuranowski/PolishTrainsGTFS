# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from datetime import datetime, timezone
from operator import itemgetter
from typing import IO, cast
from zoneinfo import ZoneInfo

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Attribution, Date, FeedInfo
from impuls.tools.strings import find_non_conflicting_id

from .. import json
from ..calendar import CalendarGenerator

AGENCY_ID_NORMALIZER = {
    "KMŁ": "KML",
    "Leo Express": "LEO",
    "ŁKA": "LKA",
}

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR

TZ = ZoneInfo("Europe/Warsaw")


class LoadSchedules(Task):
    def __init__(self, r: str = "schedules.json") -> None:
        super().__init__()
        self.r = r

        self.calendars = CalendarGenerator("PLK_")
        self.agency_names = dict[str, str]()
        self.route_names = dict[str, str]()
        self.stop_names = dict[int, str]()
        self.used_trip_ids = set[str]()

    def clear(self) -> None:
        self.calendars.clear()
        self.agency_names.clear()
        self.route_names.clear()
        self.stop_names.clear()
        self.used_trip_ids.clear()

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
                    id="MK",
                    organization_name="GTFS: Mikołaj Kuranowski",
                    url="https://mkuran.pl/gtfs/",
                    is_producer=True,
                ),
                Attribution(
                    id="PLK",
                    organization_name="Data: PKP Polskie linie Kolejowe S.A.",
                    url="https://www.plk-sa.pl/klienci-i-kontrahenci/api-otwarte-dane",
                    is_authority=True,
                    is_data_source=True,
                ),
            ),
        )

    def load_feed_info(self, db: DBConnection, f: IO[bytes]) -> None:
        timestamp = self.load_update_timestamp(f).astimezone(TZ)

        # Shift start_date by one, as the very first day will be missing night trains
        # starting two days ago. This means, that only start_date+1 has full schedules available.
        start_date, end_date = self.load_feed_dates(f)
        start_date = start_date.add_days(1)

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

    def process_route(self, db: DBConnection, r: json.Object) -> None:
        agency_id = self.get_agency_id(db, r["cc"])
        calendar_id = self.calendars.upsert(db, (Date.from_ymd_str(i[:10]) for i in r["od"]))
        schedule_id = str(r["sid"])
        order_id = str(r["oid"])
        trip_id = self.get_trip_id(agency_id, schedule_id, order_id)

        route_code = self.resolve_route_code(r)
        route_id = self.get_route_id(db, agency_id, route_code)

        plk_number = self.resolve_plk_number(r)
        display_number = get_fallback(r, "idn", "ian", default=plk_number)
        plk_name = get_fallback(r, "nm", default="")

        extra_fields = json.dumps(
            {
                "plk_category_code": route_code,
                "plk_train_number": plk_number,
                "plk_train_name": plk_name,
            }
        )

        db.raw_execute(
            "INSERT INTO trips (trip_id, route_id, calendar_id, short_name, extra_fields_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (trip_id, route_id, calendar_id, display_number, extra_fields),
        )

        route_stations = cast(list[json.Object], r["st"])
        route_stations.sort(key=itemgetter("ord"))
        for i, route_station in enumerate(route_stations):
            self.process_route_stop(db, trip_id, i, route_station)

    def process_route_stop(
        self,
        db: DBConnection,
        trip_id: str,
        sequence: int,
        s: json.Object,
    ) -> None:
        stop_id = self.get_stop_id(db, s["id"])
        plk_sequence = cast(int, s["ord"])

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
                "Trip %s has no time at stop %d (plk_seq %d)",
                trip_id,
                stop_id,
                plk_sequence,
            )
            return

        arrival = parse_time(arrival_time, arrival_day)
        departure = parse_time(departure_time, departure_day)

        arr_platform = s.get("apl", "")
        dep_platform = s.get("dpl", "")
        arr_track = s.get("atr", "")
        dep_track = s.get("dtr", "")

        extra_fields = json.dumps(
            {
                "track": dep_track or arr_track,
                "plk_category_code": get_fallback(s, "dcc", "acc", default=""),
                "plk_sequence": str(plk_sequence),
                "arrival_platform": arr_platform,
                "arrival_track": arr_track,
            }
        )

        db.raw_execute(
            "INSERT INTO stop_times (trip_id, stop_sequence, stop_id, arrival_time, "
            "departure_time, platform, extra_fields_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                trip_id,
                sequence,
                stop_id,
                arrival,
                departure,
                dep_platform or arr_platform,
                extra_fields,
            ),
        )

    def get_agency_id(self, db: DBConnection, carrier_code: str) -> str:
        agency_id = carrier_code.strip()
        agency_id = AGENCY_ID_NORMALIZER.get(agency_id, agency_id)
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

    def resolve_plk_number(self, route: json.Object) -> str:
        # Collect all unique numbers from the route stops. Note that the order matters.
        international_number = get_fallback(route, "idn", "ian", default="")
        seen_numbers = set[str]()
        numbers = list[str]()
        for s in route["st"]:
            a = get_fallback(s, "dtn", "atn", default="").lstrip("0")
            is_invalid = "brak" in a or "/" in a
            is_international = a == international_number or len(a) <= 3
            if a and not is_invalid and not is_international and a not in seen_numbers:
                seen_numbers.add(a)
                numbers.append(a)

        # XXX: Hotfix for longer, undetected international numbers
        # In particular: [1014, 41022, 41023] and [14022, 14023, 1014]
        if any(len(i) == 4 for i in numbers) and any(len(i) == 5 for i in numbers):
            numbers = [i for i in numbers if len(i) == 5]

        # Resolve all used numbers into a human-readable string
        match numbers:
            case [a]:
                return a
            case [a, b] if can_numbers_be_combined(a, b):
                return f"{a}/{b[-1]}"
            case _:
                if numbers:
                    self.logger.warning("Don't know how to combine train numbers %r", numbers)
                fallback = route.get("nn") or international_number
                if not fallback:
                    raise ValueError("train with absolutely no numbers")
                return fallback

    def resolve_route_code(self, route: json.Object) -> str:
        categories = {c for s in route["st"] if (c := get_fallback(s, "dcc", "acc", default=""))}
        if categories:
            return "/".join(sorted(categories))
        else:
            return route["ccs"]

    def get_trip_id(self, agency_id: str, schedule_id: str, order_id: str) -> str:
        base = "_".join(("PLK", agency_id, schedule_id, order_id))
        id = find_non_conflicting_id(self.used_trip_ids, base, "_")
        if id != base:
            self.logger.warning("Non-unique trip_id: %s", base)
        self.used_trip_ids.add(id)
        return id


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


def get_fallback[T](obj: json.Object, *keys: str, default: T) -> T:
    """Chains multiple `obj.get` calls until the first true-ish element,
    or returns `default` if no such element exists. Equivalent to
    `obj.get(keys[0]) or obj.get(keys[1]) or ... or default`.

    >>> get_fallback({"foo": "spam", "bar": "eggs"}, "foo", "bar", default="")
    'spam'
    >>> get_fallback({"bar": "eggs"}, "foo", "bar", default="")
    'eggs'
    >>> get_fallback({"foo": "", "bar": "eggs"}, "foo", "bar", default="")
    'eggs'
    >>> get_fallback({}, "foo", "bar", default="")
    ''
    """

    for key in keys:
        if item := obj.get(key):
            return cast(T, item)
    return default


def unique[T](iter: Iterable[T]) -> list[T]:
    """Returns all unique elements from `iter`, preserving iteration order.
    Similar to `list(set(iter))`.

    >>> unique([1, 2, 3])
    [1, 2, 3]
    >>> unique([3, 1, 1, 2, 1, 1, 2, 3, 1])
    [3, 1, 2]
    """

    seen = set[T]()
    result = list[T]()
    for elem in iter:
        if elem not in seen:
            seen.add(elem)
            result.append(elem)
    return result


def can_numbers_be_combined(a: str, b: str) -> bool:
    return a != "" and b != "" and a[:-1] == b[:-1] and abs(int(a[-1]) - int(b[-1])) == 1
