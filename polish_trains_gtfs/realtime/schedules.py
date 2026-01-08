# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import csv
import re
from collections.abc import Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from io import TextIOWrapper
from typing import NamedTuple, Self
from zipfile import ZipFile

from impuls.model import Date
from impuls.tools.temporal import BoundedDateRange
from impuls.tools.types import StrPath

from .tools import TripDate


@dataclass(frozen=True, eq=True)
class OrderKey:
    schedule_id: int
    order_id: int
    operating_date: Date


@dataclass
class StopTime:
    trip: TripDate
    stop_sequence: int
    stop_id: str


@dataclass
class Trips:
    all: list[TripDate]
    by_order_number: dict[int, StopTime]


class DatePair(NamedTuple):
    operating_date: Date
    start_date: Date


class TripKeyPair(NamedTuple):
    gtfs: TripDate
    live: OrderKey


@dataclass
class Schedules:
    by_order: dict[OrderKey, Trips]
    valid_operating_dates: BoundedDateRange

    @classmethod
    def load_from_gtfs(cls, gtfs_path: StrPath) -> Self:
        # NOTE: "start_date" and "operating_date" are not the same and should not be confused.
        # "start_date" refers to GTFS dates, which may be shifted compared with
        # PLK's "operating_date" to ensure GTFS has no negative times.

        with ZipFile(gtfs_path, "r") as arch:
            with _open_text(arch, "feed_info.txt") as f:
                valid_operating_dates = _load_feed_dates(f)

            with _open_text(arch, "calendar_dates.txt") as f:
                services = _load_services(f, valid_operating_dates)

            with _open_text(arch, "trips.txt") as f:
                by_order, trip_id_to_keys = _load_trips(f, services)

            with _open_text(arch, "stop_times.txt") as f:
                _load_stop_times(f, by_order, trip_id_to_keys)

        return cls(by_order, valid_operating_dates)


def _load_feed_dates(f: Iterable[str]) -> BoundedDateRange:
    row = next(csv.DictReader(f))
    # feed_start_date is the first **full** day, including trips started on the previous day
    start = Date.from_ymd_str(row["feed_start_date"]).add_days(-1)
    end = Date.from_ymd_str(row["feed_end_date"])
    return BoundedDateRange(start, end)


def _load_services(f: Iterable[str], range: BoundedDateRange) -> dict[str, list[DatePair]]:
    services = dict[str, list[DatePair]]()
    for row in csv.DictReader(f):
        service_id = row["service_id"]
        start_date_offset = _extract_start_date_offset(service_id)
        start_date = Date.from_ymd_str(row["date"])
        operating_date = start_date.add_days(-start_date_offset)
        if operating_date in range:
            services.setdefault(row["service_id"], []).append(DatePair(operating_date, start_date))
    return services


def _load_trips(
    f: Iterable[str],
    services: Mapping[str, Sequence[DatePair]],
) -> tuple[dict[OrderKey, Trips], dict[str, list[TripKeyPair]]]:
    by_order = dict[OrderKey, Trips]()
    trip_id_to_keys = dict[str, list[TripKeyPair]]()

    for row in csv.DictReader(f):
        trip_id = row["trip_id"]
        schedule_id, order_id = _extract_schedule_order_ids(trip_id)
        if schedule_id is None or order_id is None:
            raise ValueError(f"failed to extract schedule & order ids from trip_id {trip_id!r}")

        service_id = row["service_id"]
        for operating_date, start_date in services.get(service_id, []):
            key = OrderKey(schedule_id, order_id, operating_date)
            trip_date = TripDate(trip_id, start_date)

            trip_id_to_keys.setdefault(trip_id, []).append(TripKeyPair(trip_date, key))
            if t := by_order.get(key):
                t.all.append(trip_date)
            else:
                by_order[key] = Trips([trip_date], {})

    return by_order, trip_id_to_keys


def _load_stop_times(
    f: Iterable[str],
    by_order: Mapping[OrderKey, Trips],
    trip_id_to_keys: Mapping[str, Sequence[TripKeyPair]],
) -> None:
    for row in csv.DictReader(f):
        trip_id = row["trip_id"]
        for trip_date, key in trip_id_to_keys.get(trip_id, ()):
            stop_time = StopTime(trip_date, int(row["stop_sequence"]), row["stop_id"])
            order_number = int(row["plk_order"])
            by_order[key].by_order_number[order_number] = stop_time


@contextmanager
def _open_text(arch: ZipFile, fname: str) -> Generator[TextIOWrapper, None, None]:
    with arch.open(fname, "r") as f:
        yield TextIOWrapper(f, encoding="utf-8-sig", newline="")


def _extract_start_date_offset(service_id: str) -> int:
    if m := re.search(r"([+-][0-9]+)D$", service_id):
        return int(m[1])
    return 0


def _extract_schedule_order_ids(trip_id: str) -> tuple[int, int] | tuple[None, None]:
    if m := re.search(r"^([0-9]+)_([0-9]+)", trip_id):
        return int(m[1]), int(m[2])
    return None, None
