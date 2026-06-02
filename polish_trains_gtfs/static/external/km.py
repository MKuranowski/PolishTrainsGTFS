# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable
from xml.sax import parse as sax_parse
from xml.sax.handler import ContentHandler as SAXContentHandler
from xml.sax.xmlreader import AttributesImpl
from zoneinfo import ZoneInfo

from impuls import DBConnection, HTTPResource, LocalResource, Resource, TaskRuntime
from impuls.model import Attribution, Date, Route, Stop
from impuls.tools.temporal import date_range, get_european_railway_schedule_revision

from ..util.apikey import get_apikey
from ..util.calendar import CalendarGenerator
from .task import LoadExternal

TZ = ZoneInfo("Europe/Warsaw")

SHUTTLE_BUS_STOP_IDS = {"36467", "0"}


class LoadKM(LoadExternal):
    def __init__(self) -> None:
        super().__init__()
        self.calendars = CalendarGenerator("KM_")

    @staticmethod
    def get_required_resources() -> dict[str, Resource]:
        apikey = get_apikey("KM_APIKEY")
        if apikey == "__local__":
            return {"schedules_km.xml": LocalResource("_impuls_workspace/schedules_km.xml")}
        else:
            revision = get_european_railway_schedule_revision()
            url = f"https://mazowieckie.home.pl/ofertyxml/{apikey}/rjp-{revision}/xml/"
            return {"schedules_km.xml": HTTPResource.get(url)}

    def execute(self, r: TaskRuntime) -> None:
        self.calendars.clear()
        with r.db.transaction():
            self.insert_static_objects(r.db, fetch_time=r.resources["schedules_km.xml"].fetch_time)
            sax_parse(
                r.resources["schedules_km.xml"].stored_at,
                SchedulesHandler(lambda t: self.on_train(r.db, t)),
            )

    def on_train(self, db: DBConnection, t: "ParsedTrain") -> None:
        # Only care about Modlin - Lotnisko Modlin shuttle buses
        if (
            any(i.service_type != "BUS" for i in t.stop_times)
            or {i.id for i in t.stop_times} != SHUTTLE_BUS_STOP_IDS
        ):
            return

        trip_id = f"KM_{t.numbers[0]}_{t.version}"
        calendar_id = self.calendars.upsert(db, t.dates)

        db.raw_execute(
            "INSERT INTO trips (trip_id, route_id, calendar_id, short_name) VALUES (?,'KM_ZL',?,?)",
            (trip_id, calendar_id, t.numbers[0]),
        )
        db.raw_execute_many(
            "INSERT INTO stop_times (trip_id,stop_sequence,stop_id,arrival_time,departure_time) "
            "VALUES (?, ?, ?, ?, ?)",
            ((trip_id, idx, i.id, i.arrival, i.departure) for idx, i in enumerate(t.stop_times)),
        )

    def insert_static_objects(self, db: DBConnection, fetch_time: datetime) -> None:
        fetch_time_str = fetch_time.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
        db.create(
            Route(
                id="KM_ZL",
                agency_id="KM",
                short_name="ZL",
                long_name="",
                type=Route.Type.BUS,
            )
        )
        db.create(Stop(id="0", name="Lotnisko Modlin", lat=0, lon=0))
        db.create(
            Attribution(
                id="KM",
                organization_name=f"Data: Koleje Mazowieckie - KM sp. z o.o. ({fetch_time_str})",
                url="https://bip.mazowieckie.com.pl/pl/ponowne-wykorzystywanie-informacji-sektora-publicznego",
                is_operator=True,
                is_data_source=True,
            ),
        )


@dataclass
class ParsedStopTime:
    id: str
    arrival: int
    departure: int
    service_type: str


@dataclass
class ParsedTrain:
    numbers: list[str] = field(default_factory=list[str])
    version: str = ""
    symbol: str = ""
    dates: set[Date] = field(default_factory=set[Date])
    stop_times: list[ParsedStopTime] = field(default_factory=list[ParsedStopTime])


class DaysNesting(Enum):
    OUTER = 0
    INCLUDE = 1
    EXCLUDE = 2


class SchedulesHandler(SAXContentHandler):
    def __init__(self, cb: Callable[[ParsedTrain], None]) -> None:
        self.callback = cb

        self.train = ParsedTrain()
        self.days_nesting = DaysNesting.OUTER
        self.chars: list[str] | None = None

    def characters(self, content: str) -> None:
        if self.chars is not None:
            self.chars.append(content)

    def startElement(self, name: str, attrs: AttributesImpl) -> None:
        if name in {"number", "version", "symbol"}:
            self.chars = []

        elif name == "include":
            self.days_nesting = DaysNesting.INCLUDE

        elif name == "exclude":
            self.days_nesting = DaysNesting.EXCLUDE

        elif name == "days":
            start = Date.from_ymd_str(attrs["start"])
            end = Date.from_ymd_str(attrs["end"])

            if self.days_nesting is DaysNesting.OUTER:
                weekdays = _day_operation_code_to_compressed_weekdays(attrs["dayOperationCode"])
                for d in date_range(start, end):
                    if weekdays & (1 << d.weekday()):
                        self.train.dates.add(d)

            elif self.days_nesting is DaysNesting.INCLUDE:
                assert "dayOperationCode" not in attrs
                for d in date_range(start, end):
                    self.train.dates.add(d)

            elif self.days_nesting is DaysNesting.EXCLUDE:
                assert "dayOperationCode" not in attrs
                for d in date_range(start, end):
                    self.train.dates.discard(d)

        elif name == "station":
            arr = _parse_time(attrs["arr"] or attrs["dep"])
            dep = _parse_time(attrs["dep"] or attrs["arr"])

            # Time travel fix
            prev_dep = self.train.stop_times[-1].departure if self.train.stop_times else 0
            while arr < prev_dep:
                arr += 86400
            while dep < arr:
                dep += 86400

            self.train.stop_times.append(
                ParsedStopTime(
                    id=attrs["id"],
                    arrival=arr,
                    departure=dep,
                    service_type=attrs.get("serviceType", "KM"),
                )
            )

        elif name == "train":
            self.train = ParsedTrain()

    def endElement(self, name: str) -> None:
        if name == "version":
            version = self.collect_chars()
            if not version:
                raise ValueError("Train <version> element is empty")
            self.train.version = version
            self.chars = None

        elif name == "number":
            number = self.collect_chars()
            if not number:
                raise ValueError("Train <number> element is empty")
            self.train.numbers.append(number)

        elif name == "symbol":
            symbol = self.collect_chars()
            if symbol:
                self.train.symbol = symbol

        elif name in {"include", "exclude"}:
            self.days_nesting = DaysNesting.OUTER

        elif name == "train":
            if self.train.numbers and self.train.stop_times and self.train.dates:
                self.callback(self.train)

    def collect_chars(self) -> str:
        assert self.chars is not None
        s = "".join(self.chars)
        self.chars = None
        return s


def _parse_time(x: str) -> int:
    parts = x.split(":")
    if len(parts) == 3:
        h, m, s = map(int, parts)
    elif len(parts) == 2:
        h, m = map(int, parts)
        s = 0
    else:
        raise ValueError(f"invalid time string: {x}")
    return h * 3600 + m * 60 + s


def _day_operation_code_to_compressed_weekdays(c: str) -> int:
    # Compressed weekdays are encoded right-to-left,
    # lowest bit (1 << 0) is Monday, highest (1 << 6) is Sunday
    if c == "A":
        return 0b0011111  # Mon-Fri
    elif c == "B":
        return 0b1011111  # Sun-Fri
    elif c == "C":
        return 0b1100000  # Sat-Sun
    elif c == "D":
        return 0b0011111  # Mon-Fri
    elif c == "E":
        return 0b0111111  # Mon-Sat
    elif c == "+":
        return 0b1000000  # Sun
    elif c.isnumeric():
        mask = 0
        for one_based_weekday_str in c:
            mask |= 1 << (int(one_based_weekday_str) - 1)
        return mask
    else:
        raise ValueError(f"unrecognized dayOperationCode: {c}")
