# SPDX-FileCopyrightText: 2026 Marcin Kasznia, Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import difflib
from collections import defaultdict
from collections.abc import Container, Generator, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from ftplib import FTP_TLS
from itertools import groupby
from operator import itemgetter
from typing import NamedTuple, Self, TypeAlias, cast

from impuls import DBConnection, LocalResource, Task, TaskRuntime
from impuls.errors import DataError, InputNotModified, MultipleDataErrors
from impuls.model import Date, StopTime, TimePoint, Trip
from impuls.resource import ConcreteResource, Resource, ZippedResource

from ..util import json
from ..util.apikey import get_apikey
from .task import LoadExternal

ROMAN_TO_ARABIC = {
    "I": "1",
    "II": "2",
    "III": "3",
    "IV": "4",
    "V": "5",
    "VI": "6",
    "VII": "7",
    "VIII": "8",
    "IX": "9",
    "X": "10",
    "XI": "11",
    "XII": "12",
}

# List of stops from KPD that are problematic (either don't appear in PLK API or appear under different ids)
IGNORED_STOPS = (
    "179301",  # Мостиська IІ /Mostistka/' - not in PLK API
    "179215",  # Horka - technical station - not in PLK API
    "179193",  # Jagodin - not in PLK API
    "178501",  # Kępno - elevated part of the station, in PLK API as 45401 (same as lower part of the station)
)


class TrainKey(NamedTuple):
    """Unique identifier of a train - its number and departure date."""

    date: Date
    number: str

    def expanded(self) -> Generator["TrainKey"]:
        for number in get_plk_train_numbers(self.number):
            yield TrainKey(self.date, number)


@dataclass(slots=True)
class KPDStopTime:
    stop_id: str
    stop_name: str
    departure_platform: str
    departure_track: str
    arrival_platform: str
    arrival_track: str

    def enhance(self, plk: StopTime) -> bool:
        """Enhances the details of an existing PLK StopTime. Returns True if `plk` was modified."""
        if plk.platform or not self.departure_platform:
            return False

        plk.platform = self.departure_platform

        extra_fields = plk.get_extra_fields()
        extra_fields["track"] = self.departure_track
        extra_fields["arrival_platform"] = self.arrival_platform
        extra_fields["arrival_track"] = self.arrival_track
        plk.set_extra_fields(extra_fields)

        return True

    def convert(self, trip_id: str) -> StopTime:
        """Creates an "empty" StopTime based on this KPDStop. Such StopTime mustn't be exported
        in GTFS and must be removed via CleanWaypoints.
        """
        return StopTime(
            stop_id=self.stop_id,
            trip_id=trip_id,
            stop_sequence=0,
            arrival_time=TimePoint(),
            departure_time=TimePoint(),
            pickup_type=StopTime.PassengerExchange.NONE,
            drop_off_type=StopTime.PassengerExchange.NONE,
            platform=self.departure_platform,
            extra_fields_json=json.dumps(
                {
                    "track": self.departure_track,
                    "arrival_platform": self.arrival_platform,
                    "arrival_track": self.arrival_track,
                    "stop_name": self.stop_name,
                }
            ),
        )

    @classmethod
    def from_csv_row(cls, row: Mapping[str, str]) -> Self:
        stop_id = row["NumerStacji"]
        stop_name = normalize_null(row["NazwaStacji"])

        if row["StacjaHandlowa"] != "1":
            departure_platform = "WAYPOINT"
            departure_track = ""
            arrival_platform = "WAYPOINT"
            arrival_track = ""
        else:
            departure_platform = normalize_platform(row["PeronWyjazd"])
            departure_track = normalize_null(row["TorWyjazd"])
            arrival_platform = normalize_platform(row["PeronWjazd"])
            arrival_track = normalize_null(row["TorWjazd"])

        return cls(
            stop_id,
            stop_name,
            departure_platform,
            departure_track,
            arrival_platform,
            arrival_track,
        )


class CleanWaypoints(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute(
                """
                DELETE FROM stop_times
                WHERE pickup_type = 1 and drop_off_type = 1
                """
            )

            # renumber stop_sequence
            r.db.raw_execute(
                """
                UPDATE stop_times
                SET stop_sequence = stop_sequence + 10000
                WHERE trip_id like "%IC%";
                """
            )
            r.db.raw_execute(
                """
                WITH reordered AS (
                    SELECT
                        rowid AS rid,
                        ROW_NUMBER() OVER (
                            PARTITION BY trip_id
                            ORDER BY stop_sequence
                        ) AS new_stop_sequence
                    FROM stop_times
                    WHERE trip_id like "%IC%"
                )
                UPDATE stop_times
                SET stop_sequence = (
                    SELECT new_stop_sequence
                    FROM reordered
                    WHERE reordered.rid = stop_times.rowid
                )
                WHERE trip_id like "%IC%";
                """
            )


class LoadICKPD(LoadExternal):
    def __init__(self) -> None:
        super().__init__()
        self.waypoints = set[str]()
        self.known_stops = set[str]()
        self.unmatched_pax_stops = dict[str, str]()

    @staticmethod
    def get_required_resources() -> dict[str, Resource]:
        ftp_credentials = get_apikey("IC_FTP_CREDENTIALS")
        if ftp_credentials:
            username, _, password = ftp_credentials.strip().partition(",")
            return {
                "ic_kpd_rozklad.csv": ZippedResource(
                    r=FTPResource(
                        "rozklad/KPD_Rozklad.zip",
                        username,
                        password,
                        "ftps.intercity.pl",
                    ),
                    file_name_in_zip="KPD_Rozklad.csv",
                ),
                "waypoints.yaml": LocalResource("data/waypoints.yaml"),
            }
        else:
            return {}

    def clear(self) -> None:
        self.waypoints.clear()
        self.known_stops.clear()
        self.unmatched_pax_stops.clear()

    def execute(self, r: TaskRuntime) -> None:
        kpd_resource = r.resources.get("ic_kpd_rozklad.csv")
        if kpd_resource is None:
            self.logger.warning("No IC KPD Rozklad resource available. Skipping execution.")
            return

        self.clear()
        self.load_lookup_tables(r)

        plk_trips = PLKTrips.from_db(r.db)
        last_date = plk_trips.latest_date()
        total_trips = len(plk_trips)
        matched = 0
        curated = 0
        self.logger.info("Processing KPD and PLK schedules for %d trips", total_trips)

        kpd_trips = KPDTrips(
            csv_rows=kpd_resource.csv(encoding="windows-1250", delimiter=";"),
            waypoints=self.waypoints,
        )
        with r.db.transaction():
            for i, kpd_trip in enumerate(kpd_trips):
                if kpd_trip.date > last_date:
                    break  # KPD_Rozklad.csv is sorted on dep. date, so we can stop early

                if i % 500 == 0:
                    self.logger.debug("Read %d KPD trips", i)

                if plk_trip := plk_trips.pop(kpd_trip):
                    matched += 1

                    plk_times = self.get_plk_stop_times(plk_trip, r)
                    kpd_times = list(kpd_trips.stop_times())

                    combined_times = self.combine_stop_times(plk_times, kpd_times)
                    if combined_times:
                        curated += 1
                        self.replace_stop_times(r.db, combined_times)

        self.logger.info(
            "Processed all %d trips, matched %d (%.2f %%), curated %d (%.2f %%)",
            total_trips,
            matched,
            100 * matched / total_trips,
            curated,
            100 * curated / total_trips,
        )
        self.check_unmatched_stops()

    def load_lookup_tables(self, r: TaskRuntime) -> None:
        self.waypoints = {cast(str, i) for i in r.resources["waypoints.yaml"].yaml()["waypoints"]}
        self.known_stops = {cast(str, i[0]) for i in r.db.raw_execute("SELECT stop_id FROM stops")}

    def get_plk_stop_times(self, trip: Trip, r: TaskRuntime) -> list[StopTime]:
        with r.db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence",
            StopTime,
            (trip.id,),
        ) as query:
            return query.all()

    def combine_stop_times(
        self,
        plk: Sequence[StopTime],
        kpd: Sequence[KPDStopTime],
    ) -> list[StopTime] | None:
        """Enhances PLK stop times with KPD stop times; inserting missing waypoints."""

        combined = list[StopTime]()
        changed = False
        trip_id = plk[0].trip_id

        plk_stop_ids = [v.stop_id for v in plk]
        kpd_stop_ids = [v.stop_id for v in kpd]
        matcher = difflib.SequenceMatcher(None, plk_stop_ids, kpd_stop_ids)

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                for plk_st, kpd_st in zip(plk[i1:i2], kpd[j1:j2]):
                    if kpd_st.enhance(plk_st):
                        changed = True
                    combined.append(plk_st)
            else:
                assert tag in ("replace", "delete", "insert")
                changed = changed or j1 != j2  # j1 != j2 => non-empty kpd_stops slice

                combined.extend(plk[i1:i2])
                combined.extend(self.convert_waypoints_only(kpd[j1:j2], trip_id))

        if not changed:
            return None

        for i in range(len(combined)):
            combined[i].stop_sequence = i

        return ensure_start_and_end_not_at_waypoint(combined)

    def convert_waypoints_only(
        self,
        kpd: Iterable[KPDStopTime],
        trip_id: str,
    ) -> Generator[StopTime]:
        for st in kpd:
            if st.departure_platform == "WAYPOINT":
                yield st.convert(trip_id)
            else:
                self.unmatched_pax_stops[st.stop_id] = st.stop_name

    def replace_stop_times(self, db: DBConnection, stop_times: Sequence[StopTime]) -> None:
        trip_id = stop_times[0].trip_id
        self.create_missing_stops(db, stop_times)
        db.raw_execute("DELETE FROM stop_times WHERE trip_id = ?", (trip_id,))
        db.create_many(StopTime, stop_times)

    def create_missing_stops(self, db: DBConnection, combined: Iterable[StopTime]) -> None:
        for st in combined:
            if st.stop_id not in self.known_stops:
                name = st.get_extra_field("stop_name") or ""
                name = f"{name} (waypoint)" if name and st.platform == "WAYPOINT" else name

                db.raw_execute(
                    "INSERT INTO stops (stop_id, name, lat, lon) VALUES (?, ?, 0, 0)",
                    (st.stop_id, name),
                )
                self.known_stops.add(st.stop_id)

    def check_unmatched_stops(self) -> None:
        if self.unmatched_pax_stops:
            raise MultipleDataErrors(
                "loading KPD schedules",
                [
                    DataError(f"unmatched passenger stop: {id} ({name})")
                    for id, name in sorted(self.unmatched_pax_stops.items())
                ],
            )


class PLKTrips:
    """Set of PKP IC trips to curate, as provided by PKP PLK (stored in the DB)."""

    def __init__(self) -> None:
        self._by_id = dict[str, Trip]()
        self._by_number_date = dict[TrainKey, Trip]()

    def __len__(self) -> int:
        return len(self._by_id)

    def load(self, db: DBConnection) -> None:
        """Overwrites the current set of trips by re-loading them from the database."""

        self._by_id.clear()
        self._by_number_date.clear()

        calendars = self.load_calendars(db)
        for trip in self.load_trips(db):
            self._by_id[trip.id] = trip

            numbers_str = trip.get_extra_field("plk_train_number") or trip.short_name
            for number in get_plk_train_numbers(numbers_str):
                for date in calendars[trip.calendar_id]:
                    self._by_number_date[TrainKey(date, number)] = trip

    def pop(self, key: TrainKey) -> Trip | None:
        """
        Finds the first trip with any of the given numbers operating on the given date;
        removes it from the store and returns it.
        """
        for sub_key in key.expanded():
            if t := self.pop_exact(sub_key):
                return t
        return None

    def pop_exact(self, key: TrainKey) -> Trip | None:
        """
        Finds a Trip with a given number, operating on the given date;
        removes it from the store and returns it.
        """
        trip = self._by_number_date.get(key)
        if trip and trip.id in self._by_id:
            del self._by_id[trip.id]
            return trip
        return None

    def latest_date(self) -> Date:
        return max((i.date for i in self._by_number_date), default=Date(1, 1, 1))

    @staticmethod
    def load_calendars(db: DBConnection) -> defaultdict[str, list[Date]]:
        """Creates a lookup table from calendar_id to a list of operating dates
        of that calendar. Assumes a calendar_exceptions-only dataset.
        """
        calendars = defaultdict[str, list[Date]](list)
        with db.raw_execute(
            "SELECT calendar_id, date FROM calendar_exceptions WHERE exception_type = 1",
        ) as query:
            for row in query:
                calendar_id = cast(str, row[0])
                date = Date.from_ymd_str(cast(str, row[1]))
                calendars[calendar_id].append(date)
        return calendars

    @staticmethod
    def load_trips(db: DBConnection) -> Generator[Trip]:
        """Generates all trips to curate."""
        with db.typed_out_execute("SELECT * FROM trips WHERE trip_id LIKE 'PLK_IC_%'", Trip) as q:
            yield from q

    @classmethod
    def from_db(cls, db: DBConnection) -> Self:
        """Creates a new PLKTrips set and loads data into it."""
        self = cls()
        self.load(db)
        return self


class KPDTrips(Iterator[TrainKey]):
    """KPDTrips iterates over trips present in a KPD_Rozklad.csv file.

    The iterator works lazily, it only generates TrainKeys.
    To extract the KPDStops, one must call the `stop_times` method.
    """

    GroupKey: TypeAlias = tuple[str, str]  # DataOdjazdu, NrPociagu
    GroupValue: TypeAlias = Iterable[Mapping[str, str]]
    GroupItem: TypeAlias = tuple[GroupKey, GroupValue]

    _it: Iterator[GroupItem]
    _curr: GroupItem

    def __init__(
        self,
        csv_rows: Iterable[Mapping[str, str]],
        waypoints: Container[str],
    ) -> None:
        # NOTE: This assumes that the input file is sorted on (DataOdjazdu, NrPociagu, Lp).
        #       For the past 5 years that was the case.
        # FIXME: Instead of manually excluding stops, we should ignore StacjaHandlowa rows
        #        which are not matched with a corresponding PLK StopTime.
        self._it = groupby(
            filter(
                lambda row: (
                    (row["StacjaHandlowa"] == "1" or row["NumerStacji"] in waypoints)
                    and row["NumerStacji"] not in IGNORED_STOPS
                ),
                csv_rows,
            ),
            key=itemgetter("DataOdjazdu", "NrPociagu"),
        )
        self._curr = (("", ""), ())

    def __next__(self) -> TrainKey:
        self._curr = next(self._it)
        date_str, number = self._curr[0]
        return TrainKey(Date.from_ymd_str(date_str), number)

    def stop_times(self) -> Generator[KPDStopTime]:
        """Returns a generator of KPDStops of the last-generated trip.

        This method is only available once per trip, as the returned iterator
        is shared with the top-level CSV row iterator.
        """
        yield from map(KPDStopTime.from_csv_row, self._curr[1])


def normalize_platform(x: str) -> str:
    if x == "NULL":
        return ""

    if x[-1:] == "a":
        base = x[:-1]
        suffix = "a"
    else:
        base = x
        suffix = ""

    base = ROMAN_TO_ARABIC.get(base, base)
    return f"{base}{suffix}"


def normalize_null(x: str) -> str:
    return "" if x == "NULL" else x


def get_plk_train_numbers(plk_number: str) -> tuple[str, str]:
    """
    Parses the PLK train number and returns a tuple of (main_number, extra_number).
    """
    main_number, _, slash_number = plk_number.partition("/")
    if slash_number:
        extra_number = main_number[:-1] + slash_number
    else:
        # flip last bit: odd -> lower even; even -> higher odd
        extra_number = str(int(main_number) ^ 1)
    return main_number, extra_number


def ensure_start_and_end_not_at_waypoint(stops_times: list[StopTime]) -> list[StopTime]:
    def is_waypoint(index: int) -> bool:
        stop_time = stops_times[index]
        return (
            stop_time.pickup_type == StopTime.PassengerExchange.NONE
            and stop_time.drop_off_type == StopTime.PassengerExchange.NONE
        )

    start = 0
    end = len(stops_times) - 1

    while start <= end and is_waypoint(start):
        start += 1

    while end >= start and is_waypoint(end):
        end -= 1

    return stops_times[start : end + 1]


class FTP_TLS_Patched(FTP_TLS):
    """A patched FTP client"""

    def makepasv(self) -> tuple[str, int]:
        """Parse PASV response, but ignore provided IP.
        PKP IC's FTP sends incorrect addresses."""
        _, port = super().makepasv()
        return self.host, port

    def mod_time(self, filename: str) -> datetime:
        """Get modification time of file on the server.
        Returns an aware datetime object."""
        resp = self.voidcmd("MDTM " + filename)
        date = resp.split(" ")[1]

        if len(date) == 14:
            return datetime.strptime(date, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        elif len(date) > 15:
            return datetime.strptime(date[:21], "%Y%m%d%H%M%S.%f").replace(tzinfo=UTC)
        else:
            raise ValueError(f"invalid MDTM command response {resp}")

    def iter_binary(self, cmd: str, blocksize: int = 8192) -> Iterator[bytes]:
        self.voidcmd("TYPE I")
        with self.transfercmd(cmd) as conn:
            while data := conn.recv(blocksize):
                yield data
        return self.voidresp()


class FTPResource(ConcreteResource):
    def __init__(self, filename: str, username: str, password: str, ftp_host: str) -> None:
        super().__init__()
        self.filename = filename
        self.username = username
        self.password = password
        self.ftp_host = ftp_host

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        with FTP_TLS_Patched(self.ftp_host, self.username, self.password) as ftp:
            ftp.prot_p()

            current_last_modified = ftp.mod_time(self.filename)
            if conditional and current_last_modified <= self.last_modified:
                raise InputNotModified

            self.last_modified = current_last_modified
            self.fetch_time = datetime.now(UTC)
            yield from ftp.iter_binary(f"RETR {self.filename}")
