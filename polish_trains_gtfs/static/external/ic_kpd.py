# SPDX-FileCopyrightText: 2026 Marcin Kasznia, Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import difflib
from collections import defaultdict
from collections.abc import Container, Generator, Iterable, Iterator, Mapping
from datetime import UTC, datetime
from ftplib import FTP_TLS
from itertools import groupby
from operator import itemgetter
from typing import NamedTuple, Self, TypeAlias, cast

from impuls import DBConnection, LocalResource, Task, TaskRuntime
from impuls.errors import InputNotModified
from impuls.model import CalendarException, Date, Stop, StopTime, TimePoint, Trip
from impuls.resource import ConcreteResource, Resource, ZippedResource

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
    date: Date
    train_number: str


class KPDStop(NamedTuple):
    stop_id: str
    departure_platform: str
    departure_track: str

    @classmethod
    def from_csv_row(cls, row: Mapping[str, str]) -> Self:
        stop_id = row["NumerStacji"]
        departure_platform = row["PeronWyjazd"]
        if row["StacjaHandlowa"] != "1":
            departure_platform = "WAYPOINT"
        departure_track = row["TorWyjazd"]
        return cls(stop_id, departure_platform, departure_track)


KPDLookup = defaultdict[str, dict[str, list[KPDStop]]]
CalendarLookup = defaultdict[str, list[str]]


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
        self.important_waypoints = set[str]()
        self.calendar_lookup = CalendarLookup(list)

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
        self.important_waypoints.clear()
        self.calendar_lookup.clear()

    def execute(self, r: TaskRuntime) -> None:
        kpd_resource = r.resources.get("ic_kpd_rozklad.csv")
        if kpd_resource is None:
            self.logger.warning("No IC KPD Rozklad resource available. Skipping execution.")
            return

        self.clear()
        self.load_lookup_tables(r)

        plk_trips = PLKTrips.from_db(r.db)
        total_trips = len(plk_trips)
        matched = 0
        curated = 0
        self.logger.info("Processing KPD and PLK schedules for %d trips", total_trips)

        kpd_trips = KPDTrips(
            csv_rows=kpd_resource.csv(encoding="windows-1250", delimiter=";"),
            waypoints=self.important_waypoints,
        )
        with r.db.transaction():
            for i, (kpd_date, kpd_number) in enumerate(kpd_trips):
                if i % 500 == 0:
                    self.logger.debug("Read %d KPD trips", i)

                if plk_trip := plk_trips.pop_any(get_plk_train_numbers(kpd_number), kpd_date):
                    matched += 1

                    plk_times = self.get_plk_stop_times(plk_trip, r)
                    kpd_times = list(kpd_trips.stop_times())

                    combined_times = self.enhance_items(plk_times, kpd_times, r)
                    if combined_times:
                        curated += 1
                        self.apply_combined_times(plk_trip, combined_times, r)

        self.logger.info(
            "Processed all %d trips, matched %d (%.2f %%), curated %d (%.2f %%)",
            total_trips,
            matched,
            100 * matched / total_trips,
            curated,
            100 * curated / total_trips,
        )

    def load_lookup_tables(self, r: TaskRuntime) -> None:
        self.important_waypoints = {
            cast(str, i) for i in r.resources["waypoints.yaml"].yaml()["waypoints"]
        }

        with r.db.transaction():
            all_stops = {stop.id for stop in r.db.retrieve_all(Stop).all()}
            for stop_id in self.important_waypoints:
                if stop_id not in all_stops:
                    r.db.create(
                        Stop(
                            id=stop_id,
                            name=f"Waypoint {stop_id}",
                            lat=0.0,
                            lon=0.0,
                        )
                    )
                    all_stops.add(stop_id)

        calendar_dates = r.db.retrieve_all(CalendarException).all()
        for entry in calendar_dates:
            self.calendar_lookup[entry.calendar_id].append(str(entry.date))

    def get_plk_stop_times(self, trip: Trip, r: TaskRuntime) -> list[StopTime]:
        stops_from_plk = r.db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence",
            StopTime,
            (trip.id,),
        ).all()
        return stops_from_plk

    def enhance_items(
        self, plk_stop_times: list[StopTime], kpd_stops: list[KPDStop], r: TaskRuntime
    ) -> list[StopTime] | None:
        """
        Enhances PLK stop times with KPD stop data and inserts KPD stops when missing from PLK.
        """
        keys_plk = [v.stop_id for v in plk_stop_times]
        keys_kpd = [v.stop_id for v in kpd_stops]

        combined = list[StopTime]()
        updated_stops = set[str]()
        trip_id = plk_stop_times[0].trip_id

        matcher = difflib.SequenceMatcher(None, keys_plk, keys_kpd)

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                for plk, kpd in zip(plk_stop_times[i1:i2], kpd_stops[j1:j2]):
                    if merge_stop_time_with_kpd(plk, kpd):
                        updated_stops.add(plk.stop_id)
                    combined.append(plk)
            else:
                if tag in ("replace", "delete"):
                    for plk in plk_stop_times[i1:i2]:
                        combined.append(plk)
                if tag in ("replace", "insert"):
                    for kpd in kpd_stops[j1:j2]:
                        combined.append(kpd_to_stop_time(trip_id, kpd))
                        updated_stops.add(kpd.stop_id)

        if not updated_stops:
            return None

        for i in range(len(combined)):
            combined[i].stop_sequence = i

        return ensure_start_and_end_not_at_waypoint(combined)

    def apply_combined_times(self, trip: Trip, combined: list[StopTime], r: TaskRuntime) -> None:
        try:
            r.db.raw_execute("DELETE FROM stop_times WHERE trip_id = ?", (trip.id,))
            # r.db.create_many(StopTime, combined)
            for st in combined:
                r.db.create(st)
        except Exception:
            self.logger.exception(
                "Error occurred while updating stop_times for trip %s %s",
                trip.id,
                trip.get_extra_field("plk_train_number"),
            )
            self.logger.debug("Stops for trip %s: %s", trip.id, combined)
            raise


class PLKTrips:
    """Set of PKP IC trips to curate, as provided by PKP PLK (stored in the DB)."""

    def __init__(self) -> None:
        self._by_id = dict[str, Trip]()
        self._by_number_date = dict[tuple[str, Date], Trip]()

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
                    self._by_number_date[number, date] = trip

    def pop_any(self, numbers: Iterable[str], date: Date) -> Trip | None:
        """
        Finds the first trip with any of the given numbers operating on the given date;
        removes it from the store and returns it.
        """
        for number in numbers:
            if t := self.pop(number, date):
                return t
        return None

    def pop(self, number: str, date: Date) -> Trip | None:
        """
        Finds a Trip with a given number, operating on the given date;
        removes it from the store and returns it.
        """
        trip = self._by_number_date.get((number, date))
        if trip and trip.id in self._by_id:
            del self._by_id[trip.id]
            return trip
        return None

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

    def stop_times(self) -> Generator[KPDStop]:
        """Returns a generator of KPDStops of the last-generated trip.

        This method is only available once per trip, as the returned iterator
        is shared with the top-level CSV row iterator.
        """
        yield from map(KPDStop.from_csv_row, self._curr[1])


def normalize_platform(x: str) -> str:
    if x[-1:] == "a":
        base = x[:-1]
        suffix = "a"
    else:
        base = x
        suffix = ""

    base = ROMAN_TO_ARABIC.get(base, base)
    return f"{base}{suffix}"


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


def merge_stop_time_with_kpd(plk: StopTime, kpd: KPDStop) -> bool:
    if not plk.platform:
        plk.platform = normalize_platform(kpd.departure_platform)
        plk.set_extra_field("track", kpd.departure_track)
        return True
    return False


def kpd_to_stop_time(trip_id: str, kpd: KPDStop) -> StopTime:
    """
    Creates an "empty" StopTime based on KPDStop. Such StopTime mustn't be exported in GTFS and must be removed via CleanWaypoints
    """
    return StopTime(
        stop_id=kpd.stop_id,
        trip_id=trip_id,
        stop_sequence=0,
        arrival_time=TimePoint(),
        departure_time=TimePoint(),
        pickup_type=StopTime.PassengerExchange.NONE,
        drop_off_type=StopTime.PassengerExchange.NONE,
    )


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
