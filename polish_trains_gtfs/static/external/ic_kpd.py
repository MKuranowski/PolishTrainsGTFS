import csv
import difflib
from collections import defaultdict
from typing import NamedTuple, cast


from ..util.apikey import get_apikey
from .task import LoadExternal
from collections.abc import Iterator
from datetime import datetime, timezone
from ftplib import FTP_TLS
from itertools import groupby
from operator import itemgetter

from impuls import LocalResource, Task, TaskRuntime
from impuls.model import StopTime, TimePoint, Trip, CalendarException, Stop
from impuls.errors import InputNotModified
from impuls.resource import ConcreteResource, Resource, ZippedResource
from impuls.tools.types import StrPath

CSVRow = dict[str, str]


class TrainKey(NamedTuple):
    date: str
    train_number: str


class KPDStop(NamedTuple):
    stop_id: str
    departure_platform: str
    departure_track: str


KPDLookup = defaultdict[str, dict[str, list[KPDStop]]]
CalendarLookup = defaultdict[str, list[str]]


class CleanWaypoints(Task):
    def execute(self, r: TaskRuntime):
        with r.db.transaction():
            r.db.raw_execute(
                """
                DELETE FROM stop_times
                WHERE pickup_type = 1 and drop_off_type = 1
                """
            )

            #renumber stop_sequence
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
    def __init__(self):
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
                "waypoints.yaml": LocalResource("data/waypoints.yaml")
            }
        else:
            return {}

    def clear(self):
        self.important_waypoints.clear()
        self.calendar_lookup.clear()


    def execute(self, r: TaskRuntime) -> None:
        self.clear()
        self.load_lookup_tables(r)
        kpd = self.load_kpd(r)

        if not kpd:
            return

        for plk_trip in self.get_trips_to_process(r):
            if kpd_times := self.find_kpd_match(kpd, plk_trip, r):
                plk_times = self.get_plk_stop_times(plk_trip, r)
                combined_times = self.enhance_items(plk_times, kpd_times, r)
                if combined_times:
                    self.apply_combined_times(plk_trip, combined_times, r)

    def load_lookup_tables(self, r: TaskRuntime):
        self.important_waypoints = {cast(str, i) for i in r.resources["waypoints.yaml"].yaml()["waypoints"]}

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

    def load_kpd(self, r: TaskRuntime) -> KPDLookup | None:
        if "ic_kpd_rozklad.csv" not in r.resources:
            self.logger.warning(
                "No IC KPD Rozklad resource available. Skipping execution."
            )
            return

        rows = train_rows(r.resources["ic_kpd_rozklad.csv"].stored_at, self.important_waypoints)

        return build_kpd_lookup(rows)


    def get_trips_to_process(self, r:TaskRuntime) -> list[Trip]:
        return r.db.typed_out_execute("SELECT * FROM trips WHERE trip_id LIKE 'PLK_IC_%'", Trip).all()

    def find_kpd_match(self, kpd: KPDLookup, trip: Trip, r: TaskRuntime) -> list[KPDStop] | None:

        plk_number = trip.get_extra_field("plk_train_number")
        if not plk_number:
            self.logger.warning("Trip %s has no plk_train_number", trip.id)
            return

        main_number, extra_number = get_plk_train_numbers(plk_number)

        if main_number not in kpd:
            if extra_number in kpd:
                main_number = extra_number
            else:
                self.logger.warning(
                    "Train %s not found in KPD Rozklad data.", main_number
                )
                return

        calendar_dates_for_trip = self.calendar_lookup.get(trip.calendar_id, [])
        if not calendar_dates_for_trip:
            self.logger.warning(
                "Trip %s has no calendar dates. Skipping.", trip.id
            )
            return

        date = next((d for d in calendar_dates_for_trip if d in kpd[main_number]), None)
        if not date:
            self.logger.warning(
                f"Train {trip.id} / {main_number} has no stops in KPD for any of its active dates. Skipping."
            )
            return
        stops_from_kpd = kpd[main_number][date]
        return stops_from_kpd

    def get_plk_stop_times(self, trip: Trip, r: TaskRuntime) -> list[StopTime]:
        stops_from_plk = r.db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence",
            StopTime,
            (trip.id,),
        ).all()
        return stops_from_plk

    def enhance_items(self, plk_stop_times: list[StopTime], kpd_stops: list[KPDStop], r: TaskRuntime) -> list[StopTime] | None:
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
                    stop_time, updated = merge_stop_time_with_kpd(plk, kpd)
                    combined.append(stop_time)
                    if updated:
                        updated_stops.add(plk.stop_id)
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


    def apply_combined_times(self, trip: Trip, combined: list[StopTime], r: TaskRuntime):
        try:
            r.db.raw_execute(
                """
                            DELETE FROM stop_times
                            WHERE trip_id = ?
                            """,
                (trip.id,),
            )
            r.db.create_many(StopTime, combined)
        except Exception:
            self.logger.error(
                f"Error occurred while updating stop_times for trip {trip.id} {trip.get_extra_field('plk_train_number')}",
                exc_info=True,
            )
            self.logger.debug(
                f"Stops for trip {trip.id}: {combined}"
            )
            raise


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
        main_int = int(main_number)
        if main_int % 2 == 0:
            extra_number = str(main_int + 1)
        else:
            extra_number = str(main_int - 1)
    return main_number, extra_number


def merge_stop_time_with_kpd(plk: StopTime, kpd: KPDStop) -> tuple[StopTime, bool]:
    updated = False
    if not plk.platform:
        plk.platform = normalize_platform(kpd.departure_platform)
        plk.set_extra_field("track", kpd.departure_track)
        updated = True
    return plk, updated

def kpd_to_stop_time(trip_id: str, kpd: KPDStop):
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


def build_kpd_lookup(rows: Iterator[tuple[TrainKey, Iterator[CSVRow]]]) -> KPDLookup:
    """
    Parses train rows from KPD CSV and builds a lookup mapping
    clean train numbers to date, and date to a list of KPDStop.
    """
    parsed = KPDLookup(dict)
    for key, lines in rows:
        clean_number = str(int(key.train_number.split("/")[0]) // 2 * 2)
        stops = list[KPDStop]()
        for line in lines:
            stop_id = line["NumerStacji"]
            departure_platform = line["PeronWyjazd"]
            if line["StacjaHandlowa"] != "1":
                departure_platform = "WAYPOINT"
            departure_track = line["TorWyjazd"]
            stops.append(KPDStop(stop_id, departure_platform, departure_track))
        parsed[clean_number][key.date] = stops
    return parsed
    


def ensure_start_and_end_not_at_waypoint(
    stops_times: list[StopTime]
) -> list[StopTime]:
    def is_waypoint(index: int) -> bool:
        stop_time = stops_times[index]
        return stop_time.pickup_type == StopTime.PassengerExchange.NONE and stop_time.drop_off_type == StopTime.PassengerExchange.NONE

    start = 0
    end = len(stops_times) - 1

    while start <= end and is_waypoint(start):
        start += 1

    while end >= start and is_waypoint(end):
        end -= 1

    return stops_times[start : end + 1]


def train_rows(filename: StrPath, important_waypoints: set[str]) -> Iterator[tuple[TrainKey, Iterator[CSVRow]]]:
    # NOTE: This assumes that the input file is sorted on (DataOdjazdu, NrPociagu, Lp).
    #       For the past 5 years that was the case.
    with open(filename, "r", encoding="windows-1250", newline="") as f:
        all_rows = csv.DictReader(f, delimiter=";")
        pax_rows = filter(
            lambda r: (
                r["StacjaHandlowa"] == "1"
                or r["NumerStacji"] in important_waypoints
            )
            and r["NumerStacji"] not in IGNORED_STOPS,
            all_rows,
        )
        for key, group in groupby(pax_rows, itemgetter("DataOdjazdu", "NrPociagu")):
            yield TrainKey(*key), group


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
            date = datetime.strptime(date, "%Y%m%d%H%M%S")
        elif len(date) > 15:
            date = datetime.strptime(date[:21], "%Y%m%d%H%M%S.%f")
        else:
            raise ValueError(f"invalid MDTM command response {resp}")

        # reinterpret date as UTC
        date = date.replace(tzinfo=timezone.utc)
        return date

    def iter_binary(self, cmd: str, blocksize: int = 8192) -> Iterator[bytes]:
        self.voidcmd("TYPE I")
        with self.transfercmd(cmd) as conn:
            while data := conn.recv(blocksize):
                yield data
        return self.voidresp()


class FTPResource(ConcreteResource):
    def __init__(
        self, filename: str, username: str, password: str, ftp_host: str
    ) -> None:
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
            self.fetch_time = datetime.now(timezone.utc)
            yield from ftp.iter_binary(f"RETR {self.filename}")


# List of stops from KPD that are problematic (either don't appear in PLK API or appear under different ids)
IGNORED_STOPS = (
    "179301", # Мостиська IІ /Mostistka/' - not in PLK API
    "179215", # Horka - technical station, not in PLK API
    "179193", # Jagodin - not in PLK API
    "178501", # Kępno - elevated part of the station, in PLK API as 45401 (same as lower part of the station)
)

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
