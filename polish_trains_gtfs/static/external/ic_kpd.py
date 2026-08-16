import csv
import difflib
from typing import NamedTuple, TypedDict, cast
import json


from ..util.apikey import get_apikey
from .task import LoadExternal
from collections.abc import Iterator
from datetime import datetime, timezone
from ftplib import FTP_TLS
from itertools import groupby
from operator import itemgetter

from impuls import LocalResource, Task, TaskRuntime
from impuls.model import StopTime, Trip, CalendarException, Stop
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


KPDLookup = dict[str, dict[str, list[KPDStop]]]
CalendarLookup = dict[str, list[str]]


class CleanNonPaxStops(Task):
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

class Stops(TypedDict):
    stops: list[str]

class LoadICKPD(LoadExternal):
    def __init__(self):
        super().__init__()

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
                "non_pax_stops.yaml": LocalResource("data/non_pax_stops.yaml")
            }
        else:
            return {}

    def execute(self, r: TaskRuntime) -> None:
        if "ic_kpd_rozklad.csv" not in r.resources:
            self.logger.warning(
                "No IC KPD Rozklad resource available. Skipping execution."
            )
            return

        non_pax_important_stops = cast(Stops, r.resources["non_pax_stops.yaml"].yaml()).get("stops")

        with r.db.transaction():
            all_stops = {stop.id for stop in r.db.retrieve_all(Stop).all()}
            for stop_id in non_pax_important_stops:
                if stop_id not in all_stops:
                    r.db.create(
                        Stop(
                            id=stop_id,
                            name=f"Non-Pax Important Stop {stop_id}",
                            lat=0.0,
                            lon=0.0,
                        )
                    )
                    all_stops.add(stop_id)

        rows = train_rows(r.resources["ic_kpd_rozklad.csv"].stored_at, non_pax_important_stops)

        kpd_lookup = build_kpd_lookup(rows)

        trips = r.db.typed_out_execute("SELECT * FROM trips WHERE trip_id LIKE 'PLK_IC_%'", Trip).all()
        calendar_dates = r.db.retrieve_all(CalendarException).all()
        calendar_lookup = build_calendar_lookup(calendar_dates)

        for trip in trips:
            plk_number = trip.get_extra_field("plk_train_number")
            if not plk_number:
                self.logger.warning("Trip %s has no plk_train_number", trip.id)
                continue

            main_number, extra_number = get_plk_train_numbers(plk_number)

            if main_number not in kpd_lookup:
                if extra_number in kpd_lookup:
                    main_number = extra_number
                else:
                    self.logger.warning(
                        "Train %s not found in KPD Rozklad data.", main_number
                    )
                    continue

            calendar_dates_for_trip = calendar_lookup.get(trip.calendar_id, [])
            if not calendar_dates_for_trip:
                self.logger.warning(
                    "Trip %s has no calendar dates. Skipping.", trip.id
                )
                continue

            date = next((d for d in calendar_dates_for_trip if d in kpd_lookup[main_number]), None)
            if not date:
                self.logger.warning(
                    f"Train {trip.id} / {main_number} has no stops in KPD for any of its active dates. Skipping."
                )
                continue
            stops_from_kpd = kpd_lookup[main_number][date]
            stops_from_plk = r.db.typed_out_execute(
                "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence",
                StopTime,
                (trip.id,),
            ).all()

            combined, diverging = merge_stop_sequences(
                stops_from_plk, stops_from_kpd
            )

            if not diverging:
                continue

            filtered = ensure_start_and_end_at_pax_station(combined)
            try:
                r.db.raw_execute(
                    """
                                DELETE FROM stop_times
                                WHERE trip_id = ?
                                """,
                    (trip.id,),
                )
                r.db.raw_execute_many(
                    """
                INSERT INTO stop_times (stop_id, trip_id, stop_sequence,
                                arrival_time, departure_time, pickup_type, drop_off_type, stop_headsign, shape_dist_traveled, platform, extra_fields_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                    [
                        (
                            stop_id,
                            trip.id,
                            i + 1,
                            int(plk.arrival_time.total_seconds()) if plk else 0,
                            int(plk.departure_time.total_seconds()) if plk else 0,
                            plk.pickup_type.value if plk else 1,
                            plk.drop_off_type.value if plk else 1,
                            plk.stop_headsign if plk else "",
                            plk.shape_dist_traveled if plk else None,
                            merge_platform(plk, kpd),
                            merge_extra_fields(plk, kpd),
                        )
                        for i, (stop_id, plk, kpd) in enumerate(filtered)
                    ],
                )
            except Exception:
                self.logger.error(
                    f"Error occurred while updating stop_times for trip {trip.id} / {main_number}",
                    exc_info=True,
                )
                self.logger.debug(
                    f"Stops for trip {trip.id} / {main_number}: {filtered}"
                )
                raise


def merge_platform(plk: StopTime | None, kpd: KPDStop | None) -> str:
    if plk and plk.platform:
        return plk.platform

    if kpd:
        return normalize_platform(kpd.departure_platform)

    return ""


def merge_extra_fields(plk: StopTime | None, kpd: KPDStop | None) -> str | None:
    """
    Add track number when it's missing in plk data
    """
    if not plk:
        return None

    if plk.platform or not kpd:
        # PLK data contains platform, so it also contains track
        return plk.extra_fields_json

    extra_fields = json.loads(plk.extra_fields_json or "{}")

    extra_fields["track"] = kpd.departure_track
    return json.dumps(extra_fields)


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


def merge_stop_sequences(
    stops_from_plk: list[StopTime], stops_from_kpd: list[KPDStop]
) -> tuple[list[tuple[str, StopTime | None, KPDStop | None]], set[str]]:
    """
    Merges PLK and KPD stop sequences and identifies diverging stop IDs.
    """
    keys_plk = [v.stop_id for v in stops_from_plk]
    keys_kpd = [v.stop_id for v in stops_from_kpd]

    combined: list[tuple[str, StopTime | None, KPDStop | None]] = []
    diverging: set[str] = set()

    matcher = difflib.SequenceMatcher(None, keys_plk, keys_kpd)

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for plk, kpd in zip(stops_from_plk[i1:i2], stops_from_kpd[j1:j2]):
                combined.append((plk.stop_id, plk, kpd))
        else:
            if tag in ("replace", "delete"):
                for k in stops_from_plk[i1:i2]:
                    combined.append((k.stop_id, k, None))
                    diverging.add(k.stop_id)
            if tag in ("replace", "insert"):
                for k in stops_from_kpd[j1:j2]:
                    combined.append((k.stop_id, None, k))
                    diverging.add(k.stop_id)

    return combined, diverging


def build_kpd_lookup(rows: Iterator[tuple[TrainKey, Iterator[CSVRow]]]) -> KPDLookup:
    """
    Parses train rows from KPD CSV and builds a lookup mapping
    clean train numbers to date, and date to a list of KPDStop.
    """
    parsed: KPDLookup = {}
    for key, lines in rows:
        clean_number = str(int(key.train_number.split("/")[0]) // 2 * 2)
        stops: list[KPDStop] = []
        for line in lines:
            stop_id = line["NumerStacji"]
            departure_platform = line["PeronWyjazd"]
            if line["StacjaHandlowa"] != "1":
                departure_platform = "NO_PAX"
            departure_track = line["TorWyjazd"]
            stops.append(KPDStop(stop_id, departure_platform, departure_track))
        parsed.setdefault(clean_number, {})[key.date] = stops
    return parsed


def build_calendar_lookup(calendar_dates: list[CalendarException]) -> CalendarLookup:
    """
    Builds a lookup mapping calendar IDs to a list of dates (as strings).
    """
    lookup: CalendarLookup = {}
    for entry in calendar_dates:
        lookup.setdefault(entry.calendar_id, []).append(str(entry.date))
    return lookup


def ensure_start_and_end_at_pax_station(
    stops: list[tuple[str, StopTime | None, KPDStop | None]],
) -> list[tuple[str, StopTime | None, KPDStop | None]]:
    def is_no_pax(index: int) -> bool:
        kpd_stop = stops[index][2]
        return kpd_stop is not None and kpd_stop.departure_platform == "NO_PAX"

    start = 0
    end = len(stops) - 1

    while start <= end and is_no_pax(start):
        start += 1

    while end >= start and is_no_pax(end):
        end -= 1

    return stops[start : end + 1]


def train_rows(filename: StrPath, non_pax_important_stops: list[str]) -> Iterator[tuple[TrainKey, Iterator[CSVRow]]]:
    # NOTE: This assumes that the input file is sorted on (DataOdjazdu, NrPociagu, Lp).
    #       For the past 5 years that was the case.
    with open(filename, "r", encoding="windows-1250", newline="") as f:
        all_rows = csv.DictReader(f, delimiter=";")
        pax_rows = filter(
            lambda r: (
                r["StacjaHandlowa"] == "1"
                or r["NumerStacji"] in non_pax_important_stops
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
    "NO_PAX": "NO_PAX",
}
