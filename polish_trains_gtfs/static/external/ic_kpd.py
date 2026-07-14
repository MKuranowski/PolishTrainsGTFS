import json
import csv
import difflib
from typing import List, Optional, Tuple, Set


from ..util.apikey import get_apikey
from .task import LoadExternal
from collections.abc import Iterator
from datetime import datetime, timezone
from ftplib import FTP_TLS
from itertools import groupby
from operator import itemgetter

from impuls import TaskRuntime
from impuls.model import Trip, CalendarException, Stop
from impuls.errors import InputNotModified
from impuls.resource import ConcreteResource, Resource, ZippedResource
from impuls.tools.types import StrPath

CSVRow = dict[str, str]
TrainKey = tuple[str, str]  # Date, TrainNumber


NON_PAX_FILTERED = []

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
                )
            }
        else:
            return {}

    def execute(self, r: TaskRuntime) -> None:
        if "ic_kpd_rozklad.csv" not in r.resources:
            self.logger.warning(
                "No IC KPD Rozklad resource available. Skipping execution."
            )

        with r.db.transaction():
            for stop_id in NON_PAX_IMPORTANT_STOPS:
                try:
                    r.db.create(
                        Stop(
                            id=stop_id,
                            name=f"Non-Pax Important Stop {stop_id}",
                            lat=0.0,
                            lon=0.0,
                        )
                    )
                except Exception as e:
                    NON_PAX_FILTERED.append(stop_id)
                    self.logger.error(
                        f"Error occurred while creating stop {stop_id}: {e}"
                    )

        rows = train_rows(r.resources["ic_kpd_rozklad.csv"].stored_at)
        # 1. parse
        # parse structure: Map[CleanNumber, Map[Date, List[Stops]]]

        parsed = {}  # CleanNumber -> Date -> List[Stops]
        for (date, train_number), lines in rows:
            clean_number = str(int(train_number.split("/")[0]) // 2 * 2)
            stops: List[Tuple[str, str, str]] = []  # List of (stop_id, ..ExtraData)
            for line in lines:
                stop_id = line["NumerStacji"]
                departure_platform = line["PeronWyjazd"]
                if line["StacjaHandlowa"] != "1":
                    departure_platform = "NO_PAX"
                departure_track = line["TorWyjazd"]
                stops.append((stop_id, departure_platform, departure_track))
            parsed[clean_number] = parsed.get(clean_number, {})
            parsed[clean_number][date] = stops
        with open("parsed_kpd.json", "w", encoding="utf-8") as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
        # 2. match
        trips = r.db.retrieve_all(Trip).all()
        calendar_dates = r.db.retrieve_all(CalendarException).all()
        calendar_lookup = {
            k: [v.date for v in g]
            for k, g in groupby(calendar_dates, key=lambda c: c.calendar_id)
        }

        for trip in trips:
            with r.db.transaction() as tx:
                if "IC" not in trip.id:
                    continue
                main_number, _, slash_number = trip.get_extra_field(
                    "plk_train_number"
                ).partition("/")
                if slash_number:
                    extra_number = main_number[:-1] + slash_number
                else:
                    main_int = int(main_number)
                    if main_int % 2 == 0:
                        extra_number = str(main_int + 1)
                    else:
                        extra_number = str(main_int - 1)

                if main_number not in parsed:
                    if extra_number in parsed:
                        main_number = extra_number
                    else:
                        self.logger.warning(
                            f"Train {main_number} not found in KPD Rozklad data."
                        )
                        continue

                calendar_dates = calendar_lookup.get(trip.calendar_id, [])
                if not calendar_dates:
                    self.logger.warning(
                        f"Trip {trip.id} has no calendar dates. Skipping."
                    )
                    continue

                date = calendar_dates[0]
                if str(date) not in parsed[main_number]:
                    if len(calendar_dates) > 1:
                        date = calendar_dates[1]
                    if str(date) not in parsed[main_number]:
                        self.logger.warning(
                            f"Train {trip.id} / {main_number} has no stops in KPD for date {date}. Skipping."
                        )
                        continue
                stops_from_kpd = parsed[main_number][str(date)]
                raw_stops_from_plk = r.db.raw_execute(
                    """
                    SELECT stop_id, trip_id, stop_sequence, arrival_time, departure_time, pickup_type, drop_off_type, stop_headsign, shape_dist_traveled, platform, extra_fields_json
                    FROM stop_times
                    WHERE trip_id = ?
                    ORDER BY stop_sequence
                """,
                    (trip.id,),
                ).all()

                keys_plk = [v[0] for v in raw_stops_from_plk]
                keys_kpd = [v[0] for v in stops_from_kpd]

                combined: List[
                    Tuple[str, Optional[Tuple[str]], Optional[Tuple[str]]]
                ] = []  # stop_id, plk, kpd
                diverging: Set[str] = set()

                matcher = difflib.SequenceMatcher(None, keys_plk, keys_kpd)

                for tag, i1, i2, j1, j2 in matcher.get_opcodes():
                    if tag == "equal":
                        for plk, kpd in zip(
                            raw_stops_from_plk[i1:i2], stops_from_kpd[j1:j2]
                        ):
                            combined.append((plk[0], plk, kpd))
                    else:
                        if tag in ("replace", "delete"):
                            for k in raw_stops_from_plk[i1:i2]:
                                combined.append((k[0], k, None))
                                diverging.add(k[0])
                        if tag in ("replace", "insert"):
                            for k in stops_from_kpd[j1:j2]:
                                combined.append((k[0], None, k))
                                diverging.add(k[0])

                if not diverging:
                    continue

                r.db.raw_execute(
                    """
                                DELETE FROM stop_times
                                WHERE trip_id = ?
                                """,
                    (trip.id,),
                )

                filtered = ensure_start_and_end_at_pax_station(combined)
                try:
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
                                plk[3] if plk else 0,  # arrival_time
                                plk[4] if plk else 0,  # departure_time
                                plk[5] if plk else 1,  # pickup_type
                                plk[6] if plk else 1,  # drop_off_type
                                plk[7] if plk else "",  # stop_headsign
                                plk[8] if plk else None,  # shape_dist_traveled
                                plk[9] if plk and plk[9] else normalize_platform(kpd[1]) if kpd else "",  # platform
                                plk[10] if plk else None,  # extra_fields_json
                            )
                            for i, (stop_id, plk, kpd) in enumerate(filtered)
                        ],
                    )
                except Exception as e:
                    tx.rollback()
                    self.logger.error(
                        f"Error occurred while updating stop_times for trip {trip.id} / {main_number}: {e}, {e.args}"
                    )
                    self.logger.debug(f"Stops for trip {trip.id} / {main_number}: {filtered}")
                    continue
        # [WARNING 21:27:12.546] Task.LoadICKPD: Missing stops in database: {'
        # 179301 - Мостиська IІ /Mostistka/' - nie występuje w PDP
        # , '179215', - Horka, nie występuje w PDP
        # '179193', - Jagodin, nie występuje w PDP
        # '178501'} - Kępno "Górne", w KDP jako 45401


def normalize_platform(x: str) -> str:
    if x[-1:] == "a":
        base = x[:-1]
        suffix = "a"
    else:
        base = x
        suffix = ""

    base = ROMAN_TO_ARABIC.get(base, base)
    return f"{base}{suffix}"

def ensure_start_and_end_at_pax_station(
    stops: list[Tuple[str, Optional[Tuple[str, str]], Optional[Tuple[str, str]]]],
) -> list[Tuple[str, Optional[Tuple[str, str]], Optional[Tuple[str, str]]]]:
    def is_no_pax(index: int) -> bool:
        kpd_stop = stops[index][2]
        return kpd_stop is not None and kpd_stop[1] == "NO_PAX"

    start = 0
    end = len(stops) - 1

    while start <= end and is_no_pax(start):
        start += 1

    while end >= start and is_no_pax(end):
        end -= 1

    return stops[start : end + 1]


# PLK:
# 0: stop_id,
# 1: trip_id,
# 2: stop_sequence,
# 3: arrival_time,
# 4: departure_time,
# 5: pickup_type,
# 6: drop_off_type,
# 7: stop_headsign,
# 8: shape_dist_traveled,
# 9: platform,
# 10: extra_fields_json

# KDP:
# 0: stop_id,
# 1: departure_platform,
# 2: departure_track


def train_rows(filename: StrPath) -> Iterator[tuple[TrainKey, Iterator[CSVRow]]]:
    # NOTE: This assumes that the input file is sorted on (DataOdjazdu, NrPociagu, Lp).
    #       For the past 5 years that was the case.
    with open(filename, "r", encoding="windows-1250", newline="") as f:
        all_rows = csv.DictReader(f, delimiter=";")
        pax_rows = filter(
            lambda r: (
                r["StacjaHandlowa"] == "1"
                or r["NumerStacji"] in NON_PAX_FILTERED#NON_PAX_IMPORTANT_STOPS
            )
            and r["NumerStacji"] not in ("179301", "179215", "179193", "178501"),
            all_rows,
        )
        yield from groupby(pax_rows, itemgetter("DataOdjazdu", "NrPociagu"))


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


# List of non pax stops that are important for routing
# TODO: move to a yaml
NON_PAX_IMPORTANT_STOPS = [
    # Swarzędz - Poznań: Franowo vs Wschodni
    "28522",  # Poznań Starołęka
    # "29801", # Poznań Wschód - excluded, due to problematic location
    # CMK vs other routes
    "64899",  # Włoszczowa Północ
    "48959",  # Opoczno Południe
    "64865",  # Góra Włodowska
    "64923",  # Knapówka
    # Szczecin Dąbie - Szczecin Główny: Port Centralny vs Dziewoklicz
    "299",  # Dziewoklicz
    "109",  # Szczecin Port Centralny
    # Warszawa Wschodnia - Otwock and Wołomin
    "265314",  # Warszawa Wschodnia Towarowa R49
    "265315",  # Warszawa Wschodnia Towarowa R51
    # Kraków: "Small Bypass" (to/via Płaszów, not via Główny)
    "80200",  # Kraków Olsza
    # Łódż: Łódź Widzew to Łódź Kaliska, without direction change
    "177970",  # Łódź Olechów Łoa
    # LK12: Cargo bypass of Warsaw
    "40345",  # Góra Kalwaria
    "47175",  # Tarnów
    # Trains from Pilawa to Warsaw via Mińsk Mazowiecki (skipping Otwock)
    "38729",  # Sulejówek Miłosna
    # Niespieszny Trains
    "79277",  # Dłubnia (Kraków bypass)
    "79566",  # Podgrabie / Podłęże PZS R201 (Kraków bypass)
    "45245",  # Kalisz
    "46581",  # Pabianice
    "28571",  # Koziegłowy (Poznań northern bypass)
    # GOP
    "73502",  # Mysłowice
    "74153",  # Dorota
    "179007",  # Długoszyn
    "73064",  # Katowice Kostuchna
]

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