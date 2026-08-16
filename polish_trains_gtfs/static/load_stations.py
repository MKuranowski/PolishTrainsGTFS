# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import NamedTuple, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Stop

from ..geo import Platform, Station, load_stations
from .util import json


class TripStop(NamedTuple):
    trip_id: str
    stop_sequence: int


class PlatformTrack(NamedTuple):
    platform: str
    track: str


class StationToUpdate(NamedTuple):
    station_id: str
    name: str
    trips: defaultdict[PlatformTrack, list[TripStop]]


FAKE_PLATFORMS = {"", "BUS"}

PLATFORM_MATCH_OVERRIDE: Mapping[tuple[str, str, str], tuple[str, str]] = {
    ("33571", "1", ""): ("1/2", ""),  # W-wa Śródmieście - match platform "1" with "1/2"
    ("33571", "3", ""): ("2/3", ""),  # W-wa Śródmieście - match platform "3" with "2/3"
}


class LoadStations(Task):
    to_update: dict[str, StationToUpdate]

    def __init__(self) -> None:
        super().__init__()
        self.to_update = {}

    def execute(self, r: TaskRuntime) -> None:
        self.to_update = self.get_stations_to_update(r.db)
        stations = load_stations(r.resources["geo.osm"].stored_at)
        with r.db.transaction():
            for station in stations.values():
                self.apply(r.db, station)
        self.ensure_everything_updated()

    def apply(self, db: DBConnection, station: Station) -> None:
        # Don't apply the station if it's not used
        if not any(id in self.to_update for id in station.all_ids()):
            return

        self.upsert_station(db, station)
        self.create_fallback_platform(db, station)
        self.create_platforms(db, station)
        self.create_exits(db, station)
        self.remove_secondary_stops(db, station)
        self.mark_as_updated(station)

    def upsert_station(self, db: DBConnection, station: Station) -> None:
        stop = Stop(
            id=station.id,
            name=station.name,
            lat=station.lat,
            lon=station.lon,
            wheelchair_boarding=station.accessible,
            location_type=Stop.LocationType.STATION,
            extra_fields_json=json.dumps(
                {
                    "country": station.country,
                    "plk_secondary_id": "/".join(station.other_ids),
                }
            ),
        )

        if station.id in self.to_update:
            db.update(stop)  # departures are moved to a stop later on
        else:
            db.create(stop)

    def create_fallback_platform(self, db: DBConnection, station: Station) -> None:
        db.create(
            Stop(
                id=f"{station.id}_FALLBACK",
                name=station.name,
                lat=station.lat,
                lon=station.lon,
                wheelchair_boarding=station.accessible,
                location_type=Stop.LocationType.STOP,
                parent_station=station.id,
                extra_fields_json=json.dumps({"country": station.country, "stop_access": "0"}),
            )
        )

    def create_platforms(self, db: DBConnection, station: Station) -> None:
        to_move = self.get_all_trips_to_move(station.all_ids())
        for (platform, track), trips in to_move.items():
            if platform in FAKE_PLATFORMS:
                self.move_departures(db, f"{station.id}_FALLBACK", trips)
            else:
                # Find platform details in geo data
                geo_platform = self.find_matching_platform(station, platform, track)
                if station.platforms and not geo_platform:
                    self.logger.warning(
                        "%s %s: missing data for platform %r track %r",
                        station.id,
                        station.name,
                        platform,
                        track,
                    )

                # Prefer platform-specific geo data
                if geo_platform:
                    geo_obj = geo_platform
                    display_platform = geo_platform.name or platform
                    display_track = geo_platform.track or track
                else:
                    geo_obj = station
                    display_platform = platform
                    display_track = track

                # Combine platform and track for platform_code
                platform_code = (
                    f"{display_platform} t. {display_track}" if display_track else display_platform
                )

                # Insert the platform to the database
                geo_obj = geo_platform or station
                db_platform = Stop(
                    id=f"{station.id}_RAIL_{platform}_{track}",
                    name=station.name,
                    lat=geo_obj.lat,
                    lon=geo_obj.lon,
                    wheelchair_boarding=geo_obj.accessible,
                    location_type=Stop.LocationType.STOP,
                    parent_station=station.id,
                    platform_code=platform_code,
                    extra_fields_json=json.dumps({"country": station.country, "stop_access": "0"}),
                )
                db.create(db_platform)
                self.move_departures(db, db_platform.id, trips)

    def find_matching_platform(
        self,
        station: Station,
        platform: str,
        track: str,
    ) -> Platform | None:
        # Override matching criteria
        if track_override := PLATFORM_MATCH_OVERRIDE.get((station.id, platform, track)):
            platform, track = track_override
        elif platform_override := PLATFORM_MATCH_OVERRIDE.get((station.id, platform, "")):
            platform, _ = platform_override

        best: Platform | None = None

        for candidate in station.platforms:
            matches_platform = platform == candidate.name
            matches_track = track == candidate.track or candidate.track == ""

            if matches_platform and matches_track and (best is None or best.track == ""):
                best = candidate

        return best

    def move_departures(self, db: DBConnection, new_id: str, trips: Iterable[TripStop]) -> None:
        db.raw_execute_many(
            "UPDATE stop_times SET stop_id = ? WHERE trip_id = ? AND stop_sequence = ?",
            ((new_id, trip_id, stop_sequence) for trip_id, stop_sequence in trips),
        )

    def create_exits(self, db: DBConnection, station: Station) -> None:
        db.create_many(
            Stop,
            (
                Stop(
                    id=f"{station.id}_EXIT_{exit.node_id}",
                    name=exit.name,
                    lat=exit.lat,
                    lon=exit.lon,
                    wheelchair_boarding=exit.accessible,
                    location_type=Stop.LocationType.EXIT,
                    parent_station=station.id,
                    extra_fields_json=json.dumps(
                        {
                            "country": station.country,
                            "only_platforms": "/".join(exit.platforms),
                        }
                    ),
                )
                for exit in station.exits
            ),
        )

    def remove_secondary_stops(self, db: DBConnection, station: Station) -> None:
        db.raw_execute_many(
            "DELETE FROM stops WHERE stop_id = ?",
            ((id,) for id in station.other_ids),
        )

    def mark_as_updated(self, station: Station) -> None:
        for id in station.all_ids():
            self.to_update.pop(id, None)

    def get_stations_to_update(self, db: DBConnection) -> dict[str, StationToUpdate]:
        to_update = dict[str, StationToUpdate]()
        # FIXME: use arrival_platform and arrival_track when (departure) platform is fake
        with db.raw_execute(
            "SELECT stop_id, name, trip_id, stop_sequence, platform, "
            " json_extract(t.extra_fields_json, '$.track'), "
            " json_extract(t.extra_fields_json, '$.arrival_platform'), "
            " json_extract(t.extra_fields_json, '$.arrival_track') "
            "FROM stop_times t JOIN stops s USING (stop_id)"
        ) as q:
            for row in q:
                stop_id = cast(str, row[0])
                stop_name = cast(str, row[1])
                trip_id = cast(str, row[2])
                stop_sequence = cast(int, row[3])

                # Resolve the platform
                dep_platform = cast(str, row[4])
                arr_platform = cast(str, row[6]) or ""
                platform = arr_platform if dep_platform in FAKE_PLATFORMS else dep_platform

                # Resolve the track, but only for non-fake platforms
                if platform not in FAKE_PLATFORMS:
                    dep_track = (cast(str | None, row[5]) or "").upper()
                    arr_track = (cast(str | None, row[7]) or "").upper()
                    if dep_platform in FAKE_PLATFORMS:
                        track = arr_track or dep_track
                    else:
                        track = dep_track or arr_track
                else:
                    track = ""

                key = PlatformTrack(platform, track)
                if (station := to_update.get(stop_id)) is None:
                    station = StationToUpdate(stop_id, stop_name, defaultdict(list))
                    to_update[stop_id] = station

                station.trips[key].append(TripStop(trip_id, stop_sequence))

        return to_update

    def get_all_trips_to_move(
        self,
        ids: Iterable[str],
    ) -> defaultdict[PlatformTrack, list[TripStop]]:
        trips = defaultdict[PlatformTrack, list[TripStop]](list)
        for id in ids:
            if s := self.to_update.get(id):
                for key, stops in s.trips.items():
                    trips[key].extend(stops)
        return trips

    def ensure_everything_updated(self) -> None:
        if self.to_update:
            raise MultipleDataErrors(
                "LoadStations",
                [
                    DataError(f"Missing data for {i.station_id} {i.name!r}")
                    for i in self.to_update.values()
                ],
            )
