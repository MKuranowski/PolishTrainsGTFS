# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Collection, Container, Hashable, Iterable
from operator import attrgetter

from impuls.tools.geo import earth_distance_m
from impuls.tools.types import StrPath

from .issue import Issue
from .loader import load_data
from .model import ChildEntity, DirectionHint, Station


class Validator(ABC):
    @abstractmethod
    def check_station(self, station: Station) -> Iterable[Issue]:
        raise NotImplementedError

    def finalize(self) -> Iterable[Issue]:
        return ()


class UniqueValidator[T](Validator):
    def __init__(self, key_description: str) -> None:
        super().__init__()
        self.key_description = key_description
        self.stations_by_key = defaultdict[T, list[Station]](list)

    @abstractmethod
    def extract_keys(self, station: Station) -> Iterable[T]:
        raise NotImplementedError

    def check_station(self, station: Station) -> Iterable[Issue]:
        for key in self.extract_keys(station):
            self.stations_by_key[key].append(station)
        return ()

    def finalize(self) -> Iterable[Issue]:
        for key, stations in self.stations_by_key.items():
            if len(stations) > 1:
                yield from Issue.many(stations, f"non-unique {self.key_description} {key!r}")


class UniqueIDValidator(UniqueValidator[str]):
    def __init__(self) -> None:
        super().__init__("id")

    def extract_keys(self, station: Station) -> Iterable[str]:
        yield station.id
        yield from station.other_ids


class UniqueNameValidator(UniqueValidator[str]):
    def __init__(self) -> None:
        super().__init__("name")

    def extract_keys(self, station: Station) -> Iterable[str]:
        yield station.name


class CountryValidator(Validator):
    REGEX = re.compile(r"^[A-Z]{2}$")

    def check_station(self, station: Station) -> Iterable[Issue]:
        if not self.REGEX.match(station.country):
            yield Issue(station, f"invalid country {station.country!r}")


class RailAttachmentValidator(Validator):
    EXEMPTED = frozenset(("0",))  # Exempt Lotnisko Modlin, as it's bus only

    def __init__(self, rail_nodes: Container[int]) -> None:
        super().__init__()
        self.rail_nodes = rail_nodes

    def check_station(self, station: Station) -> Iterable[Issue]:
        if station.id in self.EXEMPTED:
            return

        elif station.stop_positions:
            for stop_position in station.stop_positions:
                if stop_position.node_id not in self.rail_nodes:
                    yield Issue(stop_position, "not attached to a railway=rail way")

        else:
            # Check the station itself
            if station.node_id not in self.rail_nodes:
                yield Issue(
                    station,
                    "not attached to a railway=rail way (or missing stop positions)",
                )


class ChildEntityDistanceValidator(Validator):
    MAX_PLATFORM_DISTANCE = 300.0
    MAX_EXIT_DISTANCE = 500.0
    MAX_STOP_POSITION_DISTANCE = 300.0

    def check_station(self, station: Station) -> Iterable[Issue]:
        yield from self.check_children(station, station.platforms, self.MAX_PLATFORM_DISTANCE)
        yield from self.check_children(station, station.exits, self.MAX_EXIT_DISTANCE)
        yield from self.check_children(
            station,
            station.stop_positions,
            self.MAX_STOP_POSITION_DISTANCE,
        )

    def check_children(
        self,
        station: Station,
        children: Iterable[ChildEntity],
        max_distance: float,
    ) -> Iterable[Issue]:
        for child in children:
            d = earth_distance_m(station.lat, station.lon, child.lat, child.lon)
            if d > max_distance:
                yield Issue(child, f"too far away from station ({d:.1f} m)")


class UniquePlatformNameValidator(Validator):
    def check_station(self, station: Station) -> Iterable[Issue]:
        # Group platforms by name
        by_name = _group_by(station.platforms, attrgetter("name"))

        # Validate each named group
        for name, platforms in by_name.items():
            if len(platforms) <= 1:
                continue  # unique platform with the same name

            # Multiple platforms with the same name - check track numbers
            by_track = _group_by(platforms, attrgetter("track"))

            # Check against non-empty tracks
            if empty := by_track.get(""):
                yield from Issue.many(
                    empty,
                    f"missing track number - there are multiple {name!r} platforms",
                )

            # Check against duplicate tracks
            for track, tracks_platforms in by_track.items():
                if track and len(tracks_platforms) > 1:
                    yield from Issue.many(
                        tracks_platforms,
                        f"non-unique platform+track {name!r}+{track!r}",
                    )


class UselessStopPositionValidator(Validator):
    def check_station(self, station: Station) -> Iterable[Issue]:
        if len(station.stop_positions) == 1:
            yield Issue(
                station.stop_positions[0],
                "sole stop position - put railway=station directly on railway=rail",
            )


class StopPositionPlatformValidator(Validator):
    def check_station(self, station: Station) -> Iterable[Issue]:
        # Check that a stop_position has platforms
        for stop_position in station.stop_positions:
            if not stop_position.platforms:
                yield Issue(stop_position, "no platforms")

        # Group stop positions by referenced platforms
        by_platform = _group_by_many(station.stop_positions, attrgetter("platforms"))

        # Report non-unique platform hints
        for platform, stop_positions in by_platform.items():
            if len(stop_positions) > 1:
                for stop_position in stop_positions:
                    yield Issue(stop_position, f"non-unique platform hint {platform!r}")


class StopPositionTowardsValidator(Validator):
    def __init__(self, valid_stations: Container[str]) -> None:
        super().__init__()
        self.valid_stations = valid_stations

    def check_station(self, station: Station) -> Iterable[Issue]:
        by_hint = _group_by_many(station.stop_positions, attrgetter("towards"))

        # 1. Ensure a "fallback" stop position
        if station.stop_positions and not by_hint.get("fallback"):
            yield Issue(station, "no towards=fallback stop position")

        # 2. Ensure each hint is only used once
        # 3. Ensure each hint refers to a valid station
        for hint, stop_positions in by_hint.items():
            if len(stop_positions) > 1:
                yield from Issue.many(stop_positions, f"non-unique towards hint {hint!r}")

            if hint != "fallback" and hint not in self.valid_stations:
                yield from Issue.many(
                    stop_positions,
                    f"towards {hint!r} references a non-existing station",
                )


class BusStopDirectionValidator(Validator):
    def check_station(self, station: Station) -> Iterable[Issue]:
        # 1. If one stop - ensure no hints - and finish validating
        if len(station.bus_stops) == 1:
            bus_stop = station.bus_stops[0]
            if bus_stop.direction:
                yield Issue(bus_stop, "sole bus stop should not have any direction hints")
            return

        # 2. Ensure each hint is referenced only once
        by_hint = _group_by_many(station.bus_stops, attrgetter("direction"))
        for hint, bus_stops in by_hint.items():
            if len(bus_stops) > 1:
                yield from Issue.many(bus_stops, f"non-unique direction hint {hint!r}")

        # 3. Ensure at least one non-T hint is present
        if station.bus_stops and not self.has_direction_hints(by_hint):
            yield Issue(station, "bus stop with a non-T direction hint is missing")

    @staticmethod
    def has_direction_hints(hints: Collection[DirectionHint]) -> bool:
        return len(hints) >= 2 or (len(hints) == 1 and "T" not in hints)


class BusStopTowardsValidator(Validator):
    def __init__(self, valid_stations: Container[str]) -> None:
        super().__init__()
        self.valid_stations = valid_stations

    def check_station(self, station: Station) -> Iterable[Issue]:
        # 1. If one stop - ensure no hints - and finish validating
        if len(station.bus_stops) == 1:
            bus_stop = station.bus_stops[0]
            if bus_stop.towards:
                yield Issue(bus_stop, "sole bus stop should not have any towards hints")
            return

        # 2. Ensure each hint is referenced only once
        # 3. Ensure each hint references a valid station
        by_hint = _group_by_many(station.bus_stops, attrgetter("towards"))
        for hint, bus_stops in by_hint.items():
            if len(bus_stops) > 1:
                yield from Issue.many(bus_stops, f"non-unique towards hint {hint!r}")

            if hint not in self.valid_stations:
                yield from Issue.many(
                    bus_stops,
                    f"towards {hint!r} references a non-existing station",
                )

        # 4. Ensure either towards hint when no direction hints are present
        for bus_stop in station.bus_stops:
            if not bus_stop.direction and not bus_stop.towards:
                yield Issue(bus_stop, "no direction or towards hints")


def make_validators(
    valid_stations: Container[str] | None = None,
    rail_nodes: Container[int] | None = None,
) -> list[Validator]:
    validators = list[Validator]()
    validators.append(UniqueIDValidator())
    validators.append(UniqueNameValidator())
    if rail_nodes is not None:
        validators.append(RailAttachmentValidator(rail_nodes))
    validators.append(ChildEntityDistanceValidator())
    validators.append(UniquePlatformNameValidator())
    validators.append(UselessStopPositionValidator())
    validators.append(StopPositionPlatformValidator())
    if valid_stations is not None:
        validators.append(StopPositionTowardsValidator(valid_stations))
    validators.append(BusStopDirectionValidator())
    if valid_stations is not None:
        validators.append(BusStopTowardsValidator(valid_stations))
    return validators


def validate(file: StrPath) -> list[Issue]:
    stations, issues, rail_nodes = load_data(file, raise_on_errors=False)
    assert rail_nodes is not None

    validators = make_validators(stations, rail_nodes)
    for station in stations.values():
        for validator in validators:
            issues.extend(validator.check_station(station))
    for validator in validators:
        issues.extend(validator.finalize())

    return issues


def _group_by[T, K: Hashable](it: Iterable[T], key: Callable[[T], K]) -> defaultdict[K, list[T]]:
    grouped = defaultdict[K, list[T]](list)
    for i in it:
        grouped[key(i)].append(i)
    return grouped


def _group_by_many[T, K: Hashable](
    it: Iterable[T],
    keys: Callable[[T], Iterable[K]],
) -> defaultdict[K, list[T]]:
    grouped = defaultdict[K, list[T]](list)
    for i in it:
        for key in keys(i):
            grouped[key].append(i)
    return grouped


if __name__ == "__main__":
    from argparse import ArgumentParser
    from sys import exit

    arg_parser = ArgumentParser()
    arg_parser.add_argument("file", default="data/geo.osm", nargs="?")
    args = arg_parser.parse_args()

    issues = validate(args.file)
    for issue in issues:
        print(issue)
    exit(1 if issues else 0)
