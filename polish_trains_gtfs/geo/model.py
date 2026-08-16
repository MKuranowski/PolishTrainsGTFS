# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import chain
from typing import Literal, TypeAlias, TypeGuard, get_args

DirectionHint = Literal["N", "NE", "E", "SE", "S", "SW", "W", "NW", "*", "T"]
VALID_DIRECTION_HINTS: set[DirectionHint] = set(get_args(DirectionHint))


def is_valid_direction_hint(s: str) -> TypeGuard[DirectionHint]:
    return s in VALID_DIRECTION_HINTS


@dataclass
class BaseEntity:
    node_id: int
    lat: float
    lon: float


@dataclass
class Platform(BaseEntity):
    name: str
    track: str = ""
    accessible: bool | None = field(default=None, repr=False)


@dataclass
class Exit(BaseEntity):
    name: str
    accessible: bool | None = field(default=None, repr=False)
    platforms: list[str] = field(default_factory=list[str], repr=False)


@dataclass
class StopPosition(BaseEntity):
    platforms: list[str] = field(default_factory=list[str])
    towards: list[str] = field(default_factory=list[str], repr=False)


@dataclass
class BusStop(BaseEntity):
    direction: list[DirectionHint] = field(default_factory=list[DirectionHint])
    towards: list[str] = field(default_factory=list[str])


@dataclass
class Station(BaseEntity):
    id: str
    name: str
    accessible: bool | None = field(default=None, repr=False)
    translations: dict[str, str] = field(default_factory=dict[str, str], repr=False)
    other_ids: list[str] = field(default_factory=list[str])

    country: str = "PL"
    request_stop: bool = False
    waypoint: bool = False

    platforms: list[Platform] = field(default_factory=list[Platform], repr=False)
    exits: list[Exit] = field(default_factory=list[Exit], repr=False)
    stop_positions: list[StopPosition] = field(default_factory=list[StopPosition], repr=False)
    bus_stops: list[BusStop] = field(default_factory=list[BusStop], repr=False)

    def all_ids(self) -> Iterable[str]:
        yield self.id
        yield from self.other_ids

    def all_child_entities(self) -> Iterable["ChildEntity"]:
        return chain(self.platforms, self.exits, self.stop_positions, self.bus_stops)


ChildEntity: TypeAlias = Platform | Exit | StopPosition | BusStop
Entity: TypeAlias = Station | ChildEntity
