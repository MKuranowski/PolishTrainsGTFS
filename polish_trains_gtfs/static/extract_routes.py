# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, NamedTuple, NotRequired, TypedDict, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Trip

SelectorConfig = Mapping[str, Any]


class RouteConfig(TypedDict):
    route_code: str
    select: Sequence[SelectorConfig]


class AgencyConfig(TypedDict):
    disregard_stops_up_to: NotRequired[str | int]
    routes: Sequence[RouteConfig]


Config = Mapping[str, AgencyConfig]


class Assignment(NamedTuple):
    trip_id: str
    agency_id: str
    route_code: str

    @property
    def route_id(self) -> str:
        return f"{self.agency_id}_{self.route_code}"


class Selector(ABC):
    route_code: str

    def __init__(self, route_code: str) -> None:
        self.route_code = route_code

    def and_(self, other: "Selector") -> "Selector":
        return CombinedSelector(self.route_code, (self, other))

    @abstractmethod
    def requires_stops(self) -> bool: ...

    @abstractmethod
    def matches(self, t: Trip, stops: Iterable[str]) -> str | None: ...


class CombinedSelector(Selector):
    selectors: list[Selector]

    def __init__(self, route_code: str, selectors: Iterable[Selector] = ()) -> None:
        super().__init__(route_code)
        self.selectors = list(selectors)

    def and_(self, other: Selector) -> "CombinedSelector":
        self.selectors.append(other)
        return self

    def requires_stops(self) -> bool:
        return any(i.requires_stops() for i in self.selectors)

    def matches(self, t: Trip, stops: Iterable[str]) -> str | None:
        code: str | None = None
        for s in self.selectors:
            if (m := s.matches(t, stops)) is not None:
                code = code or self.route_code or m
            else:
                return None  # short circuit
        return code


class AnySelector(Selector):
    def and_[T: Selector](self, other: T) -> T:
        return other

    def requires_stops(self) -> bool:
        return False

    def matches(self, t: Trip, stops: Iterable[str]) -> str | None:
        return self.route_code


class RetainSelector(Selector):
    def __init__(self) -> None:
        super().__init__("")

    def requires_stops(self) -> bool:
        return False

    def matches(self, t: Trip, stops: Iterable[str]) -> str | None:
        return t.route_id.partition("_")[2]


class NameSelector(Selector):
    def __init__(self, name_pattern: str, route_code: str) -> None:
        super().__init__(route_code)
        self.name_pattern = re.compile(name_pattern)

    def requires_stops(self) -> bool:
        return False

    def matches(self, t: Trip, stops: Iterable[str]) -> str | None:
        name = t.get_extra_field("plk_train_name") or ""
        if m := self.name_pattern.search(name):
            return m.expand(self.route_code)
        return None


class PassesThroughSelector(Selector):
    def __init__(self, required_stops: set[str], route_code: str) -> None:
        super().__init__(route_code)
        self.required_stops = required_stops

    def requires_stops(self) -> bool:
        return True

    def matches(self, t: Trip, stops: Iterable[str]) -> str | None:
        if self.required_stops.issubset(stops):
            return self.route_code
        return None


def create_selector_from_config(code: str, cfg: SelectorConfig) -> Selector:
    s: Selector = AnySelector(code)
    unused_keys = set(cfg.keys())

    if cfg.get("retain"):
        unused_keys.discard("retain")
        s = s.and_(RetainSelector())

    if name_pattern := cfg.get("name"):
        unused_keys.discard("name")
        s = s.and_(NameSelector(name_pattern, code))

    if required_stops := cfg.get("passes_through"):
        unused_keys.discard("passes_through")
        s = s.and_(PassesThroughSelector({str(i) for i in required_stops}, code))

    if unused_keys:
        raise ValueError(f"Unknown selector keys: {', '.join(sorted(unused_keys))}")
    return s


class ExtractRoutes(Task):
    def __init__(self, r: str = "route_extract.yaml") -> None:
        super().__init__()
        self.r = r
        self.leftover = list[Trip]()

    def execute(self, r: TaskRuntime) -> None:
        self.leftover.clear()
        config = cast(Config, r.resources[self.r].yaml())
        assignments = list[Assignment]()

        for agency_id, agency_config in config.items():
            assignments.extend(self.assign_trips_for_agency(r.db, agency_id, agency_config))

        with r.db.transaction():
            self.run_assignments(r.db, assignments)
        self.check_leftover()

    def assign_trips_for_agency(
        self,
        db: DBConnection,
        agency_id: str,
        cfg: AgencyConfig,
    ) -> Iterable[Assignment]:
        selectors = self.create_selectors(cfg["routes"])
        requires_stops = any(i.requires_stops() for i in selectors)
        disregard_stops_up_to = str(cfg.get("disregard_stops_up_to", ""))
        to_curate = self.get_trips_to_curate(db, agency_id, requires_stops, disregard_stops_up_to)

        for trip, stops in to_curate:
            for selector in selectors:
                if (route_code := selector.matches(trip, stops)) is not None:
                    yield Assignment(trip.id, agency_id, route_code)
                    break
            else:
                self.leftover.append(trip)

    def create_selectors(self, configs: Iterable[RouteConfig]) -> list[Selector]:
        return [
            create_selector_from_config(r["route_code"], s) for r in configs for s in r["select"]
        ]

    def get_trips_to_curate(
        self,
        db: DBConnection,
        agency_id: str,
        requires_stops: bool = True,
        disregard_stops_up_to: str = "",
    ) -> Iterable[tuple[Trip, list[str]]]:
        q = db.typed_out_execute(
            "SELECT * FROM trips WHERE "
            "(SELECT agency_id FROM routes WHERE trips.route_id = routes.route_id) = ?",
            Trip,
            (agency_id,),
        )
        for trip in q:
            stops = (
                self.get_stops_of_trip(db, trip.id, disregard_stops_up_to) if requires_stops else []
            )
            yield trip, stops

    def get_stops_of_trip(
        self,
        db: DBConnection,
        trip_id: str,
        disregard_up_to: str = "",
    ) -> list[str]:
        stops = [
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT stop_id FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence ASC",
                (trip_id,),
            )
        ]
        if disregard_up_to:
            disregard_idx = index_of(stops, disregard_up_to)
            disregard_is_last = disregard_idx == len(stops) - 1
            if disregard_idx is not None and not disregard_is_last:
                return stops[disregard_idx:]
        return stops

    def run_assignments(self, db: DBConnection, assignments: Sequence[Assignment]) -> None:
        db.raw_execute_many(
            "INSERT OR IGNORE INTO routes (agency_id, route_id, short_name, long_name, type) "
            "VALUES (?, ?, ?, '', 2)",
            ((i.agency_id, i.route_id, i.route_code) for i in assignments),
        )
        db.raw_execute_many(
            "UPDATE trips SET route_id = ? WHERE trip_id = ?",
            ((i.route_id, i.trip_id) for i in assignments),
        )
        db.raw_execute(
            "DELETE FROM routes WHERE NOT EXISTS "
            "(SELECT 1 FROM trips WHERE trips.route_id = routes.route_id)"
        )

    def check_leftover(self) -> None:
        if self.leftover:
            raise MultipleDataErrors(
                "route extraction",
                [DataError(f"no route extracted for trip {i}") for i in self.leftover],
            )


def index_of[T](s: Sequence[T], elem: T) -> int | None:
    try:
        return s.index(elem)
    except ValueError:
        return None
