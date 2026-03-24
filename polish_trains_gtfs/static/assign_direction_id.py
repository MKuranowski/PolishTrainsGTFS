# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import deque
from collections.abc import Container, Iterable, Sequence
from itertools import pairwise
from typing import NotRequired, Self, TypedDict, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors

from .util import describe


class Config(TypedDict):
    agencies: NotRequired[Sequence[str]]
    routes: NotRequired[Sequence[str]]
    outbound_pairs: Sequence[Sequence[str | int]]
    pairs_to_ignore: NotRequired[Sequence[Sequence[str | int]]]
    force_for_conflicting: NotRequired[bool]


class AssignDirectionID(Task):
    MAX_TRIES = 5

    def __init__(self, r: str = "directions.yaml") -> None:
        super().__init__()
        self.r = r
        self.leftover = list[str]()

    def clear(self) -> None:
        self.leftover.clear()

    def execute(self, r: TaskRuntime) -> None:
        self.clear()
        configs = cast(Sequence[Config], r.resources[self.r].yaml())

        for config in configs:
            self.logger.info("Assigning directions for %s", _describe_config(config))
            self.assign(r.db, config)

        self._check_leftovers(r.db)

    def assign(self, db: DBConnection, config: Config) -> None:
        outbound_pairs = _parse_config_pairs(config["outbound_pairs"])
        ignored_pairs = _parse_config_pairs(config.get("pairs_to_ignore", []), add_backwards=True)

        all_trips = list(_trips_of_config(db, config))
        queue = deque(_QueueItem.from_db(trip_id, db) for trip_id in all_trips)
        conflicting = list[_QueueItem]()
        assigned_directions = list[tuple[int, str]]()

        # Attempt to match trains from the queue with the existing outbound pairs
        while queue:
            item = queue.popleft()
            item.tries += 1

            # Compare how many stop pairs follow known outbound_pairs forward and backward
            item_outbound, item_inbound = item.count_pairs(outbound_pairs)
            match item_outbound, item_inbound:
                # No intersection after a couple of expansions - bail out
                case 0, 0 if item.tries > self.MAX_TRIES:
                    self.leftover.append(item.trip_id)

                # No intersection - put at the end of queue, hoping further pair inferences
                # help when the item is expanded next time
                case 0, 0:
                    queue.append(item)

                # Some pairs outbound, no inbound - infer outbound direction,
                # and expand known outbound pairs
                case _, 0:
                    assigned_directions.append((0, item.trip_id))
                    outbound_pairs.update(
                        i for i in item.get_forward_pairs() if i not in ignored_pairs
                    )

                # Some pairs inbound, no outbound - infer inbound direction,
                # and expand known outbound pairs
                case 0, _:
                    assigned_directions.append((1, item.trip_id))
                    outbound_pairs.update(
                        i for i in item.get_backward_pairs() if i not in ignored_pairs
                    )

                # Some pairs outbound, some inbound - mark as conflicting and drop from queue
                case _, _:
                    conflicting.append(item)

        # Attempt to force-match conflicting trains by inferring outbound
        # if more pairs match with known outbound pairs.
        if conflicting and config.get("force_for_conflicting", False):
            # TODO: Respect original pairs (directly from config) more than
            #       the inferred pairs from `outbound_pairs`.
            for item in conflicting:
                item_outbound, item_inbound = item.count_pairs(outbound_pairs)
                direction = 0 if item_outbound >= item_inbound else 1
                self.logger.debug(
                    "Forcing direction %d for trip %s",
                    direction,
                    describe.trip(db, item.trip_id),
                )
                assigned_directions.append((direction, item.trip_id))

            self.logger.warning(
                "Forced direction for %d / %d (%.2f %%) trains",
                len(conflicting),
                len(all_trips),
                100 * len(conflicting) / len(all_trips),
            )

        else:
            self.leftover.extend(i.trip_id for i in conflicting)

        # Commit inferred directions
        with db.transaction():
            db.raw_execute_many(
                "UPDATE trips SET direction = ? WHERE trip_id = ?",
                assigned_directions,
            )

    def _check_leftovers(self, db: DBConnection) -> None:
        if self.leftover:
            raise MultipleDataErrors(
                f"direction_id assignment with {self.MAX_TRIES} max attempts",
                [DataError(f"no direction for {describe.trip(db, i)}") for i in self.leftover],
            )


class _QueueItem:
    def __init__(self, trip_id: str, stops: Iterable[str]) -> None:
        self.trip_id = trip_id
        self.stops = list(stops)
        self.tries = 0

    def count_pairs(self, outbound_pairs: Container[tuple[str, str]]) -> tuple[int, int]:
        outbound = 0
        inbound = 0

        for a, b in pairwise(self.stops):
            outbound += (a, b) in outbound_pairs
            inbound += (b, a) in outbound_pairs

        return outbound, inbound

    def get_forward_pairs(self) -> Iterable[tuple[str, str]]:
        return pairwise(self.stops)

    def get_backward_pairs(self) -> Iterable[tuple[str, str]]:
        yield from ((b, a) for a, b in pairwise(self.stops))

    @classmethod
    def from_db(cls, trip_id: str, db: DBConnection) -> Self:
        with db.raw_execute(
            "SELECT stop_id FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence ASC",
            (trip_id,),
        ) as query:
            return cls(trip_id, (cast(str, i[0]) for i in query))


def _trips_of_config(db: DBConnection, c: Config) -> Iterable[str]:
    routes = list[str]()

    # First, transform all agency_ids into routes
    for agency_id in c.get("agencies", tuple()):
        with db.raw_execute("SELECT route_id FROM routes WHERE agency_id = ?", (agency_id,)) as q:
            routes.extend(cast(str, r[0]) for r in q)

    # Second, add all explicit routes
    routes.extend(c.get("routes", tuple()))

    # Third, generate trip_ids
    for route_id in routes:
        with db.raw_execute("SELECT trip_id FROM trips WHERE route_id = ?", (route_id,)) as q:
            yield from (cast(str, r[0]) for r in q)


def _parse_config_pairs(
    pairs: Sequence[Sequence[str | int]],
    add_backwards: bool = False,
) -> set[tuple[str, str]]:
    parsed = set[tuple[str, str]]()
    for pair in pairs:
        if len(pair) != 2:
            raise ValueError("outbound_pairs entries must have exactly 2 elements")
        a = str(pair[0])
        b = str(pair[1])
        parsed.add((a, b))
        if add_backwards:
            parsed.add((b, a))
    return parsed


def _describe_config(c: Config) -> dict[str, Sequence[str]]:
    r = dict[str, Sequence[str]]()
    if agencies := c.get("agencies"):
        r["agencies"] = agencies
    if routes := c.get("routes"):
        r["routes"] = routes
    return r
