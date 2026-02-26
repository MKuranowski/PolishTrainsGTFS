# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import osmium
import osmium.filter
import osmium.osm
import routx
from impuls.model import Stop, StopTime, Trip
from impuls.tools.types import StrPath

from .model import MatchedNode, MatchedTrip


class Matcher(Protocol):
    def load(
        self,
        graph: routx.Graph,
        stops: Iterable[Stop],
        osm_path: StrPath,
        extra_config: Any,
    ) -> None: ...
    def match(self, trip: Trip, stop_times: Sequence[StopTime]) -> MatchedTrip | None: ...


@dataclass
class TrainStopPosition:
    node_id: int
    towards: set[str] = field(default_factory=set[str])
    platforms: set[str] = field(default_factory=set[str])

    def is_fallback(self) -> bool:
        return not self.towards


class TrainMatcher(Matcher):
    def __init__(self) -> None:
        self.force_via = dict[tuple[str, str], int]()
        self.stop_positions = defaultdict[str, list[TrainStopPosition]](list)

    def load(
        self,
        graph: routx.Graph,
        stops: Iterable[Stop],
        osm_path: StrPath,
        extra_config: Any,
    ) -> None:
        self.force_via.clear()
        self.stop_positions.clear()

        self.load_force_via(routx.KDTree.build(graph), extra_config)
        self.load_specific_stop_positions(osm_path)
        self.load_generic_stop_positions(osm_path)

    def load_force_via(self, kd_tree: routx.KDTree, extra_config: Any) -> None:
        self.force_via.clear()
        for cfg in extra_config.get("force_via", []):
            self.force_via[cfg["from"], cfg["to"]] = kd_tree.find_nearest_node(*cfg["via"]).id

    def load_specific_stop_positions(self, osm_path: StrPath) -> None:
        fp = (
            osmium.FileProcessor(osm_path)
            .with_filter(osmium.filter.EntityFilter(osmium.osm.NODE))
            .with_filter(osmium.filter.TagFilter(("public_transport", "stop_position")))
        )

        for node in fp:
            assert isinstance(node, osmium.osm.Node)

            station_id = node.tags.get("ref:station") or ""
            if not station_id:
                continue

            platforms = set(_unpack_osm_list(node.tags.get("platforms") or ""))
            match node.tags.get("towards"):
                case "fallback" | "" | None:
                    towards = set[str]()
                case lst:
                    towards = set(_unpack_osm_list(lst))

            self.stop_positions[station_id].append(TrainStopPosition(node.id, towards, platforms))

    def load_generic_stop_positions(self, osm_path: StrPath) -> None:
        fp = (
            osmium.FileProcessor(osm_path)
            .with_filter(osmium.filter.EntityFilter(osmium.osm.NODE))
            .with_filter(osmium.filter.TagFilter(("railway", "station")))
        )

        for node in fp:
            assert isinstance(node, osmium.osm.Node)

            station_id = node.tags.get("ref") or ""
            if station_id and station_id not in self.stop_positions:
                self.stop_positions[station_id] = [TrainStopPosition(node.id)]

    def match(self, trip: Trip, stop_times: Sequence[StopTime]) -> MatchedTrip:
        nodes = list[MatchedNode]()
        for i, stop_time in enumerate(stop_times):
            # Check and insert a forced via node
            if i > 0:
                prev_station = _extract_station_id(stop_times[i - 1].stop_id)
                station = _extract_station_id(stop_time.stop_id)
                if via_node := self.force_via.get((prev_station, station)):
                    nodes.append(MatchedNode(via_node))

            # Insert a node for the stop_time
            nodes.append(self.match_node(stop_times, i))

        return MatchedTrip(trip.id, tuple(nodes))

    def match_node(self, stop_times: Sequence[StopTime], i: int) -> MatchedNode:
        stop_time = stop_times[i]
        station_id = _extract_station_id(stop_time.stop_id)
        candidates = self.stop_positions[station_id]

        # Fast track stations with single node
        if len(candidates) <= 1:
            return MatchedNode(candidates[0].node_id, stop_time.stop_sequence)

        # Try to match on platform
        for candidate in candidates:
            if stop_time.platform in candidate.platforms:
                return MatchedNode(candidate.node_id, stop_time.stop_sequence)

        # Try to match on "towards"
        prev_station_id = _extract_station_id(stop_times[i - 1].stop_id) if i >= 1 else None
        next_station_id = (
            _extract_station_id(stop_times[i + 1].stop_id) if (i + 1) < len(stop_times) else None
        )
        for candidate in candidates:
            if next_station_id in candidate.towards or prev_station_id in candidate.towards:
                return MatchedNode(candidate.node_id, stop_time.stop_sequence)

        # Use the fallback candidate
        for candidate in candidates:
            if candidate.is_fallback():
                return MatchedNode(candidate.node_id, stop_time.stop_sequence)

        # No fallback stop_position - raise error
        raise ValueError(f"no fallback public_transport=stop_position at station {station_id}")


class BusMatcher(Matcher):
    MAX_DISTANCE_M = 50.0

    def __init__(self) -> None:
        self.stop_to_node = dict[str, int]()

    def load(
        self,
        graph: routx.Graph,
        stops: Iterable[Stop],
        osm_path: StrPath,
        extra_config: Any,
    ) -> None:
        kd_tree = routx.KDTree.build(graph)
        self.stop_to_node.clear()
        for stop in stops:
            if "BUS" in stop.id or stop.id == "0":
                candidate = kd_tree.find_nearest_node(stop.lat, stop.lon)
                distance = routx.earth_distance(stop.lat, stop.lon, candidate.lat, candidate.lon)
                if distance <= self.MAX_DISTANCE_M:
                    self.stop_to_node[stop.id] = candidate.id

    def match(self, trip: Trip, stop_times: Sequence[StopTime]) -> MatchedTrip | None:
        matched_nodes = list[MatchedNode]()
        for stop_time in stop_times:
            if node_id := self.stop_to_node.get(stop_time.stop_id):
                matched_nodes.append(MatchedNode(node_id, stop_time.stop_sequence))
            else:
                return None  # Don't generate a shape without a matched node in the graph
        return MatchedTrip(trip.id, tuple(matched_nodes))


def _unpack_osm_list(value: str, separator: str = ";") -> list[str]:
    return value.split(separator) if value else []


def _extract_station_id(x: str) -> str:
    return x.partition("_")[0]
