# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any, cast

import osmium
import osmium.filter
import osmium.osm
import routx
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime
from impuls.tools.types import StrPath

# TODO: Add support for bus shapes


class GenerateShapes(Task):
    def __init__(self, graph_resource: str, extra_config_resource: str | None = None) -> None:
        super().__init__()
        self.graph_resource = graph_resource
        self.extra_config_resource = extra_config_resource

    def execute(self, r: TaskRuntime) -> None:
        osm_path = r.resources[self.graph_resource].stored_at
        extra_config = (  # type: ignore
            r.resources[self.extra_config_resource].yaml() if self.extra_config_resource else {}
        )

        # 1. Load the graph
        self.logger.info("Loading routing graph from %s", self.graph_resource)
        graph = self.load_graph(osm_path)

        # 2. Load per-station stop positions
        self.logger.info("Loading stop positions from %s", self.graph_resource)
        stop_positions = self.load_stop_positions(osm_path)

        # 3. Select trips to generate shapes for
        self.logger.info("Selecting trips")
        trip_ids = self.select_trips(r.db)

        # 4. Match each trips' stops with nodes
        self.logger.debug("Matching %d trips with nodes", len(trip_ids))
        force_via = self._load_force_via(graph, extra_config)
        trips = [self.match_trip(trip_id, r.db, stop_positions, force_via) for trip_id in trip_ids]

        # 5. Group trips by the same sequence of nodes
        self.logger.debug("Grouping trips with the same shape")
        grouped_trips = defaultdict[tuple[_MatchedNode, ...], list[str]](list)
        for trip_id, nodes in trips:
            grouped_trips[nodes].append(trip_id)

        # 6. Generate shapes for every unique sequence of nodes
        self.logger.info("Generating %d shapes", len(grouped_trips))
        with r.db.transaction():
            for shape_id, (nodes, trips) in enumerate(grouped_trips.items()):
                if (shape_id + 1) % 50 == 0:
                    self.logger.debug(
                        "Generated %d / %d (%.2f %%) shapes",
                        shape_id,
                        len(grouped_trips),
                        100 * shape_id / len(grouped_trips),
                    )

                shape, distances = self.generate_shape(graph, nodes)
                r.db.raw_execute("INSERT INTO shapes (shape_id) VALUES (?)", (shape_id,))
                r.db.raw_execute_many(
                    "INSERT INTO shape_points (shape_id, sequence, lat, lon, shape_dist_traveled) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        (shape_id, idx, round(lat, 6), round(lon, 6), round(dist, 3))
                        for idx, (lat, lon, dist) in enumerate(shape)
                    ),
                )
                r.db.raw_execute_many(
                    "UPDATE trips SET shape_id = ? WHERE trip_id = ?",
                    ((shape_id, trip_id) for trip_id in trips),
                )
                r.db.raw_execute_many(
                    "UPDATE stop_times SET shape_dist_traveled = ? "
                    "WHERE trip_id = ? AND stop_sequence = ?",
                    (
                        (round(dist, 3), trip_id, stop_seq)
                        for trip_id in trips
                        for stop_seq, dist in distances.items()
                    ),
                )

    def load_graph(self, osm_path: StrPath) -> routx.Graph:
        g = routx.Graph()
        g.add_from_osm_file(
            osm_path,
            routx.OsmProfile.RAILWAY,
            format=routx.OsmFormat.XML,
        )
        return g

    def load_stop_positions(self, osm_path: StrPath) -> "_StopPositions":
        stop_positions = _StopPositions(list)
        self._load_specific_stop_positions(osm_path, stop_positions)
        self._load_generic_stop_positions(osm_path, stop_positions)
        return stop_positions

    @staticmethod
    def _load_specific_stop_positions(osm_path: StrPath, stop_positions: "_StopPositions") -> None:
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

            stop_positions[station_id].append(_StopPosition(node.id, towards, platforms))

    @staticmethod
    def _load_generic_stop_positions(osm_path: StrPath, stop_positions: "_StopPositions") -> None:
        fp = (
            osmium.FileProcessor(osm_path)
            .with_filter(osmium.filter.EntityFilter(osmium.osm.NODE))
            .with_filter(osmium.filter.TagFilter(("railway", "station")))
        )

        for node in fp:
            assert isinstance(node, osmium.osm.Node)

            station_id = node.tags.get("ref") or ""
            if station_id and station_id not in stop_positions:
                stop_positions[station_id] = [_StopPosition(node.id)]

    @staticmethod
    def _load_force_via(graph: routx.Graph, extra_config: Any) -> dict[tuple[str, str], int]:
        force_via_config = extra_config.get("force_via", [])
        if not force_via_config:
            return {}

        kd_tree = routx.KDTree.build(graph)
        force_via = dict[tuple[str, str], int]()

        for cfg in force_via_config:
            via_node = kd_tree.find_nearest_node(*cfg["via"]).id  # type: ignore
            force_via[cfg["from"], cfg["to"]] = via_node

        return force_via

    def select_trips(self, db: DBConnection) -> list[str]:
        with db.raw_execute(
            "SELECT trip_id FROM trips LEFT JOIN routes USING (route_id) WHERE routes.type = 2",
        ) as q:
            return [cast(str, i[0]) for i in q]

    def match_trip(
        self,
        trip_id: str,
        db: DBConnection,
        stop_positions: "_StopPositions",
        force_via: Mapping[tuple[str, str], int],
    ) -> "_MatchedTrip":
        # Retrieve all stop_times of the trip
        with db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence ASC",
            StopTime,
            (trip_id,),
        ) as q:
            stop_times = list(q)

        # Match each stop with a node in the graph
        nodes = list["_MatchedNode"]()
        for i, stop_time in enumerate(stop_times):
            # Check and insert a forced via node
            if i > 0:
                prev_station = _extract_station_id(stop_times[i - 1].stop_id)
                station = _extract_station_id(stop_time.stop_id)
                if via_node := force_via.get((prev_station, station)):
                    nodes.append(_MatchedNode(via_node))

            # Insert a node for the stop_time
            nodes.append(self.match_node(stop_times, i, stop_positions))

        return trip_id, tuple(nodes)

    @staticmethod
    def match_node(
        stop_times: Sequence[StopTime],
        i: int,
        stop_positions: "_StopPositions",
    ) -> "_MatchedNode":
        stop_time = stop_times[i]
        station_id = _extract_station_id(stop_time.stop_id)
        candidates = stop_positions[station_id]

        # Fast track stations with single node
        if len(candidates) <= 1:
            return _MatchedNode(candidates[0].node_id, stop_time.stop_sequence)

        # Try to match on platform
        for candidate in candidates:
            if stop_time.platform in candidate.platforms:
                return _MatchedNode(candidate.node_id, stop_time.stop_sequence)

        # Try to match on "towards"
        prev_station_id = _extract_station_id(stop_times[i - 1].stop_id) if i >= 1 else None
        next_station_id = (
            _extract_station_id(stop_times[i + 1].stop_id) if (i + 1) < len(stop_times) else None
        )
        for candidate in candidates:
            if next_station_id in candidate.towards or prev_station_id in candidate.towards:
                return _MatchedNode(candidate.node_id, stop_time.stop_sequence)

        # Use the fallback candidate
        for candidate in candidates:
            if candidate.is_fallback():
                return _MatchedNode(candidate.node_id, stop_time.stop_sequence)

        # No fallback stop_position - raise error
        raise ValueError(f"no fallback public_transport=stop_position at station {station_id}")

    def generate_shape(
        self,
        graph: routx.Graph,
        nodes: Iterable["_MatchedNode"],
    ) -> tuple[list[tuple[float, float, float]], dict[int, float]]:
        shape = list[tuple[float, float, float]]()
        distances = dict[int, float]()
        total_distance = 0.0

        for i, (from_, to) in enumerate(pairwise(nodes)):
            # Record the distance to the first stop
            if i == 0:
                assert from_.stop_sequence is not None
                distances[from_.stop_sequence] = 0.0

            # Generate the shape for the leg
            leg_nodes = self.generate_shape_leg(graph, from_.node_id, to.node_id)

            # Skip first node, as it's the same as previous leg's last node,
            # except for the very first leg
            offset = 0 if i == 0 else 1

            # Save the points of the shape
            for node_id in leg_nodes[offset:]:
                node = graph[node_id]
                lat = node.lat
                lon = node.lon

                if shape:
                    prev_lat, prev_lon, _ = shape[-1]
                    total_distance += routx.earth_distance(lat, lon, prev_lat, prev_lon)

                shape.append((lat, lon, total_distance))

            # Record the distance to the stop
            if to.stop_sequence is not None:
                distances[to.stop_sequence] = total_distance

        return shape, distances

    def generate_shape_leg(self, graph: routx.Graph, from_: int, to: int) -> list[int]:
        try:
            nodes = graph.find_route(from_, to, without_turn_around=False)
        except routx.StepLimitExceeded:
            nodes = []

        if not nodes:
            self.logger.error("No shape between nodes %d and %d", from_, to)
            return [from_, to]

        return nodes


_MatchedTrip = tuple[str, tuple["_MatchedNode", ...]]

_StopPositions = defaultdict[str, list["_StopPosition"]]


@dataclass
class _StopPosition:
    node_id: int
    towards: set[str] = field(default_factory=set[str])
    platforms: set[str] = field(default_factory=set[str])

    def is_fallback(self) -> bool:
        return not self.towards


@dataclass(frozen=True)
class _MatchedNode:
    node_id: int
    stop_sequence: int | None = None


def _unpack_osm_list(value: str, separator: str = ";") -> list[str]:
    return value.split(separator) if value else []


def _extract_station_id(x: str) -> str:
    return x.partition("_")[0]
