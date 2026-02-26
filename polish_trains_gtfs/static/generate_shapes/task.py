# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections import defaultdict
from collections.abc import Sequence
from typing import Any, cast

import routx
from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Route, Stop, StopTime, Trip
from impuls.tools.types import StrPath

from .generator import ShapeGenerator
from .matcher import BusMatcher, Matcher, TrainMatcher
from .model import MatchedNode, MatchedTrip


class GenerateShapes(Task):
    def __init__(
        self,
        graph_resource: str,
        extra_config_resource: str | None = None,
    ) -> None:
        super().__init__()
        self.graph_resource = graph_resource
        self.extra_config_resource = extra_config_resource

    def execute(self, r: TaskRuntime) -> None:
        # 1. Load resources
        osm_path = r.resources[self.graph_resource].stored_at
        extra_config = (  # type: ignore
            r.resources[self.extra_config_resource].yaml() if self.extra_config_resource else {}
        )

        # 2. Load the graph
        self.logger.info("Loading routing graph from %s", self.graph_resource)
        graph = self.load_graph(osm_path)

        # 3. Load the trip_matcher
        self.logger.info("Loading trip matching lookup table")
        matcher = self.load_matcher(graph, r.db, osm_path, extra_config)

        # 4. Select trips to generate shapes for
        self.logger.info("Selecting trips")
        unmatched_trips = self.select_trips(r.db)

        # 5. Match each trips' stops with nodes
        self.logger.debug("Matching %d trips with nodes", len(unmatched_trips))
        matched_trips = self.match_trips(r.db, matcher, unmatched_trips)
        self.logger.debug(
            "Matched %d / %d (%.2f %%) trips",
            len(unmatched_trips),
            len(matched_trips),
            100 * len(matched_trips) / len(unmatched_trips),
        )
        if not matched_trips:
            return  # nothing to do

        # 6. Group trips by the same sequence of nodes
        self.logger.debug("Grouping trips with the same shape")
        grouped_trips = defaultdict[tuple[MatchedNode, ...], list[str]](list)
        for matched_trip in matched_trips:
            grouped_trips[matched_trip.nodes].append(matched_trip.trip_id)

        # 7. Generate shapes for every unique sequence of nodes
        self.logger.info("Generating %d shapes", len(grouped_trips))
        generator = ShapeGenerator(graph, self.get_shape_id_prefix())
        with r.db.transaction():
            for shape_id, (nodes, trips) in enumerate(grouped_trips.items()):
                if (shape_id + 1) % 50 == 0:
                    self.logger.debug(
                        "Generated %d / %d (%.2f %%) shapes",
                        shape_id,
                        len(grouped_trips),
                        100 * shape_id / len(grouped_trips),
                    )

                shape = generator.generate(nodes)
                shape.insert_into(r.db)
                shape.apply_for_many(r.db, trips)

    def load_graph(self, osm_path: StrPath) -> routx.Graph:
        g = routx.Graph()
        g.add_from_osm_file(
            osm_path,
            self.get_routx_profile(),
            format=routx.OsmFormat.XML,
        )
        return g

    def get_routx_profile(self) -> routx.OsmProfile:
        return routx.OsmProfile.RAILWAY

    def load_matcher(
        self,
        graph: routx.Graph,
        db: DBConnection,
        osm_path: StrPath,
        extra_config: Any,
    ) -> Matcher:
        m = self.create_matcher()
        with db.retrieve_all(Stop) as stops:
            m.load(graph, stops, osm_path, extra_config)
        return m

    def create_matcher(self) -> Matcher:
        return TrainMatcher()

    def select_trips(self, db: DBConnection) -> list[Trip]:
        return _select_trips_with_route_type(db, Route.Type.RAIL)

    def match_trips(
        self,
        db: DBConnection,
        matcher: Matcher,
        trips: Sequence[Trip],
    ) -> list[MatchedTrip]:
        return [m for trip in trips if (m := matcher.match(trip, _get_stop_times(db, trip.id)))]

    def get_shape_id_prefix(self) -> str:
        return "RAIL_"


class GenerateBusShapes(GenerateShapes):
    def get_routx_profile(self) -> routx.OsmProfile:
        return routx.OsmProfile.BUS

    def select_trips(self, db: DBConnection) -> list[Trip]:
        return _select_trips_with_route_type(db, Route.Type.BUS)

    def create_matcher(self) -> Matcher:
        return BusMatcher()

    def get_shape_id_prefix(self) -> str:
        return "BUS_"


def _select_trips_with_route_type(db: DBConnection, typ: Route.Type) -> list[Trip]:
    with db.raw_execute("SELECT route_id FROM routes WHERE type = ?", (typ.value,)) as q:
        route_ids = [cast(str, i[0]) for i in q]

    trips = list[Trip]()
    for id in route_ids:
        with db.typed_out_execute("SELECT * FROM trips WHERE route_id = ?", Trip, (id,)) as q:
            trips.extend(q)
    return trips


def _get_stop_times(db: DBConnection, trip_id: str) -> list[StopTime]:
    with db.typed_out_execute(
        "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence ASC",
        StopTime,
        (trip_id,),
    ) as query:
        return list(query)
