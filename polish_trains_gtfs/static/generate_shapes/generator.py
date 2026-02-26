# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import logging
from collections.abc import Iterable
from itertools import pairwise

import routx

from .model import GeneratedShape, MatchedNode, Point

logger = logging.getLogger("ShapeGenerator")


class ShapeGenerator:
    def __init__(self, graph: routx.Graph, shape_id_prefix: str = "") -> None:
        self.graph = graph
        self.shape_id_counter = 0
        self.shape_id_prefix = shape_id_prefix

    def _get_next_shape_id(self) -> str:
        id = f"{self.shape_id_prefix}{self.shape_id_counter}"
        self.shape_id_counter += 1
        return id

    def generate(self, nodes: Iterable[MatchedNode]) -> GeneratedShape:
        shape = GeneratedShape(self._get_next_shape_id())
        total_distance = 0.0

        for i, (from_, to) in enumerate(pairwise(nodes)):
            # Record the distance to the first stop
            if i == 0:
                assert from_.stop_sequence is not None
                shape.stop_distances[from_.stop_sequence] = 0.0

            # Generate the shape for the leg
            leg_nodes = self.generate_leg(from_.node_id, to.node_id)

            # Skip first node, as it's the same as previous leg's last node,
            # except for the very first leg
            offset = 0 if i == 0 else 1

            # Save the points of the shape
            for node_id in leg_nodes[offset:]:
                node = self.graph[node_id]
                if shape.points:
                    total_distance += shape.points[-1].distance_to_km(node.lat, node.lon)
                shape.points.append(Point(node.lat, node.lon, total_distance))

            # Record the distance to the stop
            if to.stop_sequence is not None:
                shape.stop_distances[to.stop_sequence] = total_distance

        return shape

    def generate_leg(self, from_: int, to: int) -> list[int]:
        try:
            nodes = self.graph.find_route(from_, to, without_turn_around=False)
        except routx.StepLimitExceeded:
            nodes = []

        if not nodes:
            logger.error("No shape between nodes %d and %d", from_, to)
            return [from_, to]

        return nodes
