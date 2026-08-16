# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from typing import Literal, NamedTuple, cast
from xml.sax import parse as sax_parse
from xml.sax.handler import ContentHandler as SAXContentHandler
from xml.sax.xmlreader import AttributesImpl as SAXAttributes

from impuls.errors import DataError, MultipleDataErrors
from impuls.tools.types import StrPath

from .issue import Issue
from .model import (
    BusStop,
    DirectionHint,
    Exit,
    Platform,
    Station,
    StopPosition,
    is_valid_direction_hint,
)


class StationData(NamedTuple):
    stations: dict[str, Station]
    issues: list[Issue]
    nodes_on_rail: set[int] | None


class StationDataLoader(SAXContentHandler):
    stations: dict[str, Station]
    issues: list[Issue]
    nodes_on_rail: set[int] | None

    _in_feature: Literal["n", "w", ""]
    _feature_id: int
    _node_position: tuple[float, float]
    _way_nodes: list[int]
    _tags: dict[str, str]

    def __init__(self, load_nodes_on_rail: bool = False) -> None:
        super().__init__()
        self.stations = {}
        self.issues = []
        self.nodes_on_rail = set() if load_nodes_on_rail else None

        self._in_feature = ""
        self._feature_id = 0
        self._node_position = 0.0, 0.0
        self._way_nodes = []
        self._tags = {}

    def startDocument(self) -> None:
        self.stations.clear()
        self.issues.clear()
        if self.nodes_on_rail is not None:
            self.nodes_on_rail.clear()

        self._in_feature = ""

    def startElement(self, name: str, attrs: SAXAttributes) -> None:
        # Ignore deleted elements
        if attrs.get("action") == "delete":
            return

        if name == "node":
            self._in_feature = "n"
            self._feature_id = int(attrs["id"])
            self._node_position = float(attrs["lat"]), float(attrs["lon"])
            self._tags.clear()

        elif name == "way" and self.nodes_on_rail is not None:
            self._in_feature = "w"
            self._feature_id = int(attrs["id"])
            self._way_nodes.clear()
            self._tags.clear()

        elif name == "tag" and self._in_feature:
            self._tags[attrs["k"]] = attrs["v"]

        elif name == "nd" and self._in_feature == "w":
            self._way_nodes.append(int(attrs["ref"]))

    def endElement(self, name: str) -> None:
        if name == "node" and self._in_feature == "n":
            railway = self._tags.get("railway")
            public_transport = self._tags.get("public_transport")
            highway = self._tags.get("highway")

            if railway == "station":
                self._on_station()

            elif public_transport == "platform":
                self._on_platform()

            elif railway == "subway_entrance":
                self._on_exit()

            elif public_transport == "stop_position":
                self._on_stop_position()

            elif highway == "bus_stop":
                self._on_bus_stop()

        elif (
            name == "way"
            and self._in_feature == "w"
            and self._tags.get("railway") == "rail"
            and self.nodes_on_rail is not None
        ):
            self.nodes_on_rail.update(self._way_nodes)

    def endDocument(self) -> None:
        self._remove_invalid_station_references()

    def _on_station(self) -> None:
        id = self._tags.get("ref", "")
        name = self._tags.get("name", "")

        # Ensure 'ref' tag is not empty
        if not id:
            self.issues.append(
                Issue(
                    Station(self._feature_id, *self._node_position, id, name),
                    "missing 'ref' tag",
                )
            )
            return

        # Ensure station is not duplicate
        s = self._get_station(id)
        if s.node_id:
            self.issues.append(
                Issue(
                    Station(self._feature_id, *self._node_position, id, name),
                    f"ID {id!r} already in use by {s}",
                )
            )
            return

        s.node_id = self._feature_id
        s.lat, s.lon = self._node_position

        # Ensure name is not empty
        s.name = name
        if not name:
            self.issues.append(Issue(s, "missing 'name' tag"))

        # Parse optional tags

        if secondary_id := self._tags.get("ref:2"):
            s.other_ids.append(secondary_id)

        if int_name := self._tags.get("int_name"):
            s.translations["int"] = int_name

        if name_pl := self._tags.get("name:pl"):
            s.translations["pl"] = name_pl

        if wheelchair := self._tags.get("wheelchair"):
            if wheelchair == "yes":
                s.accessible = True
            elif wheelchair == "no":
                s.accessible = False
            else:
                self.issues.append(Issue(s, f"invalid 'wheelchair' value: {wheelchair!r}"))

        if country := self._tags.get("country"):
            s.country = country

        if request_stop := self._tags.get("request_stop"):
            if request_stop == "yes":
                s.request_stop = True
            else:
                self.issues.append(Issue(s, f"invalid 'request_stop' value: {request_stop!r}"))

        if passenger := self._tags.get("passenger"):
            if passenger == "no":
                s.waypoint = True
            else:
                self.issues.append(Issue(s, f"invalid 'passenger' value: {passenger!r}"))

    def _on_platform(self) -> None:
        p = Platform(self._feature_id, *self._node_position, "")
        station_id = self._tags.get("ref:station", "")
        p.name, _, p.track = self._tags.get("name", "").partition(";")

        # Ensure 'ref:station' tag is not empty
        if not station_id:
            self.issues.append(Issue(p, "missing 'ref:station' tag"))
            return

        # Ensure name is not empty
        if not p.name:
            self.issues.append(Issue(p, "missing 'name' tag"))
            return

        # Check optional accessibility
        if wheelchair := self._tags.get("wheelchair"):
            if wheelchair == "yes":
                p.accessible = True
            elif wheelchair == "no":
                p.accessible = False
            else:
                self.issues.append(Issue(p, f"invalid 'wheelchair' value: {wheelchair!r}"))

        # Add platform to the station
        self._get_station(station_id).platforms.append(p)

    def _on_exit(self) -> None:
        e = Exit(self._feature_id, *self._node_position, name=self._tags.get("name", ""))
        station_id = self._tags.get("ref:station", "")

        # Ensure 'ref:station' tag is not empty
        if not station_id:
            self.issues.append(Issue(e, "missing 'ref:station' tag"))
            return

        # Ensure name is not empty
        if not e.name:
            self.issues.append(Issue(e, "missing 'name' tag"))

        # Check optional accessibility
        if wheelchair := self._tags.get("wheelchair"):
            if wheelchair == "yes":
                e.accessible = True
            elif wheelchair == "no":
                e.accessible = False
            else:
                self.issues.append(Issue(e, f"invalid 'wheelchair' value: {wheelchair!r}"))

        # Check optional platforms
        if platforms := self._tags.get("platforms"):
            e.platforms = _split_osm_list(platforms)

        # Add exit to the station
        self._get_station(station_id).exits.append(e)

    def _on_stop_position(self) -> None:
        s = StopPosition(
            self._feature_id,
            *self._node_position,
            platforms=_split_osm_list(self._tags.get("platforms", "")),
            towards=_split_osm_list(self._tags.get("towards", "")),
        )
        station_id = self._tags.get("ref:station", "")

        # Ensure 'ref:station' tag is not empty
        if not station_id:
            self.issues.append(Issue(s, "missing 'ref:station' tag"))
            return

        # Ensure 'platforms' is not empty
        if not s.platforms:
            self.issues.append(Issue(s, "missing 'platforms' tag"))
            return

        # Add stop_position to the station
        self._get_station(station_id).stop_positions.append(s)

    def _on_bus_stop(self) -> None:
        s = BusStop(
            self._feature_id,
            *self._node_position,
            towards=_split_osm_list(self._tags.get("towards", "")),
        )
        station_id = self._tags.get("ref:station", "")
        all_hints = _split_osm_list(self._tags.get("direction", ""))
        s.direction, invalid_hints = _extract_direction_hints(all_hints)

        # Ensure 'ref:station' tag is not empty
        if not station_id:
            self.issues.append(Issue(s, "missing 'ref:station' tag"))
            return

        # Validate direction hints
        if invalid_hints:
            self.issues.append(Issue(s, f"invalid 'direction' hints: {', '.join(invalid_hints)}"))

        # Add bus stop to the station
        self._get_station(station_id).bus_stops.append(s)

    def _get_station(self, id: str) -> Station:
        if (s := self.stations.get(id)) is None:
            s = Station(node_id=0, id=id, name="", lat=0.0, lon=0.0)
            self.stations[id] = s
        return s

    def _remove_invalid_station_references(self) -> None:
        invalid_stations = list[str]()

        for id, station in self.stations.items():
            if not station.node_id:
                invalid_stations.append(id)
                for child_entity in station.all_child_entities():
                    self.issues.append(Issue(child_entity, f"references invalid station {id!r}"))

        for invalid_id in invalid_stations:
            del self.stations[invalid_id]


def load_data(file: StrPath, /, raise_on_errors: bool = False) -> StationData:
    l = StationDataLoader(load_nodes_on_rail=True)
    sax_parse(file, l)
    if raise_on_errors and l.issues:
        raise MultipleDataErrors(f"loading {file}", cast(list[DataError], l.issues))
    return StationData(l.stations, l.issues, l.nodes_on_rail)


def load_stations(
    file: StrPath,
    /,
    ignore_errors: bool = False,
) -> dict[str, Station]:
    l = StationDataLoader(load_nodes_on_rail=False)
    sax_parse(file, l)
    if not ignore_errors and l.issues:
        raise MultipleDataErrors(f"loading {file}", cast(list[DataError], l.issues))
    return l.stations


def _split_osm_list(v: str, sep: str = ";") -> list[str]:
    return v.split(sep) if v else []


def _extract_direction_hints(values: Iterable[str]) -> tuple[list[DirectionHint], list[str]]:
    valid = list[DirectionHint]()
    invalid = list[str]()
    for hint in values:
        if is_valid_direction_hint(hint):
            valid.append(hint)
        else:
            invalid.append(hint)
    return valid, invalid
