# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT


from dataclasses import dataclass
from pathlib import Path
from typing import cast
from xml.sax import ContentHandler as XmlSaxContentHandler
from xml.sax import parse as xml_sax_parse
from xml.sax.xmlreader import AttributesImpl as XmlSaxAttributes

import impuls


@dataclass
class Station:
    id: str = ""
    name: str = ""
    lat: float = 0.0
    lon: float = 0.0

    def __bool__(self) -> bool:
        return bool(self.id and self.name and self.lat and self.lon)


class PLRailMapLoader(XmlSaxContentHandler):
    def __init__(self) -> None:
        super().__init__()
        self.stations = list[Station]()
        self.current_station = Station()

    def startElement(self, name: str, attrs: XmlSaxAttributes) -> None:
        if name == "node":
            self.current_station = Station(lat=float(attrs["lat"]), lon=float(attrs["lon"]))
        elif name == "tag":
            if attrs["k"] == "ref":
                self.current_station.id = attrs["v"]
            elif attrs["k"] == "name":
                self.current_station.name = attrs["v"]

    def endElement(self, name: str) -> None:
        if name == "node" and self.current_station:
            self.stations.append(self.current_station)

    @classmethod
    def load_from_file(cls, path: Path) -> list[Station]:
        handler = cls()
        xml_sax_parse(path, handler)
        return handler.stations


class LoadStops(impuls.Task):
    def __init__(self) -> None:
        super().__init__()
        self.to_update = dict[str, str]()

    def execute(self, r: impuls.TaskRuntime) -> None:
        self.to_update = {
            cast(str, i[0]): cast(str, i[1])
            for i in r.db.raw_execute("SELECT stop_id, name FROM stops")
        }
        stations = PLRailMapLoader.load_from_file(r.resources["pl_rail_map.osm"].stored_at)
        with r.db.transaction():
            for station in stations:
                self._apply(station, r.db)
        self._ensure_everything_curated()

    def _apply(self, station: Station, db: impuls.DBConnection) -> None:
        if station.id not in self.to_update:
            return  # unused station

        self.to_update.pop(station.id, None)
        db.raw_execute(
            "UPDATE stops SET name = ?, lat = ?, lon = ? WHERE stop_id = ?",
            (station.name, station.lat, station.lon, station.id),
        )

    def _ensure_everything_curated(self) -> None:
        if self.to_update:
            raise impuls.errors.MultipleDataErrors(
                "LoadStationData",
                [
                    impuls.errors.DataError(f"Missing data for {id} {name!r}")
                    for id, name in self.to_update.items()
                ],
            )
