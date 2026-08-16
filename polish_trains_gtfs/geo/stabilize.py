# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from io import Writer
from itertools import chain
from operator import attrgetter
from pathlib import Path
from typing import Any, Literal, TypeAlias, get_args
from xml.sax import parse as sax_parse
from xml.sax.handler import ContentHandler
from xml.sax.saxutils import quoteattr
from xml.sax.xmlreader import AttributesImpl as SAXAttributes

FeatureType = Literal["node", "way", "relation"]
VALID_FEATURE_TYPES: set[FeatureType] = set(get_args(FeatureType))

DEFAULT_PATH = Path("data/geo.osm")


@dataclass
class Node:
    id: int
    lat: float
    lon: float
    tags: dict[str, str] = field(default_factory=dict[str, str])

    def write(self, xml: "IncrementalXMLWriter") -> None:
        if self.tags:
            with xml.tag(
                "node",
                id=self.id,
                action="modify",
                lat=self.lat,
                lon=self.lon,
                version="1",
            ):
                for k, v in self.tags.items():
                    xml.leaf("tag", k=k, v=v)

        else:
            xml.leaf("node", id=self.id, action="modify", lat=self.lat, lon=self.lon, version="1")


@dataclass
class Way:
    id: int
    tags: dict[str, str] = field(default_factory=dict[str, str])
    nodes: list[int] = field(default_factory=list[int])

    def write(self, xml: "IncrementalXMLWriter") -> None:
        with xml.tag("way", id=self.id, action="modify", version="1"):
            for ref in self.nodes:
                xml.leaf("nd", ref=ref)
            for k, v in self.tags.items():
                xml.leaf("tag", k=k, v=v)


@dataclass
class Relation:
    @dataclass
    class Member:
        type: FeatureType
        ref: int
        role: str

        def write(self, xml: "IncrementalXMLWriter") -> None:
            xml.leaf("member", type=self.type, ref=self.ref, role=self.role)

    id: int
    tags: dict[str, str] = field(default_factory=dict[str, str])
    members: list[Member] = field(default_factory=list[Member])

    def write(self, xml: "IncrementalXMLWriter") -> None:
        with xml.tag("way", id=self.id, action="modify", version="1"):
            for member in self.members:
                member.write(xml)
            for k, v in self.tags.items():
                xml.leaf("tag", k=k, v=v)


Feature: TypeAlias = Node | Way | Relation


class IncrementalXMLWriter:
    def __init__(self, f: Writer[str], indent: str = "  ") -> None:
        self.f = f
        self.indent = indent
        self.level = 0

        self.f.write('<?xml version="1.0" encoding="utf-8"?>\n')

    @contextmanager
    def tag(self, name: str, **attrs: Any) -> Generator[None, None, None]:
        self.write_indent()
        self.f.write("<")
        self.f.write(name)
        for k, v in attrs.items():
            self.f.write(" ")
            self.f.write(k)
            self.f.write("=")
            self.f.write(quoteattr(str(v)))
        self.f.write(">\n")

        self.level += 1
        yield
        self.level -= 1

        self.write_indent()
        self.f.write("</")
        self.f.write(name)
        self.f.write(">\n")

    def leaf(self, name: str, **attrs: Any) -> None:
        self.write_indent()
        self.f.write("<")
        self.f.write(name)
        for k, v in attrs.items():
            self.f.write(" ")
            self.f.write(k)
            self.f.write("=")
            self.f.write(quoteattr(str(v)))
        self.f.write("/>\n")

    def write_indent(self) -> None:
        for _ in range(self.level):
            self.f.write(self.indent)


@dataclass
class IDCounter:
    max: int = 0
    negative_count: int = 0

    def visit(self, id: int) -> None:
        self.max = max(self.max, id)
        self.negative_count += id < 0


@dataclass
class IDReplacer:
    counter: int = 0
    mapping: dict[int, int] = field(default_factory=dict[int, int])

    def get(self, id: int) -> int:
        if id > 0:
            return id

        if (replacement := self.mapping.get(id)) is None:
            self.counter += 1
            replacement = self.counter
            self.mapping[id] = replacement
        return replacement


class Checker(ContentHandler):
    nodes: IDCounter
    ways: IDCounter
    relations: IDCounter
    has_deleted: bool

    def __init__(self) -> None:
        self.nodes = IDCounter()
        self.ways = IDCounter()
        self.relations = IDCounter()
        self.has_deleted = False

    def _get_counter(self, name: FeatureType) -> IDCounter:
        return getattr(self, f"{name}s")

    def startElement(self, name: str, attrs: SAXAttributes) -> None:
        if name in VALID_FEATURE_TYPES:
            if attrs.get("action") == "delete":
                self.has_deleted = True
            else:
                id = int(attrs["id"])
                self._get_counter(name).visit(id)


class Loader(ContentHandler):
    nodes: list[Node]
    ways: list[Way]
    relations: list[Relation]

    _feature: Feature | None

    def __init__(self) -> None:
        super().__init__()
        self.nodes = []
        self.ways = []
        self.relations = []
        self._feature = None

    def startElement(self, name: str, attrs: SAXAttributes) -> None:
        if name == "node" and attrs.get("action") != "delete":
            self._feature = Node(int(attrs["id"]), float(attrs["lat"]), float(attrs["lon"]))

        elif name == "way" and attrs.get("action") != "delete":
            self._feature = Way(int(attrs["id"]))

        elif name == "relation" and attrs.get("action") != "delete":
            self._feature = Relation(int(attrs["id"]))

        elif name == "tag" and self._feature is not None:
            self._feature.tags[attrs["k"]] = attrs["v"]

        elif name == "nd" and isinstance(self._feature, Way):
            self._feature.nodes.append(int(attrs["ref"]))

        elif (
            name == "member"
            and isinstance(self._feature, Relation)
            and (t := attrs["type"]) in VALID_FEATURE_TYPES
        ):
            self._feature.members.append(Relation.Member(t, int(attrs["ref"]), attrs["role"]))

    def endElement(self, name: str) -> None:
        if name == "node" and isinstance(self._feature, Node):
            self.nodes.append(self._feature)
            self._feature = None

        elif name == "way" and isinstance(self._feature, Way):
            self.ways.append(self._feature)
            self._feature = None

        elif name == "relation" and isinstance(self._feature, Relation):
            self.relations.append(self._feature)
            self._feature = None

    def get_max_id(self, type: FeatureType) -> int:
        features = getattr(self, f"{type}s")
        return max(chain((0,), (i.id for i in features)))

    def fix_ids(self) -> None:
        node_ids = IDReplacer(self.get_max_id("node"))
        way_ids = IDReplacer(self.get_max_id("way"))
        relation_ids = IDReplacer(self.get_max_id("relation"))

        for node in self.nodes:
            node.id = node_ids.get(node.id)

        for way in self.ways:
            way.id = way_ids.get(way.id)
            way.nodes = [node_ids.get(ref) for ref in way.nodes]

        for relation in self.relations:
            relation.id = relation_ids.get(relation.id)

            for member in relation.members:
                if member.type == "node":
                    member.ref = node_ids.get(member.ref)
                elif member.type == "way":
                    member.ref = way_ids.get(member.ref)
                elif member.type == "relation":
                    member.ref = relation_ids.get(member.ref)

    def fix_order(self) -> None:
        self.nodes.sort(key=attrgetter("id"))
        self.ways.sort(key=attrgetter("id"))
        self.relations.sort(key=attrgetter("id"))

    def write(self, f: Writer[str]) -> None:
        xml = IncrementalXMLWriter(f)
        with xml.tag(
            "osm",
            version="0.6",
            upload="false",
            generator="PolishTrainsGTFS",
            license="CC0-1.0",
        ):
            for feature in chain(self.nodes, self.ways, self.relations):
                feature.write(xml)


def check(file: Path = DEFAULT_PATH) -> bool:
    checker = Checker()
    sax_parse(file, checker)

    ok = True

    if checker.nodes.negative_count:
        print(checker.nodes.negative_count, "nodes with unstable ids")
        ok = False

    if checker.ways.negative_count:
        print(checker.ways.negative_count, "ways with unstable ids")
        ok = False

    if checker.relations.negative_count:
        print(checker.relations.negative_count, "relations with unstable ids")
        ok = False

    if checker.has_deleted:
        print("deleted elements present")
        ok = False

    return ok


def fix(file: Path = DEFAULT_PATH) -> None:
    loader = Loader()
    sax_parse(file, loader)

    loader.fix_ids()
    loader.fix_order()

    tmp_file = file.with_name(f".{file.name}.tmp")
    try:
        with tmp_file.open("w", encoding="utf-8") as out:
            loader.write(out)
    except BaseException:
        tmp_file.unlink(missing_ok=True)
        raise

    tmp_file.rename(file)


if __name__ == "__main__":
    from argparse import ArgumentParser
    from sys import exit

    arg_parser = ArgumentParser()
    arg_parser.add_argument("-c", "--check", action="store_true")
    arg_parser.add_argument("file", default=DEFAULT_PATH, type=Path, nargs="?")
    args = arg_parser.parse_args()

    if args.check:
        ok = check(args.file)
        exit(0 if ok else 1)
    else:
        fix(args.file)
        exit(0)
