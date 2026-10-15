# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import fnmatch
import re
from collections.abc import MutableMapping
from typing import NotRequired, TypedDict, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.errors import DataError, MultipleDataErrors
from impuls.model import Agency, Route
from impuls.tools.color import text_color_for


class CuratedRouteMatcher(TypedDict):
    id: NotRequired[str]
    short_name: NotRequired[str]
    case_sensitive: NotRequired[bool]
    regex: NotRequired[bool]


class CuratedRoute(TypedDict):
    id: str
    match: NotRequired[list[CuratedRouteMatcher]]
    short_name: str
    long_name: str
    color: NotRequired[str]
    text_color: NotRequired[str]
    type: NotRequired[int]


class CuratedAgency(TypedDict):
    id: str
    name: str
    url: str
    phone: NotRequired[str]
    routes: list[CuratedRoute]


class CuratedData(TypedDict):
    agencies: list[CuratedAgency]


class RouteMatcher:
    def __init__(
        self,
        id: str = "",
        short_name: str = "",
        case_sensitive: bool = False,
        regex: bool = False,
    ) -> None:
        self.id = self._compile(id, case_sensitive, regex)
        self.short_name = self._compile(short_name, case_sensitive, regex)

    def route_mismatch(self, r: Route) -> bool:
        return self.id is not None and not self.id.fullmatch(r.id)

    def short_name_mismatch(self, r: Route) -> bool:
        return self.short_name is not None and not self.short_name.fullmatch(r.short_name)

    def matches(self, r: Route) -> bool:
        return not (self.route_mismatch(r) or self.short_name_mismatch(r))

    @staticmethod
    def _compile(
        pat: str,
        case_sensitive: bool = False,
        regex: bool = False,
    ) -> re.Pattern[str] | None:
        if not pat:
            return None
        elif not regex:
            pat = fnmatch.translate(pat)
        return re.compile(pat, flags=(re.IGNORECASE if case_sensitive else 0))


class CurateRoutes(Task):
    def __init__(self, r: str = "routes.yaml") -> None:
        super().__init__()
        self.r = r

        self.to_curate = dict[str, tuple[Agency, dict[str, Route]]]()
        self.leftover = list[Agency | Route]()

    def execute(self, r: TaskRuntime) -> None:
        curated_data = cast(CuratedData, r.resources[self.r].yaml())
        self.load_to_curate(r.db)

        with r.db.transaction():
            for agency_data in curated_data["agencies"]:
                self.curate_agency(r.db, agency_data)
            self.clean_unused(r.db)

        self.collect_leftover_agencies()
        self.check_leftover()

    def load_to_curate(self, db: DBConnection) -> None:
        self.to_curate.clear()
        self.leftover.clear()
        for agency in db.retrieve_all(Agency):
            self.to_curate[agency.id] = (agency, {})
        for route in db.retrieve_all(Route):
            self.to_curate[route.agency_id][1][route.id] = route

    def curate_agency(self, db: DBConnection, agency_data: CuratedAgency) -> None:
        self.upsert_agency(db, agency_data)
        routes = self.get_all_routes_to_curate(agency_data)
        for route_data in agency_data["routes"]:
            self.curate_route(db, route_data, agency_data["id"], routes)
        self.leftover.extend(routes.values())

    def upsert_agency(self, db: DBConnection, data: CuratedAgency) -> None:
        if data["id"] in self.to_curate:
            db.raw_execute(
                "UPDATE agencies SET name = ?, url = ?, phone = ? WHERE agency_id = ?",
                (data["name"], data["url"], data.get("phone", ""), data["id"]),
            )
        else:
            db.raw_execute(
                "INSERT INTO agencies (agency_id, name, url, phone, timezone, lang) "
                "VALUES (?, ?, ?, ?, 'Europe/Warsaw', 'pl')",
                (data["id"], data["name"], data["url"], data.get("phone", "")),
            )

    def get_all_routes_to_curate(self, data: CuratedAgency) -> dict[str, Route]:
        _, routes = self.to_curate.pop(data["id"], (None, dict[str, Route]()))
        return routes

    def curate_route(
        self,
        db: DBConnection,
        data: CuratedRoute,
        agency_id: str,
        to_curate: MutableMapping[str, Route],
    ) -> None:
        self.upsert_route(db, data, agency_id, exists=data["id"] in to_curate)
        to_curate.pop(data["id"], None)  # Remove implicit match on exact route_id

        curated = set[str]()
        matchers = [RouteMatcher(**m) for m in data.get("match", [])]
        for route_id, route in to_curate.items():
            if any(m.matches(route) for m in matchers):
                curated.add(route_id)
                db.raw_execute(
                    "UPDATE trips SET route_id = ? WHERE route_id = ?",
                    (data["id"], route_id),
                )

        for route_id in curated:
            del to_curate[route_id]

    def upsert_route(
        self,
        db: DBConnection,
        data: CuratedRoute,
        agency_id: str,
        exists: bool,
    ) -> None:
        if "color" in data:
            color = data["color"]
            text_color = data.get("text_color", text_color_for(color))
        else:
            color = ""
            text_color = ""

        db.raw_execute(
            "INSERT INTO routes (route_id, agency_id, short_name, long_name, color, "
            "text_color, type) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (route_id) DO "
            "UPDATE SET short_name = excluded.short_name, long_name = excluded.long_name, "
            "color = excluded.color, text_color = excluded.text_color, type = excluded.type",
            (
                data["id"],
                agency_id,
                data["short_name"],
                data["long_name"],
                color,
                text_color,
                data.get("type", 2),
            ),
        )

    def clean_unused(self, db: DBConnection) -> None:
        db.raw_execute(
            "DELETE FROM routes WHERE NOT EXISTS "
            "(SELECT 1 FROM trips WHERE trips.route_id = routes.route_id)"
        )
        db.raw_execute(
            "DELETE FROM agencies WHERE NOT EXISTS "
            "(SELECT 1 FROM routes WHERE routes.agency_id = agencies.agency_id)"
        )

    def collect_leftover_agencies(self) -> None:
        for agency, _ in self.to_curate.values():
            self.leftover.append(agency)

    def check_leftover(self) -> None:
        if self.leftover:
            raise MultipleDataErrors(
                "route curation",
                [DataError(f"{obj!r} was not curated") for obj in self.leftover],
            )
