# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from copy import copy
from typing import Any, NotRequired, TypedDict

from impuls import TaskRuntime
from impuls.model import Route, StopTime
from impuls.tasks import SplitTripLegs
from impuls.tools.color import text_color_for


class BusRouteCuration(TypedDict):
    agency: NotRequired[str]
    short_name_match: str
    short_name_replacement: NotRequired[str]
    long_name_replacement: NotRequired[str]
    color: NotRequired[str]


class SplitBusLegs(SplitTripLegs):
    def __init__(self, r: str = "bus_routes.yaml") -> None:
        super().__init__(
            replacement_bus_short_name_pattern=re.compile(r"\bZKA\b", re.I),
            leg_trip_id_infix="_LEG",
        )
        self.r = r
        self.curated_routes = list[BusRouteCuration]()

    def execute(self, r: TaskRuntime) -> None:
        self.curated_routes = r.resources[self.r].yaml()["routes"]
        super().execute(r)

    def update_bus_replacement_route(self, route: Route) -> None:
        route.type = Route.Type.BUS

        # Try to match route with one of the curated ones
        for curated_route in self.curated_routes:
            # Check if agency matches
            if "agency" in curated_route and curated_route["agency"] != route.agency_id:
                continue

            # Check if short_name matches
            short_name_match = re.search(curated_route["short_name_match"], route.short_name)
            if not short_name_match:
                continue

            # Both agency and short_name match - apply route
            if short_name_template := curated_route.get("short_name_replacement"):
                route.short_name = short_name_match.expand(short_name_template)
            if long_name_template := curated_route.get("long_name_replacement"):
                route.long_name = long_name_template.format(route.long_name)

            if color := curated_route.get("color"):
                route.color = color
                route.text_color = text_color_for(color)

            return

        # Apply fallback curation
        route.short_name = f"ZKA {route.short_name}"
        route.long_name = f"{route.long_name} (Zastępcza Komunikacja Autobusowa)"
        route.color = "DE4E4E"
        route.text_color = "FFFFFF"

    def get_departure_data(self, stop_time: StopTime) -> Any:
        return stop_time.platform == "BUS" or (
            # XXX: Fix for Koleje Mazowieckie - they don't always use "BUS" platforms,
            #      sometimes they also mark buses with category starting with "Z".
            stop_time.trip_id.startswith("PLK_KM_")
            and (stop_time.get_extra_field("plk_category_code") or "").startswith("Z")
        )

    def arrival_only(self, stop_time: StopTime, previous_data: Any) -> StopTime:
        new = copy(stop_time)
        extra = new.get_extra_fields()
        new.departure_time = new.arrival_time

        if previous_data:
            new.platform = "BUS"
            extra["track"] = ""
        else:
            new.platform = extra.get("arrival_platform", "")
            extra["track"] = extra.get("arrival_track", "")

        new.set_extra_fields(extra)
        return new

    def departure_only(self, stop_time: StopTime, current_data: Any) -> StopTime:
        new = copy(stop_time)
        extra = new.get_extra_fields()
        new.arrival_time = new.departure_time

        if current_data:
            new.platform = "BUS"
            extra["track"] = ""
        elif new.platform == "BUS":
            new.platform = ""
            extra["track"] = ""

        new.set_extra_fields(extra)
        return new
