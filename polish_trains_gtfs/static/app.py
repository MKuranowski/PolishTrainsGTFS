# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import ArgumentParser, Namespace
from datetime import timedelta
from typing import cast

from impuls import App, HTTPResource, Pipeline, PipelineOptions
from impuls.model import Date
from impuls.resource import LocalResource, TimeLimitedResource
from impuls.tasks import ExecuteSQL, GenerateTripHeadsign, RemoveUnusedEntities, SaveGTFS

from ..apikey import get_apikey
from .curate_routes import CurateRoutes
from .load_schedules import LoadSchedules
from .load_stops import LoadStops
from .shift_negative_times import ShiftNegativeTimes

RESOURCE_TIME_LIMIT = timedelta(days=1)

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_phone",
        "agency_lang",
    ),
    "attributions.txt": (
        "attribution_id",
        "organization_name",
        "attribution_url",
        "is_producer",
        "is_operator",
        "is_authority",
        "is_data_source",
    ),
    "calendar_dates.txt": ("date", "service_id", "exception_type"),
    "feed_info.txt": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_version",
        "feed_start_date",
        "feed_end_date",
    ),
    "routes.txt": (
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "stops.txt": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "platform",
        "track",
    ),
    "trips.txt": (
        "trip_id",
        "route_id",
        "service_id",
        "trip_short_name",
        "trip_long_name",
        "trip_headsign",
        "order_id",
        "plk_category_code",
        "plk_train_number",
    ),
}


class PolishTrainsGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("-o", "--output", default="polish_trains.zip", help="output file path")
        parser.add_argument(
            "-d",
            "--start-date",
            type=Date.from_ymd_str,
            default=Date.today(),
            help="start date for the schedules",
        )

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        apikey = get_apikey()
        start_date = cast(Date, args.start_date)
        end_date = start_date.add_days(31)

        return Pipeline(
            options=options,
            resources={
                "schedules.json": TimeLimitedResource(
                    r=HTTPResource.get(
                        "https://pdp-api.plk-sa.pl/api/v1/schedules/shortened",
                        headers={"X-Api-Key": apikey},
                        params={"dateFrom": start_date.isoformat(), "dateTo": end_date.isoformat()},
                    ),
                    minimal_time_between=RESOURCE_TIME_LIMIT,
                ),
                "pl_rail_map.osm": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
                ),
                "routes.yaml": LocalResource("data/routes.yaml"),
            },
            tasks=[
                LoadSchedules(),
                ExecuteSQL(
                    statement="DELETE FROM agencies WHERE agency_id = 'WKD'",
                    task_name="DropWKD",
                ),
                RemoveUnusedEntities(),
                CurateRoutes(),
                LoadStops(),
                ShiftNegativeTimes(),
                ExecuteSQL(
                    statement=(
                        "UPDATE stop_times SET arrival_time = arrival_time - 3600, "
                        "departure_time = departure_time - 3600 WHERE stop_id = '179200'"
                    ),
                    task_name="FixTimesAtMockava",
                ),
                GenerateTripHeadsign(),
                SaveGTFS(GTFS_HEADERS, args.output, ensure_order=True),
            ],
        )
