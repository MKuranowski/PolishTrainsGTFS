# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import ArgumentParser, Namespace
from typing import cast

from impuls import App, HTTPResource, LocalResource, Pipeline, PipelineOptions
from impuls.model import Date
from impuls.tasks import ExecuteSQL, GenerateTripHeadsign, RemoveUnusedEntities, SaveGTFS

from ..apikey import get_apikey
from . import external
from .add_train_names import AddTrainNames
from .curate_routes import CurateRoutes
from .extract_routes import ExtractRoutes
from .load_bus_stops import LoadBusStops
from .load_schedules import LoadSchedules
from .load_stops import LoadStops
from .shift_negative_times import ShiftNegativeTimes
from .split_bus_legs import SplitBusLegs

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
    "stops.txt": (
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "location_type",
        "parent_station",
    ),
    "stop_times.txt": (
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
        "platform",
        "track",
        "plk_category_code",
        "plk_sequence",
    ),
    "transfers.txt": ("from_stop_id", "to_stop_id", "from_trip_id", "to_trip_id", "transfer_type"),
    "trips.txt": (
        "trip_id",
        "route_id",
        "service_id",
        "trip_short_name",
        "trip_headsign",
        "plk_category_code",
        "plk_train_number",
        "plk_train_name",
        "plk_order_id",
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
        parser.add_argument(
            "-e",
            "--external",
            action="store_true",
            help="load extra data from external, non-plk sources",
        )

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        apikey = get_apikey("PKP_PLK_APIKEY")

        # NOTE: We need to fetch schedules from start_date-1 to properly show night trains.
        start_date = cast(Date, args.start_date).add_days(-1)
        end_date = start_date.add_days(31)

        if args.external:
            external_resources = external.get_resources()
            external_tasks = external.get_tasks()
        else:
            external_resources = {}
            external_tasks = []

        return Pipeline(
            options=options,
            resources={
                **external_resources,
                "schedules.json": HTTPResource.get(
                    "https://pdp-api.plk-sa.pl/api/v1/schedules/shortened",
                    headers={"X-Api-Key": apikey},
                    params={"dateFrom": start_date.isoformat(), "dateTo": end_date.isoformat()},
                ),
                "pl_rail_map.osm": HTTPResource.get(
                    "https://raw.githubusercontent.com/MKuranowski/PLRailMap/master/plrailmap.osm"
                ),
                "routes.yaml": LocalResource("data/routes.yaml"),
                "route_extract.yaml": LocalResource("data/route_extract.yaml"),
            },
            tasks=[
                LoadSchedules(),
                *external_tasks,
                ExecuteSQL(
                    statement="DELETE FROM agencies WHERE agency_id = 'WKD'",
                    task_name="DropWKD",
                ),
                RemoveUnusedEntities(),
                ExtractRoutes(),
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
                AddTrainNames(),
                GenerateTripHeadsign(),
                ExecuteSQL(
                    statement=(
                        "UPDATE stop_times SET platform = 'BUS' "
                        "WHERE platform = '' AND extra_fields_json ->> 'plk_category_code' = 'BUS'"
                    ),
                    task_name="FixMissingBusPlatforms",
                ),
                SplitBusLegs(),
                LoadBusStops(),
                SaveGTFS(GTFS_HEADERS, args.output, ensure_order=True),
            ],
        )
