# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from argparse import ArgumentParser, Namespace
from datetime import timedelta

from impuls import App, HTTPResource, Pipeline, PipelineOptions
from impuls.resource import TimeLimitedResource
from impuls.tasks import GenerateTripHeadsign, SaveGTFS

from ..apikey import get_apikey
from .load_agencies import LoadAgencies
from .load_data_version import LoadDataVersion
from .load_routes import LoadRoutes
from .load_schedules import LoadSchedules
from .load_stops import LoadStops

RESOURCE_TIME_LIMIT = timedelta(days=1)

GTFS_HEADERS = {
    "agency.txt": ("agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"),
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
    "feed_info.txt": ("feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version"),
    "routes.txt": (
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
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
        "trip_headsign",
        "order_id",
        "plk_train_number",
    ),
}


class PolishTrainsGTFS(App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("-o", "--output", default="polish_trains.zip", help="output file path")

    def prepare(self, args: Namespace, options: PipelineOptions) -> Pipeline:
        apikey = get_apikey()
        return Pipeline(
            options=options,
            resources={
                "data_version.json": self.endpoint("data-version", apikey),
                "carriers.json": self.endpoint("dictionaries/carriers", apikey),
                "categories.json": self.endpoint("dictionaries/commercial-categories", apikey),
                "stations.json": self.endpoint("dictionaries/stations", apikey),
                "schedules.json": self.endpoint("schedules/shortened", apikey),
            },
            tasks=[
                LoadDataVersion(),
                LoadAgencies(),
                LoadRoutes(),
                LoadStops(),
                LoadSchedules(),
                GenerateTripHeadsign(),
                SaveGTFS(GTFS_HEADERS, args.output, ensure_order=True),
            ],
        )

    @staticmethod
    def endpoint(path: str, apikey: str) -> TimeLimitedResource:
        return TimeLimitedResource(
            r=HTTPResource.get(
                f"https://pdp-api.plk-sa.pl/api/v1/{path}",
                headers={"X-Api-Key": apikey},
            ),
            minimal_time_between=RESOURCE_TIME_LIMIT,
        )
