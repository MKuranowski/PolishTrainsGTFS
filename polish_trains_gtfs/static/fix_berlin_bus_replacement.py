# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Sequence
from typing import cast

from impuls import DBConnection, Task, TaskRuntime

BERLIN_HBF = "247482"
BERLIN_OSTBAHNHOF = "246776"
FRANKFURT_ODER = "179212"
RZEPIN = "25809"


class FixBerlinBusReplacement(Task):
    """Detects missing rail replacement bus services between Berlin Hbf and
    Frankfurt (Oder) / Rzepin on cross-border corridor trains and marks
    the bus leg using extra_fields['bus'] = 'true'.
    """

    def execute(self, r: TaskRuntime) -> None:
        candidate_trips = self.find_candidate_trips(r.db)
        if not candidate_trips:
            return

        transformed_count = 0
        with r.db.transaction():
            for trip_id in candidate_trips:
                if self.process_trip(r.db, trip_id):
                    transformed_count += 1

        self.logger.info(
            "Fixed Berlin rail replacement bus for %d trips",
            transformed_count,
        )

    def find_candidate_trips(self, db: DBConnection) -> list[str]:
        """Finds trips serving Berlin Hbf that do NOT serve Berlin Ostbahnhof."""
        return [
            cast(str, row[0])
            for row in db.raw_execute(
                "SELECT DISTINCT trip_id FROM stop_times "
                "WHERE stop_id = ? "
                "AND trip_id NOT IN ("
                "  SELECT trip_id FROM stop_times WHERE stop_id = ?"
                ")",
                (BERLIN_HBF, BERLIN_OSTBAHNHOF),
            )
        ]

    def process_trip(self, db: DBConnection, trip_id: str) -> bool:
        """Evaluates a candidate trip and applies the bus replacement tags."""
        stops = [
            (cast(int, row[0]), cast(str, row[1]))
            for row in db.raw_execute(
                "SELECT stop_sequence, stop_id FROM stop_times "
                "WHERE trip_id = ? ORDER BY stop_sequence ASC",
                (trip_id,),
            )
        ]

        if len(stops) < 2:
            self.logger.debug("Trip %s has fewer than 2 stops; skipping", trip_id)
            return False

        # Determine corridor direction based on terminal station
        is_westbound = stops[-1][1] == BERLIN_HBF
        is_eastbound = stops[0][1] == BERLIN_HBF

        if is_westbound and is_eastbound:
            self.logger.warning(
                "Trip %s serves Berlin Hbf as both first and last stop; skipping ambiguous loop trip",
                trip_id,
            )
            return False

        if not (is_westbound or is_eastbound):
            self.logger.debug(
                "Trip %s serves Berlin Hbf but not as first or last stop; skipping",
                trip_id,
            )
            return False

        # Find transfer boundary station (Frankfurt Oder preferred, fallback to Rzepin)
        boundary_seq = self.resolve_boundary_sequence(stops)
        if boundary_seq is None:
            self.logger.warning(
                "Trip %s serves Berlin Hbf without Ostbahnhof but has neither "
                "Frankfurt (Oder) nor Rzepin; skipping",
                trip_id,
            )
            return False

        if is_westbound:
            self.transform_westbound(db, trip_id, boundary_seq)
            self.logger.debug(
                "Trip %s: transformed westbound via boundary seq %d", trip_id, boundary_seq
            )
        else:
            self.transform_eastbound(db, trip_id, boundary_seq)
            self.logger.debug(
                "Trip %s: transformed eastbound via boundary seq %d", trip_id, boundary_seq
            )

        return True

    def resolve_boundary_sequence(self, stops: Sequence[tuple[int, str]]) -> int | None:
        """Finds Frankfurt (Oder) stop sequence, or falls back to Rzepin."""
        for seq, stop_id in stops:
            if stop_id == FRANKFURT_ODER:
                return seq

        for seq, stop_id in stops:
            if stop_id == RZEPIN:
                return seq

        return None

    def transform_westbound(self, db: DBConnection, trip_id: str, boundary_seq: int) -> None:
        """Westbound: boundary departure is bus; intermediate and Berlin Hbf are bus.
        Boundary stop platform/track are preserved for the arriving rail leg.
        """
        # Mark boundary departure as bus (leave platform and track untouched)
        db.raw_execute(
            "UPDATE stop_times "
            "SET extra_fields_json = json_set(COALESCE(extra_fields_json, '{}'), '$.bus', 'true') "
            "WHERE trip_id = ? AND stop_sequence = ?",
            (trip_id, boundary_seq),
        )

        # Mark all stops after boundary through Berlin Hbf as pure bus stops
        db.raw_execute(
            "UPDATE stop_times "
            "SET platform = 'BUS', "
            "    extra_fields_json = json_set("
            "        json_remove(COALESCE(extra_fields_json, '{}'), '$.track'), "
            "        '$.bus', 'true'"
            "    ) "
            "WHERE trip_id = ? AND stop_sequence > ?",
            (trip_id, boundary_seq),
        )

    def transform_eastbound(self, db: DBConnection, trip_id: str, boundary_seq: int) -> None:
        """Eastbound: stops from Berlin Hbf strictly before boundary stop are bus.
        Boundary stop departure and subsequent Polish stops remain rail.
        """
        # Mark all stops strictly prior to boundary as pure bus stops
        db.raw_execute(
            "UPDATE stop_times "
            "SET platform = 'BUS', "
            "    extra_fields_json = json_set("
            "        json_remove(COALESCE(extra_fields_json, '{}'), '$.track'), "
            "        '$.bus', 'true'"
            "    ) "
            "WHERE trip_id = ? AND stop_sequence < ?",
            (trip_id, boundary_seq),
        )
