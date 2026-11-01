# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from typing import cast

from impuls import DBConnection, Task, TaskRuntime

BERLIN_HBF = "247482"
BERLIN_OSTBAHNHOF = "246776"
FRANKFURT_ODER = "179212"
RZEPIN = "25809"


class FixBerlinBusReplacement(Task):
    """Marks the Berlin Hbf - Frankfurt (Oder) (or Rzepin) stretch as a replacement bus
    on trips terminating at Berlin Hbf without calling at Berlin Ostbahnhof.

    Must run before LoadStations, as it matches on bare station IDs.
    """

    def execute(self, r: TaskRuntime) -> None:
        fixed = 0
        with r.db.transaction():
            for trip_id in self.find_candidate_trips(r.db):
                fixed += self.fix_trip(r.db, trip_id)
        self.logger.info("Marked Berlin replacement bus on %d trips", fixed)

    def find_candidate_trips(self, db: DBConnection) -> list[str]:
        return [
            cast(str, i[0])
            for i in db.raw_execute(
                "SELECT DISTINCT trip_id FROM stop_times WHERE stop_id = ? "
                "AND trip_id NOT IN (SELECT trip_id FROM stop_times WHERE stop_id = ?)",
                (BERLIN_HBF, BERLIN_OSTBAHNHOF),
            )
        ]

    def fix_trip(self, db: DBConnection, trip_id: str) -> bool:
        stops = [
            (cast(int, i[0]), cast(str, i[1]))
            for i in db.raw_execute(
                "SELECT stop_sequence, stop_id FROM stop_times "
                "WHERE trip_id = ? ORDER BY stop_sequence",
                (trip_id,),
            )
        ]

        if stops[-1][1] == BERLIN_HBF:
            bus_condition = "stop_sequence >= ?"
        elif stops[0][1] == BERLIN_HBF:
            bus_condition = "stop_sequence < ?"
        else:
            return False

        boundary = self.find_boundary_sequence(stops)
        if boundary is None:
            self.logger.warning(
                "Trip %s starts or ends at Berlin Hbf without Ostbahnhof, "
                "but calls at neither Frankfurt (Oder) nor Rzepin",
                trip_id,
            )
            return False

        # The westbound boundary stop is a train arrival + bus departure;
        # LoadStations and SplitBusLegs restore the train arrival platform from arrival_platform.
        db.raw_execute(
            "UPDATE stop_times SET platform = 'BUS', "
            "extra_fields_json = json_set(extra_fields_json, '$.track', '') "
            f"WHERE trip_id = ? AND {bus_condition}",
            (trip_id, boundary),
        )
        return True

    @staticmethod
    def find_boundary_sequence(stops: list[tuple[int, str]]) -> int | None:
        for boundary_station in (FRANKFURT_ODER, RZEPIN):
            for seq, stop_id in stops:
                if stop_id == boundary_station:
                    return seq
        return None
