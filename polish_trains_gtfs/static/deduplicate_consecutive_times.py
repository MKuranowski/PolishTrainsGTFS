# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from itertools import groupby
from operator import itemgetter

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime

QUERY = """
WITH islands AS (
    SELECT
        trip_id,
        stop_sequence,
        (
            trip_id
            || ':'
            || stop_id
            || ':'
            || CAST((row_number() OVER trips - row_number() OVER trip_stops) AS VARCHAR)
        ) as run_id
    FROM stop_times
    WINDOW
        trips AS (PARTITION BY trip_id ORDER BY stop_sequence),
        trip_stops AS (PARTITION BY trip_id, stop_id ORDER BY stop_sequence)
), large_islands AS (
    SELECT run_id
    FROM islands
    GROUP BY run_id
    HAVING COUNT(*) > 1
)
SELECT i.run_id, t.*
FROM islands i
JOIN stop_times t ON (i.trip_id = t.trip_id AND i.stop_sequence = t.stop_sequence)
WHERE run_id IN large_islands
ORDER BY run_id, stop_sequence
"""


class DeduplicateConsecutiveTimes(Task):
    def execute(self, r: TaskRuntime) -> None:
        self.logger.debug("Finding stop time groups to collapse")
        groups_to_collapse = self.find_times_to_collapse(r.db)
        self.logger.warning("Collapsing %d groups of duplicate stop times", len(groups_to_collapse))

        to_update = list[StopTime]()
        to_remove = list[StopTime]()
        for group in groups_to_collapse:
            assert len(group) > 1
            to_update.append(self.merge_stop_time_info(group[0], group[-1]))
            to_remove.extend(group[1:])

        with r.db.transaction():
            r.db.update_many(StopTime, to_update)
            r.db.raw_execute_many(
                "DELETE FROM stop_times WHERE trip_id = ? AND stop_sequence = ?",
                ((i.trip_id, i.stop_sequence) for i in to_remove),
            )

    def find_times_to_collapse(self, db: DBConnection) -> list[list[StopTime]]:
        with db.raw_execute(QUERY) as query:
            return [
                [StopTime.sql_unmarshall(row[1:]) for row in rows]
                for _, rows in groupby(query, itemgetter(0))
            ]

    def merge_stop_time_info(self, arr: StopTime, dep: StopTime) -> StopTime:
        arr.departure_time = dep.departure_time
        arr.platform = dep.platform
        arr.set_extra_field("track", dep.get_extra_field("track"))
        return arr
