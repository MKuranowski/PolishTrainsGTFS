# SPDX-FileCopyrightText: 2026 Marcin Kasznia, Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from impuls import Task, TaskRuntime


class RenumberStopTimes(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute("UPDATE stop_times SET stop_sequence = stop_sequence + 10000")
            r.db.raw_execute(
                """
                WITH reordered AS (
                    SELECT
                        rowid AS rid,
                        ROW_NUMBER() OVER (
                            PARTITION BY trip_id
                            ORDER BY stop_sequence
                        ) - 1 AS new_stop_sequence
                    FROM stop_times
                )
                UPDATE stop_times
                SET stop_sequence = (
                    SELECT new_stop_sequence
                    FROM reordered
                    WHERE reordered.rid = stop_times.rowid
                )
                """
            )
