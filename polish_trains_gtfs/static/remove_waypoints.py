# SPDX-FileCopyrightText: 2026 Marcin Kasznia, Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from impuls import Task, TaskRuntime


class RemoveWaypoints(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute("DELETE FROM stop_times WHERE platform = 'WAYPOINT'")
            r.db.raw_execute(
                "DELETE FROM stops WHERE json_extract(extra_fields_json, '$.waypoint') = '1'"
            )
