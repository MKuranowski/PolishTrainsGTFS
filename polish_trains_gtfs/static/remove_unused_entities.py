# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from impuls.db import DBConnection
from impuls.tasks import RemoveUnusedEntities as BaseRemoveUnusedEntities


class RemoveUnusedEntities(BaseRemoveUnusedEntities):
    def __init__(self, preserve_fallback_stops: bool = False) -> None:
        super().__init__()
        self.preserve_fallback_stops = preserve_fallback_stops

    def drop_stops_without_stop_times(self, db: DBConnection) -> None:
        if self.preserve_fallback_stops:
            r = db.raw_execute(
                "DELETE FROM stops WHERE location_type = 0 AND stop_id NOT LIKE '%FALLBACK%' AND "
                "NOT EXISTS (SELECT 1 FROM stop_times WHERE stop_times.stop_id = stops.stop_id)"
            )
            self.logger.info("Dropped %d non-fallback stops without stop times", r.rowcount)
        else:
            return super().drop_stops_without_stop_times(db)
