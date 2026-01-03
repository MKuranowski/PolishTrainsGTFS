# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Date

SECOND = 1
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR


class ShiftNegativeTimes(Task):
    def execute(self, r: TaskRuntime) -> None:
        to_shift = self.find_trips_to_shift(r.db)
        if not to_shift:
            return

        self.logger.info("Shifting %d trips starting with negative times", len(to_shift))
        with r.db.transaction():
            for trip_id, day_offset in to_shift:
                self.shift_trip(r.db, trip_id, day_offset)

    def find_trips_to_shift(self, db: DBConnection) -> list[tuple[str, int]]:
        return [
            (cast(str, i[0]), cast(int, i[1]))
            for i in db.raw_execute(
                "SELECT trip_id, CAST(floor(CAST(arrival_time AS REAL) / ?) AS INTEGER) "
                "FROM stop_times "
                "WHERE stop_sequence = 0 AND arrival_time < 0",
                (DAY,),
            )
        ]

    def shift_trip(self, db: DBConnection, trip_id: str, day_offset: int) -> None:
        old_calendar_id = cast(
            str,
            db.raw_execute(
                "SELECT calendar_id FROM trips WHERE trip_id = ?",
                (trip_id,),
            ).one_must("missing trip")[0],
        )
        new_calendar_id = self.get_shifted_calendar_id(db, old_calendar_id, day_offset)
        db.raw_execute(
            "UPDATE trips SET calendar_id = ? WHERE calendar_id = ?",
            (new_calendar_id, old_calendar_id),
        )

        time_offset = DAY * -day_offset
        db.raw_execute(
            "UPDATE stop_times SET arrival_time = arrival_time + ?, "
            "departure_time = departure_time + ? WHERE trip_id = ?",
            (time_offset, time_offset, trip_id),
        )

    def get_shifted_calendar_id(self, db: DBConnection, old: str, day_offset: int) -> str:
        new = f"{old}{day_offset:+}D"

        # Check if the shifted calendar already exists
        if db.raw_execute("SELECT 1 FROM calendars WHERE calendar_id = ?", (new,)).one():
            return new

        # Get the shifted dates
        new_dates = [
            Date.from_ymd_str(cast(str, i[0])).add_days(day_offset)
            for i in db.raw_execute(
                "SELECT date FROM calendar_exceptions WHERE calendar_id = ?",
                (old,),
            )
        ]

        # Insert the shifted calendar
        db.raw_execute("INSERT INTO calendars (calendar_id) VALUES (?)", (new,))
        db.raw_execute_many(
            "INSERT INTO calendar_exceptions (calendar_id, date, exception_type) VALUES (?, ?, 1)",
            ((new, d.isoformat()) for d in new_dates),
        )

        return new
