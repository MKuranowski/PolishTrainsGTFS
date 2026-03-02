# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from typing import cast

from impuls import DBConnection


def trip(db: DBConnection, trip_id: str) -> str:
    with db.raw_execute("SELECT short_name FROM trips WHERE trip_id = ?", (trip_id,)) as q:
        row = q.one()
        if row is None:
            return f"trip {trip_id!r}"  # trip does not exists, short-circuit
        short_name = cast(str, row[0])

    with db.raw_execute(
        "SELECT name FROM stop_times LEFT JOIN stops USING (stop_id) "
        "WHERE trip_id = ? ORDER BY stop_sequence ASC LIMIT 1",
        (trip_id,),
    ) as q:
        row = q.one()
        first_stop = cast(str, row[0]) if row else ""

    with db.raw_execute(
        "SELECT name FROM stop_times LEFT JOIN stops USING (stop_id) "
        "WHERE trip_id = ? ORDER BY stop_sequence DESC LIMIT 1",
        (trip_id,),
    ) as q:
        row = q.one()
        last_stop = cast(str, row[0]) if row else ""

    if first_stop:
        return f"trip {trip_id!r} ({short_name}, {first_stop} -> {last_stop})"
    return f"trip {trip_id!r} ({short_name})"
