# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT


def get_trip_id(schedule_id: int, order_id: int, train_order_id: int | None) -> str:
    return (
        f"{schedule_id}_{order_id}"
        if train_order_id is None
        else f"{schedule_id}_{order_id}_{train_order_id}"
    )
