# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Sequence
from typing import Type

from impuls import Resource

from .km import LoadKM
from .lka import LoadLKA
from .task import LoadExternal

ALL: Sequence[Type[LoadExternal]] = [LoadKM, LoadLKA]


def get_resources() -> dict[str, Resource]:
    r = dict[str, Resource]()
    for s in ALL:
        r.update(**s.get_required_resources())
    return r


def get_early_tasks() -> list[LoadExternal]:
    """External loaders that run before LoadStops (PLRailMap-curated stops)."""
    return [s() for s in ALL if not s.runs_after_stop_curation]


def get_late_tasks() -> list[LoadExternal]:
    """External loaders that run after LoadBusStops (self-contained stops)."""
    return [s() for s in ALL if s.runs_after_stop_curation]
