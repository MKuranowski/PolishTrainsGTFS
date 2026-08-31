# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Sequence

from impuls import Resource

from .ic_kpd import LoadICKPD
from .km import LoadKM
from .task import LoadExternal

ALL: Sequence[type[LoadExternal]] = [LoadKM, LoadICKPD]


def get_resources() -> dict[str, Resource]:
    r = dict[str, Resource]()
    for s in ALL:
        r.update(**s.get_required_resources())
    return r


def get_tasks() -> list[LoadExternal]:
    return [s() for s in ALL]
