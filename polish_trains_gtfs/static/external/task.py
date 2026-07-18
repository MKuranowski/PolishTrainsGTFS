# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from abc import abstractmethod

from impuls import Resource, Task


class LoadExternal(Task):
    # When False (default), the task runs early - right after the PKP PLK
    # schedules are loaded - so its stops go through LoadStops' rail-station
    # curation against PLRailMap (e.g. KM's Modlin shuttle, whose stops are
    # registered there). When True, the task runs late - after LoadBusStops -
    # so a self-contained network that carries its own stop coordinates (e.g.
    # ŁKA's local buses) bypasses that curation, which would otherwise reject
    # every stop absent from PLRailMap.
    runs_after_stop_curation: bool = False

    @staticmethod
    @abstractmethod
    def get_required_resources() -> dict[str, Resource]:
        raise NotImplementedError
