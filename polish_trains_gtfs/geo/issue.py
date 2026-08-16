# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Iterable
from typing import Self

from impuls.errors import DataError

from .model import Entity


class Issue(DataError):
    """A DataError with a geographic Entity."""

    entity: Entity
    description: str

    def __init__(self, entity: Entity, description: str) -> None:
        super().__init__(f"{entity}: {description}")
        self.entity = entity
        self.description = description

    @classmethod
    def many(cls, entities: Iterable[Entity], description: str) -> Iterable[Self]:
        for entity in entities:
            yield cls(entity, description)
