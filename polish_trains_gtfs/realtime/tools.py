# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Self

from .gtfs_realtime_pb2 import TranslatedString, TripDescriptor


@dataclass
class TripDate:
    trip_id: int
    start_date: date

    @classmethod
    def parse(cls, schedule_id: int, operating_date: str) -> Self:
        return cls(schedule_id, date.fromisoformat(operating_date[:10]))

    def as_json(self) -> Mapping[str, str]:
        return {"trip_id": str(self.trip_id), "start_date": self.start_date.isoformat()}

    def as_gtfs_rt(self) -> TripDescriptor:
        return TripDescriptor(
            trip_id=str(self.trip_id),
            schedule_relationship=TripDescriptor.SCHEDULED,
            start_date=self.start_date.strftime("%Y%m%d"),
        )


def as_translation(x: str, lang: str = "pl") -> TranslatedString:
    return TranslatedString(translation=[TranslatedString.Translation(x, lang)])
