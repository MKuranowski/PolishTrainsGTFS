# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import chain
from typing import cast

from impuls import DBConnection, Task, TaskRuntime

AGENCIES_WITHOUT_NAMES = {"SKM", "SKMT"}

UPPER_CASE_WORDS: Sequence[re.Pattern[str]] = [
    re.compile(r"\bESKO\b", re.I),
    re.compile(r"\bKD\b", re.I),
    re.compile(r"\bKW\b", re.I),
    re.compile(r"\bPKM[0-9]", re.I),
    re.compile(r"\bZKA\b", re.I),
    re.compile(r"\bZSSK\b", re.I),
]

LOWER_CASE_WORDS: Sequence[re.Pattern[str]] = [
    re.compile(r"\bdo\b", re.I),
    re.compile(r"\bi\b", re.I),
    re.compile(r"\bna\b", re.I),
    re.compile(r"\bod\b", re.I),
]

INVALID_NAMES: Mapping[str, Sequence[re.Pattern[str]]] = {
    # Key "" can be used to apply a pattern to all agencies
    "KD": [re.compile(r"^DKA$")],
    "KS": [re.compile(r"^S[0-9S/]+\s*")],
    "KW": [re.compile(r"^PKM[0-9](?:/PKM[0-9])*\s*")],
    "LEO": [re.compile(r"^LEO EXPRESS$", re.I)],
    "PR": [
        re.compile(r"^F7/D18$"),
        re.compile(r"^KZ\s?[0-9]$"),
        re.compile(r"^PKM[0-9]$"),
        re.compile(r"^RB[0-9][0-9]?$"),
        re.compile(r"^S[0-9]+$"),
        re.compile(r"^SKA[0-9]$"),
    ],
    "RJ": [re.compile(r"^REGIOJET$", re.I)],
}


@dataclass
class TrainWithName:
    trip_id: str
    agency_id: str
    name: str


class AddTrainNames(Task):
    def execute(self, r: TaskRuntime) -> None:
        to_update = [
            (name, train.trip_id)
            for train in self.get_all_trains_with_names(r.db)
            if (name := get_normalized_name(train.name, train.agency_id))
        ]
        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE trips SET short_name = short_name || ' ' || ? WHERE trip_id = ?",
                to_update,
            )

    def get_all_trains_with_names(self, db: DBConnection) -> Iterable[TrainWithName]:
        q = db.raw_execute(
            "SELECT trip_id, agency_id, json_extract(trips.extra_fields_json, '$.plk_train_name') "
            "FROM trips LEFT JOIN routes USING (route_id) "
            "WHERE COALESCE(json_extract(trips.extra_fields_json, '$.plk_train_name'), '') != ''",
        )
        for row in q:
            yield TrainWithName(
                trip_id=cast(str, row[0]),
                agency_id=cast(str, row[1]),
                name=cast(str, row[2]),
            )


def get_normalized_name(name: str, agency_id: str = "") -> str:
    if agency_id in AGENCIES_WITHOUT_NAMES:
        return ""
    return normalize_case(strip_invalid_name_parts(name, agency_id))


def strip_invalid_name_parts(name: str, agency_id: str = "") -> str:
    if agency_id:
        patterns = chain(INVALID_NAMES.get("", []), INVALID_NAMES.get(agency_id, []))
    else:
        patterns = INVALID_NAMES.get("", [])

    for pattern in patterns:
        name = pattern.sub("", name)

    return name


def normalize_case(name: str) -> str:
    name = name.title()
    for pattern in UPPER_CASE_WORDS:
        name = pattern.sub(lambda m: m[0].upper(), name)
    for pattern in LOWER_CASE_WORDS:
        name = pattern.sub(lambda m: m[0].lower(), name)
    return name
