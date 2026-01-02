# SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import json
from collections.abc import Iterable, Mapping
from typing import IO, Any

import ijson  # type: ignore

Object = Mapping[str, Any]


def first(f: IO[str] | IO[bytes], path: str, /, seek: bool = True) -> Any:
    if seek:
        f.seek(0)
    for item in ijson.items(f, path, use_float=True):
        return item
    return None


def list_iter(f: IO[str] | IO[bytes], path: str, /, seek: bool = True) -> Iterable[Any]:
    assert path.endswith(".item"), 'to iterate over json items, last path component must be "item"'
    if seek:
        f.seek(0)
    return ijson.items(f, path, use_float=True)


def object_iter(
    f: IO[str] | IO[bytes],
    path: str,
    /,
    seek: bool = True,
) -> Iterable[tuple[str, Any]]:
    if seek:
        f.seek(0)
    return ijson.kvitems(f, path, use_float=True)


def dumps(obj: Any, readable: bool = False) -> str:
    return json.dumps(obj, indent=2 if readable else None, separators=(",", ":"))
