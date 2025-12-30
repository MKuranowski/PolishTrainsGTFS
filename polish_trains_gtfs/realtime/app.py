# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import json
from argparse import ArgumentParser
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from ..apikey import get_apikey
from .alerts import fetch_alerts
from .delays import fetch_delays


@dataclass
class Args:
    output: Path
    human_readable: bool
    json: bool

    def get_json_path(self) -> Path:
        return self.output.with_suffix(".json")

    @classmethod
    def parse(cls, argv: Sequence[str] | None = None) -> Self:
        arg_parser = ArgumentParser()
        arg_parser.add_argument(
            "-o",
            "--output",
            type=Path,
            default=Path("polish_trains.pb"),
            help="path to output GTFS-Realtime feed",
        )
        arg_parser.add_argument(
            "-r",
            "--human-readable",
            action="store_true",
            help="use human-readable protobuf format (instead of default binary)",
        )
        arg_parser.add_argument(
            "-j",
            "--json",
            action="store_true",
            help="also write realtime data in json format",
        )
        args = arg_parser.parse_args(argv)
        return cls(args.output, args.human_readable, args.json)


def main(argv: Sequence[str] | None = None) -> None:
    args = Args.parse(argv)
    apikey = get_apikey()

    facts = fetch_alerts(apikey).merge(fetch_delays(apikey))

    update_file(
        str(facts.as_gtfs_rt()) if args.human_readable else facts.as_gtfs_rt().SerializeToString(),
        args.output,
    )
    if args.json:
        update_file(
            json.dumps(
                facts.as_json(),
                indent=2 if args.human_readable else None,
                separators=(",", ":"),
            ),
            args.get_json_path(),
        )


def update_file(s: bytes | str, dst: Path):
    tmp = dst.with_name(f".{dst.name}.tmp")
    if isinstance(s, bytes):
        tmp.write_bytes(s)
    else:
        tmp.write_text(s, encoding="utf-8")
    tmp.rename(dst)
