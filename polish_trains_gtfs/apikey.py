# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import os


def get_apikey() -> str:
    key = os.getenv("PKP_PLK_APIKEY")
    if not key and (path := os.getenv("PKP_PLK_APIKEY_FILE")):
        with open(path, "r") as f:
            key = f.read()

    if not key:
        raise ValueError("PKP_PLK_APIKEY environment variable not set")

    return key.strip()
