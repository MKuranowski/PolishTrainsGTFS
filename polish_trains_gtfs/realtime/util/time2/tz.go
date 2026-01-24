// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package time2

import (
	"fmt"
	"time"
)

var PolishTimezone *time.Location

func init() {
	var err error
	PolishTimezone, err = time.LoadLocation("Europe/Warsaw")
	if err != nil {
		panic(fmt.Errorf("failed to load Europe/Warsaw timezone: %w", err))
	}
}
