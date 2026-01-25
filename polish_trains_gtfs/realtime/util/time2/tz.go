// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package time2

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

var PolishTimezone *time.Location

var rfc3339OffsetRegex = regexp.MustCompile(`Z|[+-][0-9]{1,2}:?[0-9]{0,2}$`)

func init() {
	var err error
	PolishTimezone, err = time.LoadLocation("Europe/Warsaw")
	if err != nil {
		panic(fmt.Errorf("failed to load Europe/Warsaw timezone: %w", err))
	}
}

type LocalTime time.Time

func (t *LocalTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*t = LocalTime{}
		return nil
	}
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return errors.New("LocalTime.UnmarshalJSON: input is not a JSON string")
	}
	return t.UnmarshalText(data[len(`"`) : len(data)-len(`"`)])
}

func (t *LocalTime) UnmarshalText(data []byte) error {
	var err error
	var parsed time.Time

	if rfc3339OffsetRegex.Match(data) {
		parsed, err = time.Parse(time.RFC3339, string(data))
	} else {
		parsed, err = time.ParseInLocation("2006-01-02T15:04:05", string(data), PolishTimezone)
	}

	*t = LocalTime(parsed)
	return err
}
