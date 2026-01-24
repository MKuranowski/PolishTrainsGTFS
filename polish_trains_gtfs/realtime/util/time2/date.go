// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package time2

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var dateParseRegex = regexp.MustCompile(`^([0-9]{4})[[:punct:]]?([0-9]{2})[[:punct:]]?([0-9]{2})`)

type ErrInvalidDate string

func (e ErrInvalidDate) Error() string {
	return fmt.Sprintf("invalid date string: %q", string(e))
}

type Date struct {
	Y    uint16
	M, D uint8
}

func (d Date) IsValid() bool {
	return d.M >= 1 && d.M <= 12 && d.D >= 1 && d.D <= DaysInMonth(d.Y, d.M)
}

func (d Date) StringSeparator(sep string) string {
	return fmt.Sprintf("%04d%s%02d%s%02d", d.Y, sep, d.M, sep, d.D)
}

func (d Date) String() string {
	return d.StringSeparator("-")
}

func (d Date) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Date) UnmarshalText(text []byte) error {
	s := string(text)
	m := dateParseRegex.FindStringSubmatch(s)
	if m == nil {
		return ErrInvalidDate(s)
	}

	year, err := strconv.ParseUint(m[1], 10, 16)
	if err != nil {
		return ErrInvalidDate(s)
	}

	month, err := strconv.ParseUint(m[2], 10, 8)
	if err != nil {
		return ErrInvalidDate(s)
	}

	day, err := strconv.ParseUint(m[3], 10, 8)
	if err != nil {
		return ErrInvalidDate(s)
	}

	d.Y = uint16(year)
	d.M = uint8(month)
	d.D = uint8(day)
	if !d.IsValid() {
		return ErrInvalidDate(s)
	}
	return nil
}

func (d Date) Weekday() time.Weekday {
	return time.Date(int(d.Y), time.Month(d.M), int(d.D), 12, 0, 0, 0, time.UTC).Weekday()
}

func (d Date) Next() Date {
	if d.M == 12 && d.D == 31 {
		return Date{d.Y + 1, 1, 1}
	} else if d.D == DaysInMonth(d.Y, d.M) {
		return Date{d.Y, d.M + 1, 1}
	}
	return Date{d.Y, d.M, d.D + 1}
}

func (d Date) Previous() Date {
	if d.M == 1 && d.D == 1 {
		return Date{d.Y - 1, 12, 31}
	} else if d.D == 1 {
		return Date{d.Y, d.M - 1, DaysInMonth(d.Y, d.M-1)}
	}
	return Date{d.Y, d.M, d.D - 1}
}

func (d Date) Shifted(delta int) Date {
	previous := false
	if delta < 0 {
		previous = true
		delta = -delta
	}

	for range delta {
		if previous {
			d = d.Previous()
		} else {
			d = d.Next()
		}
	}

	return d
}

func (d Date) After(o Date) bool {
	return d.Y > o.Y || (d.Y == o.Y && d.M > o.M) || (d.Y == o.Y && d.M == o.M && d.D > o.D)
}

func (d Date) Before(o Date) bool {
	return d.Y < o.Y || (d.Y == o.Y && d.M < o.M) || (d.Y == o.Y && d.M == o.M && d.D < o.D)
}

func IsLeap(y uint16) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

func DaysInMonth(y uint16, m uint8) uint8 {
	switch m {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31

	case 4, 6, 9, 11:
		return 30

	case 2:
		if IsLeap(y) {
			return 29
		}
		return 28

	default:
		return 0
	}
}
