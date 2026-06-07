// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package match

import "fmt"

type Stats struct {
	Matched          uint
	Unmatched        uint
	OutsideFeedDates uint
}

func (s Stats) String() string {
	total := s.Matched + s.Unmatched
	percentMatched := 100 * float64(s.Matched) / float64(total)
	return fmt.Sprintf("matched %d / %d (%.2f %%), rejected %d outside of feed dates", s.Matched, total, percentMatched, s.OutsideFeedDates)
}

func (s *Stats) Add(o Stats) {
	s.Matched += o.Matched
	s.Unmatched += o.Unmatched
	s.OutsideFeedDates += o.OutsideFeedDates
}
