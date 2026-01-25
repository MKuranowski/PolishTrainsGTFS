// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package backoff

import "time"

const (
	Success = true
	Failure = false
)

type Backoff struct {
	Period             time.Duration
	Failures           uint
	MaxBackoffExponent uint

	lastRun time.Time
	nextRun time.Time
}

func (b *Backoff) StartRun() {
	b.lastRun = time.Now()
}

func (b *Backoff) EndRun(success bool) time.Time {
	if success {
		b.Failures = 0
		b.nextRun = b.lastRun.Add(b.Period)
	} else {
		b.Failures++
		backoffExponent := b.Failures - 1
		if b.MaxBackoffExponent > 0 && backoffExponent > b.MaxBackoffExponent {
			backoffExponent = b.MaxBackoffExponent
		}
		sleep := time.Duration(pow(uint(b.Period), backoffExponent))
		b.nextRun = b.lastRun.Add(sleep)
	}
	return b.nextRun
}

func (b *Backoff) Wait() {
	time.Sleep(time.Until(b.nextRun))
}

func pow(base, exp uint) uint {
	r := uint(1)
	for exp > 0 {
		if exp&1 == 1 {
			r *= base
		}
		base *= base
		exp >>= 1
	}
	return r
}
