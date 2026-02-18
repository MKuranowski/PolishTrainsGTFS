// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package backoff

import (
	"fmt"
	"time"
)

type Status int

const (
	Success Status = iota
	Failure
	Retry
)

type Backoff struct {
	Period                 time.Duration
	ExponentialBackoffBase time.Duration
	Failures               uint
	MaxBackoffExponent     uint

	lastRun time.Time
	nextRun time.Time
}

func (b *Backoff) StartRun() {
	b.lastRun = time.Now()
}

func (b *Backoff) EndRun(status Status) time.Time {
	switch status {
	case Success:
		b.Failures = 0
		b.nextRun = b.lastRun.Add(b.Period)

	case Failure:
		backoffExponent := b.Failures
		b.Failures++

		if b.MaxBackoffExponent > 0 && backoffExponent > b.MaxBackoffExponent {
			backoffExponent = b.MaxBackoffExponent
		}

		sleep := time.Duration(1<<backoffExponent) * b.getBackoffBase()
		b.nextRun = b.lastRun.Add(sleep)

	case Retry:
		b.Failures = 1
		b.nextRun = b.lastRun.Add(b.getBackoffBase())

	default:
		panic(fmt.Errorf("invalid status enum value: %d", status))
	}

	return b.nextRun
}

func (b *Backoff) Wait() {
	time.Sleep(time.Until(b.nextRun))
}

func (b *Backoff) getBackoffBase() time.Duration {
	if b.ExponentialBackoffBase == 0 {
		return b.Period
	}
	return b.ExponentialBackoffBase
}
