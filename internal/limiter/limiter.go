package limiter

import (
	"time"
)

type OutgoingLimiter struct {
	ticker    <-chan time.Time
	rateLimit time.Duration
}

func NewOutgoingLimiter(rate time.Duration) *OutgoingLimiter {
	return &OutgoingLimiter{ticker: time.Tick(rate), rateLimit: rate}
}

func (limiter *OutgoingLimiter) WaitForOutgoingLimiter() {
	<-limiter.ticker
}
