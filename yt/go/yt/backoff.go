package yt

import "time"

type BackoffStrategy interface {
	Backoff(retries int) time.Duration
}
