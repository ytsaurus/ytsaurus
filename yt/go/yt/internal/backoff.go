package internal

import "github.com/cenkalti/backoff/v4"

var DefaultBackoff = backoff.NewExponentialBackOff
