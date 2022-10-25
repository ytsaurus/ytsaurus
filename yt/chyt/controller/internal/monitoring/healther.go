package monitoring

import (
	"time"
)

type HealthStatus struct {
	ModificationTime time.Time
	Err              error
}

// Healther incapsulates logic about health checking.
type Healther interface {
	GetHealthStatus() HealthStatus
}
