package monitoring

import "time"

type HTTPMonitoringConfig struct {
	Clusters                     []string
	Endpoint                     string
	HealthStatusExpirationPeriod time.Duration
}
