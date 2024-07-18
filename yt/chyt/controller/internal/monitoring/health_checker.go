package monitoring

// HealthChecker incapsulates logic about health checking.
type HealthChecker interface {
	CheckHealth() error
}
