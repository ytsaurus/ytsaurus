package monitoring

// HealthChecker encapsulates logic about health checking.
type HealthChecker interface {
	CheckHealth() error
}
