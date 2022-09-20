package monitoring

// Healther incapsulates logic about health checking.
type Healther interface {
	IsHealthy() bool
}
