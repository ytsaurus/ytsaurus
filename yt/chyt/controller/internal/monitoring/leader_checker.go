package monitoring

type LeaderChecker interface {
	// IsLeader returns whether app is running controllers or not.
	IsLeader() bool
}
