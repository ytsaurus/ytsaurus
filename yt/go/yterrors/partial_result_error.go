package yterrors

// PartialResultError is returned by yt.TableReader.Err when a lookup used
// EnablePartialResult and some keys were unavailable.
type PartialResultError struct {
	UnavailableKeyIndexes []int32
}

func (e *PartialResultError) Error() string {
	return "partial lookup result: some keys unavailable"
}

var _ error = &PartialResultError{}
