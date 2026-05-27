package yterrors

type PartialResultError struct {
	UnavailableKeyIndexes []int32
}

func (e *PartialResultError) Error() string {
	return "partial lookup result: some keys unavailable"
}

var _ error = &PartialResultError{}
