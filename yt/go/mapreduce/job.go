package mapreduce

type JobContext interface {
	LookupVault(name string) (value string, ok bool)
	JobCookie() int
}

type Job interface {
	Do(ctx JobContext, in Reader, out []Writer) error

	// InputTypes returns list of types this job expects to receive as inputs.
	//
	// Each element describing type of corresponding input table.
	InputTypes() []any

	// OutputTypes returns list of types this job produces as outputs.
	//
	// Each element describing type of corresponding output table.
	OutputTypes() []any
}

// Untyped is empty struct useful for embedding inside user job type, it provides default implementation of
// InputTypes() and OutputTypes() methods.
type Untyped struct{}

func (Untyped) InputTypes() []any {
	return nil
}

func (Untyped) OutputTypes() []any {
	return nil
}
