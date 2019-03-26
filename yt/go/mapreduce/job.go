package mapreduce

type JobContext interface {
	LookupVault(name string) (value string, ok bool)
}

type Job interface {
	Do(ctx JobContext, in Reader, out []Writer) error
}
