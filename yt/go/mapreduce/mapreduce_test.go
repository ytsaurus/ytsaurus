package mapreduce_test

import "go.ytsaurus.tech/yt/go/mapreduce"

type WordCount struct {
	mapreduce.Untyped
}

func (c *WordCount) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	return nil
}

func init() {
	mapreduce.Register(&WordCount{})
}
