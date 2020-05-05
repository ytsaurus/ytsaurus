package jobbatch

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"a.yandex-team.ru/yt/go/mapreduce"
)

type AggregateJob struct {
	mapreduce.Untyped

	JobStates [][]byte
}

func (a *AggregateJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	cookie := ctx.JobCookie()
	if cookie >= len(a.JobStates) {
		return fmt.Errorf("job cookie out of range: %d >= %d", cookie, len(a.JobStates))
	}

	var job mapreduce.Job
	dec := gob.NewDecoder(bytes.NewBuffer(a.JobStates[cookie]))
	if err := dec.Decode(&job); err != nil {
		return err
	}

	return job.Do(ctx, in, out)
}

func init() {
	mapreduce.Register(&AggregateJob{})
}
