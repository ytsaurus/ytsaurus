// package mapreduce is client to launching operations over YT.
package mapreduce

import (
	"context"
	"fmt"

	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
)

type Reader interface {
	TableIndex() int
	Scan(value interface{}) error
	Next() bool
}

type Writer interface {
	Write(value interface{}) error
}

type JobContext interface {
}

type Job interface {
	Do(ctx JobContext, in Reader, out []Writer) error
}

func Map(job Job, baseSpec *spec.Spec) *spec.Spec {
	s := baseSpec.Clone()

	env := map[string]string{}
	if s.Mapper != nil && s.Mapper.Environment != nil {
		for k, v := range s.Mapper.Environment {
			env[k] = v
		}
	}

	env["YT_INSIDE_JOB"] = "1"

	s.Type = yt.OperationMap
	s.Mapper = &spec.UserScript{
		Command:     fmt.Sprintf("./go-binary -job %s -output-pipes %d", jobName(job), len(s.OutputTablePaths)),
		Environment: env,
	}

	return s
}

type Operation interface {
	Wait() error
}

type Client interface {
	Run(ctx context.Context, spec *spec.Spec) (Operation, error)
}

func New(c yt.Client) Client {
	return &client{c: c}
}
