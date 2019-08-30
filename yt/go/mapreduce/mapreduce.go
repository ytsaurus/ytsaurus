// Package mapreduce is client to launching operations over YT.
package mapreduce

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

func jobCommand(job Job, outputPipes int) string {
	return fmt.Sprintf("./go-binary -job %s -output-pipes %d", jobName(job), outputPipes)
}

func encodeJob(job Job) (b bytes.Buffer, err error) {
	enc := gob.NewEncoder(&b)
	err = enc.Encode(&job)
	return
}

func decodeJob(r io.Reader, job *Job) error {
	dec := gob.NewDecoder(r)
	err := dec.Decode(&job)
	return err
}

type action func(ctx *context.Context) error

func (mr *client) newJobCommand(job Job, userScript **spec.UserScript, tableCount int) action {
	if *userScript == nil {
		*userScript = &spec.UserScript{}
	}
	(*userScript).Command = jobCommand(job, tableCount)

	return func(ctx *context.Context) error {
		b, err := encodeJob(job)
		if err != nil {
			return err
		}
		tmpPath := ypath.Path("//tmp").Child(guid.New().String())

		_, err = mr.yc.CreateNode(*ctx, tmpPath, yt.NodeFile, nil)
		if err != nil {
			return err
		}

		w, err := mr.yc.WriteFile(*ctx, tmpPath, nil)
		if err != nil {
			return err
		}

		if _, err = w.Write(b.Bytes()); err != nil {
			return err
		}

		if err = w.Close(); err != nil {
			return err
		}

		(*userScript).FilePaths = append((*userScript).FilePaths, spec.File{
			FileName:    "job-state",
			CypressPath: tmpPath,
			Executable:  false,
		})
		return nil
	}
}

func (mr *client) Map(mapper Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	actions := []action{
		mr.newJobCommand(mapper, &s.Mapper, len(s.OutputTablePaths)),
	}

	return mr.start(s, actions)
}

func (mr *client) Reduce(reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	actions := []action{
		mr.newJobCommand(reducer, &s.Reducer, len(s.OutputTablePaths)),
	}

	return mr.start(s, actions)
}

func (mr *client) JoinReduce(reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	actions := []action{
		mr.newJobCommand(reducer, &s.Reducer, len(s.OutputTablePaths)),
	}

	return mr.start(s, actions)
}

func (mr *client) MapReduce(mapper, reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	var actions []action
	if mapper != nil {
		actions = append(actions, mr.newJobCommand(mapper, &s.Mapper, 1+s.MapperOutputTableCount))
	}
	actions = append(actions, mr.newJobCommand(reducer, &s.Reducer, len(s.OutputTablePaths)-s.MapperOutputTableCount))

	return mr.start(s, actions)
}

func (mr *client) MapCombineReduce(mapper, combiner, reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	var actions []action
	actions = append(actions, mr.newJobCommand(mapper, &s.Mapper, 1+s.MapperOutputTableCount))
	actions = append(actions, mr.newJobCommand(combiner, &s.ReduceCombiner, 1))
	actions = append(actions, mr.newJobCommand(reducer, &s.Reducer, len(s.OutputTablePaths)-s.MapperOutputTableCount))
	return mr.start(s, actions)
}

func (mr *client) Sort(s *spec.Spec) (Operation, error) {
	s = s.Clone()
	return mr.start(s, nil)
}

func (mr *client) Merge(s *spec.Spec) (Operation, error) {
	s = s.Clone()
	return mr.start(s, nil)
}

func (mr *client) Erase(s *spec.Spec) (Operation, error) {
	s = s.Clone()
	return mr.start(s, nil)
}

func (mr *client) RemoteCopy(s *spec.Spec) (Operation, error) {
	s = s.Clone()
	return mr.start(s, nil)
}

func (mr *client) Vanilla(s *spec.Spec, jobs map[string]Job) (Operation, error) {
	s = s.Clone()

	var actions []action
	for name, job := range jobs {
		us, ok := s.Tasks[name]
		if !ok {
			return nil, xerrors.Errorf("yt: task %q is not specified in spec.tasks", name)
		}

		actions = append(actions, mr.newJobCommand(job, &us, 0))
	}

	return mr.start(s, actions)
}
