// Package mapreduce is client to launching operations over YT.
package mapreduce

import (
	"fmt"

	"a.yandex-team.ru/yt/go/mapreduce/spec"
)

func jobCommand(job Job, outputPipes int) string {
	return fmt.Sprintf("./go-binary -job %s -output-pipes %d", jobName(job), outputPipes)
}

func (mr *client) Map(mapper Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	if s.Mapper == nil {
		s.Mapper = &spec.UserScript{}
	}
	s.Mapper.Command = jobCommand(mapper, len(s.OutputTablePaths))
	return mr.start(s)
}

func (mr *client) Reduce(reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	if s.Reducer == nil {
		s.Reducer = &spec.UserScript{}
	}
	s.Reducer.Command = jobCommand(reducer, len(s.OutputTablePaths))
	return mr.start(s)
}

func (mr *client) JoinReduce(reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	if s.Reducer == nil {
		s.Reducer = &spec.UserScript{}
	}
	s.Reducer.Command = jobCommand(reducer, len(s.OutputTablePaths))
	return mr.start(s)
}

func (mr *client) MapReduce(mapper, reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	if s.Mapper != nil {
		s.Mapper = &spec.UserScript{}
	}
	s.Mapper.Command = jobCommand(mapper, 1+s.MapperOutputTableCount)
	if s.Reducer == nil {
		s.Reducer = &spec.UserScript{}
	}
	s.Reducer.Command = jobCommand(reducer, len(s.OutputTablePaths)-s.MapperOutputTableCount)
	return mr.start(s)
}

func (mr *client) MapCombineReduce(mapper, combiner, reducer Job, s *spec.Spec) (Operation, error) {
	s = s.Clone()
	if s.Mapper != nil {
		s.Mapper = &spec.UserScript{}
	}
	s.Mapper.Command = jobCommand(mapper, 1+s.MapperOutputTableCount)
	if s.ReduceCombiner != nil {
		s.ReduceCombiner = &spec.UserScript{}
	}
	s.ReduceCombiner.Command = jobCommand(combiner, 1)
	if s.Reducer == nil {
		s.Reducer = &spec.UserScript{}
	}
	s.Reducer.Command = jobCommand(reducer, len(s.OutputTablePaths)-s.MapperOutputTableCount)
	return mr.start(s)
}

func (mr *client) Sort(s *spec.Spec) (Operation, error) {
	return mr.start(s)
}

func (mr *client) Merge(s *spec.Spec) (Operation, error) {
	return mr.start(s)
}

func (mr *client) Erase(s *spec.Spec) (Operation, error) {
	return mr.start(s)
}

func (mr *client) RemoteCopy(s *spec.Spec) (Operation, error) {
	return mr.start(s)
}

func (mr *client) Vanilla(s *spec.Spec) (Operation, error) {
	return mr.start(s)
}
