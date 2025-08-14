// Package mapreduce is client to launching operations over YT.
package mapreduce

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/yt"
)

func jobCommand(job any, outputPipes int) string {
	return fmt.Sprintf("./go-binary -job %s -output-pipes %d", jobName(job), outputPipes)
}

func encodeJob(job *jobState) (b bytes.Buffer, err error) {
	enc := gob.NewEncoder(&b)
	err = enc.Encode(&job)
	return
}

func decodeJob(r io.Reader) (state *jobState, err error) {
	dec := gob.NewDecoder(r)
	err = dec.Decode(&state)
	return
}

func (mr *client) prepare(spec *spec.Spec) *prepare {
	return &prepare{
		mr:   mr,
		spec: spec.Clone(),
		ctx:  mr.ctx,
	}
}

func (mr *client) Map(mapper Job, s *spec.Spec, opts ...OperationOption) (Operation, error) {
	p := mr.prepare(s)
	p.mapperState = new(jobState)
	if err := p.addJobCommand(mapper, &p.spec.Mapper, p.mapperState, len(p.spec.OutputTablePaths), opts); err != nil {
		return nil, err
	}
	return p.start(opts)
}

func (mr *client) RawMap(mapper RawJob, s *spec.Spec, opts ...OperationOption) (Operation, error) {
	p := mr.prepare(s)
	p.mapperState = new(jobState)
	if err := p.addJobCommand(mapper, &p.spec.Mapper, p.mapperState, len(p.spec.OutputTablePaths), opts); err != nil {
		return nil, err
	}
	return p.start(opts)
}

func (mr *client) Reduce(reducer Job, s *spec.Spec, opts ...OperationOption) (Operation, error) {
	p := mr.prepare(s)
	p.reducerState = new(jobState)
	if err := p.addJobCommand(reducer, &p.spec.Reducer, p.reducerState, len(p.spec.OutputTablePaths), opts); err != nil {
		return nil, err
	}
	return p.start(opts)
}

func (mr *client) JoinReduce(reducer Job, s *spec.Spec, opts ...OperationOption) (Operation, error) {
	p := mr.prepare(s)
	p.reducerState = new(jobState)
	if err := p.addJobCommand(reducer, &p.spec.Reducer, p.reducerState, len(p.spec.OutputTablePaths), opts); err != nil {
		return nil, err
	}
	return p.start(opts)
}

func (mr *client) MapReduce(mapper, reducer Job, s *spec.Spec, opts ...OperationOption) (Operation, error) {
	p := mr.prepare(s)
	if mapper != nil {
		p.mapperState = new(jobState)
		if err := p.addJobCommand(mapper, &p.spec.Mapper, p.mapperState, 1+p.spec.MapperOutputTableCount, opts); err != nil {
			return nil, err
		}
	}

	p.reducerState = new(jobState)
	err := p.addJobCommand(
		reducer,
		&p.spec.Reducer,
		p.reducerState,
		len(p.spec.OutputTablePaths)-p.spec.MapperOutputTableCount,
		append(opts, disableIndexControlAttributes(), disableGetInputTablesSchema()), // Reduce phase does not support indexes (table, row and range).
	)
	if err != nil {
		return nil, err
	}
	return p.start(opts)
}

func (mr *client) MapCombineReduce(mapper, combiner, reducer Job, s *spec.Spec, opts ...OperationOption) (Operation, error) {
	p := mr.prepare(s)

	p.mapperState = new(jobState)
	if err := p.addJobCommand(mapper, &p.spec.Mapper, p.mapperState, 1+p.spec.MapperOutputTableCount, opts); err != nil {
		return nil, err
	}

	p.combinerState = new(jobState)
	err := p.addJobCommand(
		combiner,
		&p.spec.ReduceCombiner,
		p.combinerState,
		1,
		append(opts, disableIndexControlAttributes(), disableGetInputTablesSchema()), // Reduce phase does not support indexes (table, row and range).
	)
	if err != nil {
		return nil, err
	}

	p.reducerState = new(jobState)
	err = p.addJobCommand(
		reducer,
		&p.spec.Reducer,
		p.reducerState,
		len(p.spec.OutputTablePaths)-p.spec.MapperOutputTableCount,
		append(opts, disableIndexControlAttributes(), disableGetInputTablesSchema()), // Reduce phase does not support indexes (table, row and range).
	)
	if err != nil {
		return nil, err
	}
	return p.start(opts)
}

func (mr *client) Sort(s *spec.Spec, opts ...OperationOption) (Operation, error) {
	return mr.prepare(s).start(opts)
}

func (mr *client) Merge(s *spec.Spec, opts ...OperationOption) (Operation, error) {
	return mr.prepare(s).start(opts)
}

func (mr *client) Erase(s *spec.Spec, opts ...OperationOption) (Operation, error) {
	return mr.prepare(s).start(opts)
}

func (mr *client) RemoteCopy(s *spec.Spec, opts ...OperationOption) (Operation, error) {
	return mr.prepare(s).start(opts)
}

func (mr *client) Vanilla(s *spec.Spec, jobs map[string]Job, opts ...OperationOption) (Operation, error) {
	p := mr.prepare(s)
	p.tasksState = map[string]*jobState{}

	for name, job := range jobs {
		us, ok := p.spec.Tasks[name]
		if !ok {
			return nil, xerrors.Errorf("yt: task %q is not specified in spec.tasks", name)
		}

		state := new(jobState)
		p.tasksState[name] = state

		if err := p.addJobCommand(job, &us, state, len(us.OutputTablePaths), opts); err != nil {
			return nil, err
		}
	}

	return p.start(opts)
}

func (mr *client) Track(opID yt.OperationID) (Operation, error) {
	return &operation{yc: mr.yc, ctx: mr.ctx, opID: opID}, nil
}

func (mr *client) WithTx(tx yt.Tx) Client {
	return &client{
		ctx:         mr.ctx,
		yc:          mr.yc,
		tx:          tx,
		config:      mr.config,
		jobStateKey: mr.jobStateKey,
		aead:        mr.aead,
	}
}
