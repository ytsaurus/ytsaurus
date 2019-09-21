package mapreduce

import (
	"context"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

type prepareAction func(ctx context.Context) error

// prepare holds state for all actions required to start operation.
type prepare struct {
	mr  *client
	ctx context.Context

	mapperState   *jobState
	reducerState  *jobState
	combinerState *jobState
	tasksState    map[string]*jobState

	spec    *spec.Spec
	actions []prepareAction
}

func (p *prepare) uploadJobState(userScript *spec.UserScript, state *jobState) prepareAction {
	return func(ctx context.Context) error {
		b, err := encodeJob(state)
		if err != nil {
			return err
		}
		tmpPath := ypath.Path("//tmp").Child(guid.New().String())

		_, err = p.mr.yc.CreateNode(ctx, tmpPath, yt.NodeFile, nil)
		if err != nil {
			return err
		}

		w, err := p.mr.yc.WriteFile(ctx, tmpPath, nil)
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

func (p *prepare) addJobCommand(job Job, userScript **spec.UserScript, state *jobState, tableCount int) {
	if *userScript == nil {
		*userScript = &spec.UserScript{}
	}
	(*userScript).Command = jobCommand(job, tableCount)

	state.Job = job
	p.actions = append(p.actions, p.uploadJobState(*userScript, state))
}

func (p *prepare) prepare() error {
	if err := p.mr.uploadSelf(p.ctx); err != nil {
		return err
	}
	p.spec.PatchUserBinary(p.mr.binaryPath)

	for _, inputTablePath := range p.spec.InputTablePaths {
		var tableAttrs struct {
			Typ    yt.NodeType   `yson:"type"`
			Schema schema.Schema `yson:"schema"`
		}

		err := p.mr.yc.GetNode(p.ctx, inputTablePath.YPath().Attrs(), &tableAttrs, nil)
		if yterrors.ContainsResolveError(err) {
			return xerrors.Errorf("mr: input table %v is missing: %w", inputTablePath.YPath(), err)
		} else if err != nil {
			return err
		}

		if tableAttrs.Typ != yt.NodeTable {
			return xerrors.Errorf("mr: input %q is not a table: type=%v", inputTablePath.YPath(), tableAttrs.Typ)
		}
	}

	for _, outputTablePath := range p.spec.OutputTablePaths {
		if ok, err := p.mr.yc.NodeExists(p.ctx, outputTablePath, nil); err != nil {
			return err
		} else if !ok {
			_, err := p.mr.yc.CreateNode(p.ctx, outputTablePath, yt.NodeTable, nil)
			if err != nil {
				return err
			}
		}
	}

	for _, action := range p.actions {
		err := action(p.ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *prepare) start() (*operation, error) {
	if err := p.prepare(); err != nil {
		return nil, err
	}

	id, err := p.mr.operationStartClient().StartOperation(p.ctx, p.spec.Type, p.spec, nil)
	if err != nil {
		return nil, err
	}
	return &operation{c: p.mr.yc, ctx: p.ctx, opID: id}, nil
}
