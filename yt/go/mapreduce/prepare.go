package mapreduce

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/tink/go/keyset"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/core/buildinfo"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type prepareAction func(ctx context.Context, p *prepare) error

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
	return func(ctx context.Context, p *prepare) error {
		doUpload := func() error {
			b, err := encodeJob(state)
			if err != nil {
				return backoff.Permanent(err)
			}

			ct, err := p.mr.aead.Encrypt(b.Bytes(), nil)
			if err != nil {
				return backoff.Permanent(err)
			}

			id := guid.New().String()
			tmpPath := ypath.Path("//tmp/go_job_state").Child(id[:2]).Child(id)

			_, err = p.mr.yc.CreateNode(ctx, tmpPath, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
			if err != nil {
				return err
			}

			w, err := p.mr.yc.WriteFile(ctx, tmpPath, nil)
			if err != nil {
				return err
			}

			if _, err = w.Write(ct); err != nil {
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

		return backoff.Retry(doUpload,
			backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
	}
}

func (p *prepare) addJobCommand(job any, userScript **spec.UserScript, state *jobState, tableCount int) {
	if *userScript == nil {
		*userScript = &spec.UserScript{}
	}
	(*userScript).Command = jobCommand(job, tableCount)
	p.setGoMaxProc(*userScript)

	state.Job = job
	p.actions = append(p.actions, p.uploadJobState(*userScript, state))
}

func (p *prepare) setGoMaxProc(spec *spec.UserScript) {
	if spec == nil {
		return
	}
	maxProc := 1
	if spec.CPULimit > 0 {
		maxProc = int(math.Ceil(float64(spec.CPULimit)))
	}
	if spec.Environment == nil {
		spec.Environment = make(map[string]string)
	}
	if _, ok := spec.Environment[GoMaxProcEnvName]; !ok {
		spec.Environment[GoMaxProcEnvName] = strconv.Itoa(maxProc)
	}
}

func isSelfUploadOperationType(operationType yt.OperationType) bool {
	switch operationType {
	case yt.OperationRemoteCopy, yt.OperationMerge, yt.OperationSort, yt.OperationErase:
		return false
	}
	return true
}

func (p *prepare) prepare(opts []OperationOption) error {
	skipSelfUpload := !isSelfUploadOperationType(p.spec.Type)
	for _, opt := range opts {
		switch opt := opt.(type) {
		case *localFilesOption:
			p.actions = append(p.actions, opt.uploadLocalFiles)
		case *skipSelfUploadOption:
			skipSelfUpload = true
		case *startOperationOptsOption:
		default:
			panic(fmt.Sprintf("unsupported option type %T", opt))
		}
	}

	if !skipSelfUpload {
		if err := p.mr.uploadSelf(p.ctx); err != nil {
			return err
		}
		p.spec.PatchUserBinary(p.mr.binaryPath)
	}

	if p.spec.SecureVault == nil {
		p.spec.SecureVault = map[string]string{}
	}

	var jobStateKey bytes.Buffer
	if err := p.mr.jobStateKey.Write(keyset.NewJSONWriter(&jobStateKey), &plaintextAEAD{}); err != nil {
		return err
	}
	p.spec.SecureVault["job_state_key"] = jobStateKey.String()

	if len(p.spec.ACL) == 0 || len(p.mr.defaultACL) != 0 {
		p.spec.ACL = p.mr.defaultACL
	}

	if err := setStartedBy(p.spec); err != nil {
		return err
	}

	var cypress yt.CypressClient = p.mr.yc
	if p.mr.tx != nil {
		cypress = p.mr.tx
	}

	if p.spec.Type != yt.OperationRemoteCopy {
		for _, inputTablePath := range p.spec.InputTablePaths {
			var tableAttrs struct {
				Typ    yt.NodeType   `yson:"type"`
				Schema schema.Schema `yson:"schema"`
			}

			err := cypress.GetNode(p.ctx, inputTablePath.YPath().Attrs(), &tableAttrs, nil)
			if yterrors.ContainsResolveError(err) {
				return xerrors.Errorf("mr: input table %v is missing: %w", inputTablePath.YPath(), err)
			} else if err != nil {
				return err
			}

			if tableAttrs.Typ != yt.NodeTable {
				return xerrors.Errorf("mr: input %q is not a table: type=%v", inputTablePath.YPath(), tableAttrs.Typ)
			}
		}
	}

	createOutputTable := func(path ypath.YPath) error {
		if ok, err := cypress.NodeExists(p.ctx, path, nil); err != nil {
			return err
		} else if !ok {
			_, err := cypress.CreateNode(p.ctx, path, yt.NodeTable, nil)
			if err != nil {
				return err
			}
		}

		return nil
	}

	for _, outputTablePath := range p.spec.OutputTablePaths {
		if err := createOutputTable(outputTablePath); err != nil {
			return err
		}
	}

	for _, us := range p.spec.Tasks {
		for _, outputTablePath := range us.OutputTablePaths {
			if err := createOutputTable(outputTablePath); err != nil {
				return err
			}
		}
	}

	for _, action := range p.actions {
		err := action(p.ctx, p)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *prepare) start(opts []OperationOption) (*operation, error) {
	if err := p.prepare(opts); err != nil {
		return nil, err
	}

	var startOperationOptions *yt.StartOperationOptions
	for _, opt := range opts {
		startOperationOptsOption, ok := opt.(*startOperationOptsOption)
		if ok {
			startOperationOptions = startOperationOptsOption.options
			break
		}
	}

	id, err := p.startOperation(startOperationOptions)
	if err != nil {
		return nil, err
	}
	return &operation{yc: p.mr.yc, ctx: p.ctx, opID: *id}, nil
}

func (p *prepare) startOperation(opts *yt.StartOperationOptions) (*yt.OperationID, error) {
	expBackoff := backoff.WithContext(backoff.NewExponentialBackOff(), p.ctx)
	for {
		id, err := p.mr.operationStartClient().StartOperation(p.ctx, p.spec.Type, p.spec, opts)
		if err == nil {
			return &id, nil
		}
		if !(p.mr.config.ShouldRetryTooManyOperationsError && yterrors.ContainsErrorCode(err, yterrors.CodeTooManyOperations)) {
			return nil, err
		}

		b := expBackoff.NextBackOff()
		if b == backoff.Stop {
			return nil, err
		}

		select {
		case <-time.After(b):
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		}
	}
}

func setStartedBy(spec *spec.Spec) error {
	u, err := user.Current()
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	execPath, err := os.Executable()
	if err != nil {
		return err
	}

	spec.StartedBy = map[string]any{
		"binary":          execPath,
		"binary_name":     filepath.Base(execPath),
		"hostname":        hostname,
		"pid":             os.Getpid(),
		"user":            u.Username,
		"wrapper_version": fmt.Sprintf("yt/go/mapreduce@%s", buildinfo.Info.ArcadiaSourceRevision),
	}
	return nil
}
