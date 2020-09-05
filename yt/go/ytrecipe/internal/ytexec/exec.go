package ytexec

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"a.yandex-team.ru/library/go/core/log"
	zaplog "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/blobtable"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/blobcache"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/job"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/secret"
)

type Exec struct {
	l      log.Structured
	config Config
	yc     yt.Client
	mr     mapreduce.Client
	cache  *blobcache.Cache

	op        mapreduce.Operation
	outputDir ypath.Path

	execedAt    time.Time
	scheduledAt time.Time
}

func New(c Config) (*Exec, error) {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{c.Exec.ExecLog}
	cfg.Level.SetLevel(zapcore.DebugLevel)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	l, err := zaplog.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	var ytToken string
	if c.Exec.YTTokenEnv != "" {
		ytToken = os.Getenv(c.Exec.YTTokenEnv)
	} else {
		ytToken, err = secret.GetYTToken()
		if err != nil {
			return nil, err
		}
	}

	ytc := &yt.Config{
		Proxy:  c.Operation.Cluster,
		Logger: l,
		Token:  ytToken,
	}

	yc, err := ythttp.NewClient(ytc)
	if err != nil {
		return nil, err
	}

	e := &Exec{
		l:      l,
		config: c,
		yc:     yc,
		mr:     mapreduce.New(yc),
	}

	e.execedAt = time.Now()

	if e.config.Operation.CoordinateUpload {
		hostname, _ := os.Hostname()

		e.cache = blobcache.NewCache(l, yc, blobcache.Config{
			ProcessName:      fmt.Sprintf("%d at %s", os.Getpid(), hostname),
			Root:             c.Operation.CypressRoot,
			UploadTimeout:    time.Minute,
			UploadPingPeriod: time.Second * 15,
			EntryTTL:         c.Operation.BlobTTL,
		})
	}

	return e, nil
}

func (e *Exec) createOutputDir(ctx context.Context) (ypath.Path, error) {
	id := guid.New().String()
	path := e.config.Operation.OutputDir().Child(id[:2]).Child(id)
	ttl := e.config.Operation.OutputTTL

	_, err := e.yc.CreateNode(ctx, path, yt.NodeMap, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"expiration_time": yson.Time(time.Now().Add(ttl)),
			"fs_config":       e.config.FS,
		},
		Recursive: true,
	})

	return path, err
}

func (e *Exec) downloadJobLog(ctx context.Context, stderrTable ypath.Path) error {
	jobLog, err := os.Create(e.config.Exec.JobLog)
	if err != nil {
		return err
	}

	r, err := blobtable.ReadBlobTable(ctx, e.yc, stderrTable)
	if err != nil {
		return err
	}

	for r.Next() {
		_, err = io.Copy(jobLog, r)
		if err != nil {
			return err
		}
	}

	return r.Err()
}

func (e *Exec) Run(ctx context.Context) error {
	j, s, outDir, err := e.PrepareJob(ctx)
	if err != nil {
		return err
	}

	e.outputDir = outDir
	e.scheduledAt = time.Now()

	e.op, err = e.mr.Vanilla(s, map[string]mapreduce.Job{"testtool": j})
	if err != nil {
		return err
	}

	if err := e.writePrepare(j, outDir); err != nil {
		return err
	}

	opErr := e.op.Wait()

	if err := e.downloadJobLog(ctx, s.StderrTablePath); err != nil {
		return err
	}

	return opErr
}

func (e *Exec) ReadOutputs(ctx context.Context) (*jobfs.ExitRow, error) {
	outR, err := e.yc.ReadTable(ctx, e.outputDir.Child(OutputTableName), nil)
	if err != nil {
		return nil, err
	}
	defer outR.Close()

	stdout, err := os.Create(e.config.FS.StdoutFile)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()

	stderr, err := os.Create(e.config.FS.StderrFile)
	if err != nil {
		return nil, err
	}
	defer stderr.Close()

	ident := func(s string) (string, bool) { return s, true }
	exitRow, err := jobfs.ReadOutputTable(outR, ident, stdout, stderr)
	if err != nil {
		return nil, fmt.Errorf("error reading results: %w", err)
	} else if exitRow == nil {
		return nil, fmt.Errorf("exit code is missing in output table")
	} else {
		if err := e.writeResult(exitRow); err != nil {
			return nil, err
		}

		return exitRow, nil
	}
}

const (
	OutputTableName   = "output"
	YTOutputTableName = "yt_output"
	StderrTableName   = "stderr"
)

func init() {
	mapreduce.RegisterJobPart(map[string]interface{}{})
	mapreduce.RegisterJobPart([]interface{}{})
}

func (e *Exec) PrepareJob(ctx context.Context) (j *job.Job, s *spec.Spec, outputDir ypath.Path, err error) {
	j = &job.Job{
		OperationConfig: e.config.Operation,
		Cmd:             e.config.Cmd,
		FS:              jobfs.New(),
		DmesgLogPath:    e.config.Exec.DmesgLog,
	}

	if err = j.FS.Add(e.config.FS); err != nil {
		return
	}

	if err = e.uploadFS(ctx, j.FS); err != nil {
		return
	}

	outputDir, err = e.createOutputDir(ctx)
	if err != nil {
		return
	}

	s = spec.Vanilla().AddVanillaTask("testtool", 1)
	s.MaxFailedJobCount = 1
	s.Pool = e.config.Operation.Pool
	s.TimeLimit = yson.Duration(e.config.Operation.Timeout + job.OperationTimeReserve)
	s.Title = e.config.Operation.Title

	s.StderrTablePath = outputDir.Child(StderrTableName)
	if _, err = mapreduce.CreateStderrTable(ctx, e.yc, s.StderrTablePath); err != nil {
		return
	}

	if e.config.Operation.SpecPatch != nil {
		specPatch, _ := yson.Marshal(e.config.Operation.SpecPatch)
		if err = yson.Unmarshal(specPatch, s); err != nil {
			err = fmt.Errorf("spec patch failed: %w", err)
			return
		}
	}

	us := s.Tasks["testtool"]
	j.FS.AttachFiles(us)

	us.TmpfsPath = "tmpfs"
	us.MemoryLimit = int64(e.config.Operation.MemoryLimit) + job.MemoryReserve
	us.MemoryReserveFactor = 1.0
	us.CPULimit = float32(e.config.Operation.CPULimit)
	us.LayerPaths = []ypath.Path{job.DefaultBaseLayer}
	us.OutputTablePaths = []ypath.YPath{
		outputDir.Child(OutputTableName),
		outputDir.Child(YTOutputTableName),
	}
	us.EnablePorto = "isolate"

	if e.config.Operation.TaskPatch != nil {
		taskPatch, _ := yson.Marshal(e.config.Operation.TaskPatch)
		if err = yson.Unmarshal(taskPatch, us); err != nil {
			err = fmt.Errorf("task patch failed: %w", err)
			return
		}
	}

	return
}
