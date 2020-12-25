package ytexec

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
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
	"a.yandex-team.ru/yt/go/yterrors"
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

	errs := c.FS.Validate()
	if len(errs) != 0 {
		for _, err := range errs {
			l.Error("config error", log.Error(err))
			return nil, fmt.Errorf("config validation failed; see logs for more details")
		}
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
			ExpirationDelay:  3 * time.Hour,
		})
	}

	return e, nil
}

const (
	tmpOutputPath = ypath.Path("//tmp/ytexec_output")
	tmpUploadPath = ypath.Path("//tmp/ytexec_cache")
)

func (e *Exec) createOutputDir(ctx context.Context) (ypath.Path, error) {
	id := guid.New().String()
	path := e.config.Operation.OutputDir().Child(id[:2]).Child(id)
	ttl := e.config.Operation.OutputTTL
	opts := &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"expiration_time": yson.Time(time.Now().Add(ttl)),
			"fs_config":       e.config.FS,
		},
		Recursive: true,
	}

	_, err := e.yc.CreateNode(ctx, path, yt.NodeMap, opts)
	if yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError) && e.config.Operation.EnableResearchFallback {
		path = tmpOutputPath.Child(id[:2]).Child(id)
		_, err = e.yc.CreateNode(ctx, path, yt.NodeMap, opts)
	}

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

	doStartOperation := func() error {
		e.op, err = e.mr.Vanilla(s, map[string]mapreduce.Job{"testtool": j})

		if yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError) && e.config.Operation.EnableResearchFallback {
			s.Pool = ""
			e.op, err = e.mr.Vanilla(s, map[string]mapreduce.Job{"testtool": j})
		}

		return err
	}

	if err = e.retry(doStartOperation); err != nil {
		return err
	}

	if err := e.writePrepare(j, outDir); err != nil {
		return err
	}

	// Internal retries in Wait() should be good enough.
	opErr := e.op.Wait()

	doDownloadJobLog := func() error {
		return e.downloadJobLog(ctx, s.StderrTablePath)
	}

	if err = e.retry(doDownloadJobLog); err != nil {
		return err
	}

	return opErr
}

func (e *Exec) retry(op func() error) error {
	bf := backoff.NewExponentialBackOff()
	bf.MaxElapsedTime = time.Hour

	doOp := func() error {
		err := op()

		if yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError) {
			return backoff.Permanent(err)
		}

		return err
	}

	return backoff.RetryNotify(doOp, bf, func(err error, duration time.Duration) {
		e.l.Error("retrying error", log.Error(err), log.Duration("backoff", duration))
	})
}

func (e *Exec) ReadOutputs(ctx context.Context) (*jobfs.ExitRow, error) {
	var exitRow *jobfs.ExitRow
	doReadOutputs := func() error {
		outR, err := e.yc.ReadTable(ctx, e.outputDir.Child(OutputTableName), nil)
		if err != nil {
			return err
		}
		defer outR.Close()

		stdout, err := os.Create(e.config.FS.StdoutFile)
		if err != nil {
			return err
		}
		defer stdout.Close()

		stderr, err := os.Create(e.config.FS.StderrFile)
		if err != nil {
			return err
		}
		defer stderr.Close()

		ident := func(s string) (string, bool) { return s, true }
		exitRow, err = jobfs.ReadOutputTable(outR, ident, stdout, stderr)
		if err != nil {
			return fmt.Errorf("error reading results: %w", err)
		} else if exitRow == nil {
			return fmt.Errorf("exit code is missing in output table")
		} else {
			if err := e.writeResult(exitRow); err != nil {
				return err
			}

			return nil
		}
	}

	return exitRow, e.retry(doReadOutputs)
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

	doCreateOutputDir := func() error {
		var err error
		outputDir, err = e.createOutputDir(ctx)
		return err
	}
	if err = e.retry(doCreateOutputDir); err != nil {
		return
	}

	s = spec.Vanilla().AddVanillaTask("testtool", 1)

	// Retry errors caused by infrastructure problems. E.g portod restarts.
	s.MaxFailedJobCount = 3
	s.TimeLimit = yson.Duration(job.OperationTimeout)

	s.Pool = e.config.Operation.Pool
	s.Title = e.config.Operation.Title

	s.StderrTablePath = outputDir.Child(StderrTableName)
	doCreateStderrTable := func() error {
		_, err = mapreduce.CreateStderrTable(ctx, e.yc, s.StderrTablePath, yt.WithForce())
		return err
	}

	if err = e.retry(doCreateStderrTable); err != nil {
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
	us.TmpfsSize = int64(e.config.Operation.TmpfsSize) + j.FS.TotalSize + job.TmpfsReserve
	us.MemoryLimit = int64(e.config.Operation.MemoryLimit) + job.MemoryReserve + us.TmpfsSize

	us.MemoryReserveFactor = 1.0
	us.CPULimit = float32(e.config.Operation.CPULimit)
	us.LayerPaths = []ypath.Path{job.DefaultBaseLayer}
	us.OutputTablePaths = []ypath.YPath{
		outputDir.Child(OutputTableName),
		outputDir.Child(YTOutputTableName),
	}
	us.MakeRootFSWritable = true
	us.EnablePorto = "isolate"

	if taskPatch := e.config.Operation.TaskPatch; taskPatch != nil {
		// TODO(prime@): remove this once we are no longer using json config
		if memory, ok := taskPatch["memory_limit"]; ok {
			switch v := memory.(type) {
			case float64:
				taskPatch["memory_limit"] = int64(v)
			}
		}

		ys, _ := yson.Marshal(taskPatch)
		if err = yson.Unmarshal(ys, us); err != nil {
			err = fmt.Errorf("task patch failed: %w", err)
			return
		}
	}

	return
}
