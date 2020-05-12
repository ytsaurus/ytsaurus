package ytrecipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"a.yandex-team.ru/library/go/core/log"
	zaplog "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/yt/go/blobtable"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

func init() {
	mapreduce.Register(&Job{})
}

type Job struct {
	mapreduce.Untyped

	Config      *Config
	FS          *FS
	Env         *Env
	JobDeadline *time.Time

	L log.Structured
}

func (j *Job) OutputTypes() []interface{} {
	return []interface{}{
		&OutputRow{},
		&OutputRow{},
	}
}

const (
	envPortoSpawned = "YTRECIPE_PORTO_SPAWNED"
	baseLayer       = ypath.Path("//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz")
)

type stdwriter struct {
	mu     *sync.Mutex
	w      mapreduce.Writer
	stdout bool
}

const maxRowSize = 4 * 1024 * 1024

func (s *stdwriter) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var row OutputRow

	for n < len(p) {
		end := len(p)
		if end-n > maxRowSize {
			end = n + maxRowSize
		}

		if s.stdout {
			row.Stdout = p[n:end]
		} else {
			row.Stderr = p[n:end]
		}

		if err = s.w.Write(row); err != nil {
			return
		}

		n = end
	}

	return
}

var jobLogConfig = zap.Config{
	Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
	Encoding:         "console",
	EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
	OutputPaths:      []string{"stderr"},
	ErrorOutputPaths: []string{"stderr"},
}

func (j *Job) initLog() error {
	l, err := zaplog.New(jobLogConfig)
	if err != nil {
		return err
	}

	j.L = l
	return nil
}

func (j *Job) scheduleSignals() chan syscall.Signal {
	quit := make(chan syscall.Signal, 1)
	go func() {
		if j.JobDeadline == nil {
			return
		}

		time.Sleep(time.Until(*j.JobDeadline))
		j.L.Warn("job deadline reached", log.Time("deadline", *j.JobDeadline))
		quit <- syscall.SIGUSR2
		time.Sleep(time.Minute)

		j.L.Warn("smooth shutdown deadline exceeded")
		quit <- syscall.SIGQUIT

		j.L.Warn("hard shutdown deadline exceeded")
		time.Sleep(time.Second)
		quit <- syscall.SIGKILL
	}()

	return quit
}

func (j *Job) runJob(out mapreduce.Writer) error {
	stdout := &stdwriter{w: out, mu: new(sync.Mutex), stdout: true}
	stderr := &stdwriter{w: out, mu: stdout.mu, stdout: false}

	if err := j.FS.Recreate("fs"); err != nil {
		return err
	}
	j.L.Info("finished creating FS")

	err := j.spawnPorto(j.scheduleSignals(), stdout, stderr)

	var ctErr *ContainerExitError
	if errors.As(err, &ctErr) {
		if ctErr.IsOOM() {
			j.L.Warn("test was killed by OOM", log.Int("limit", j.Config.ResourceLimits.MemoryLimit))
			_, err = fmt.Fprintf(stderr, "\n\n\nTest killed by OOM (limit %d)\n", j.Config.ResourceLimits.MemoryLimit)
			if err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}

	j.L.Info("test process finished")
	if err := j.FS.EmitResults(out); err != nil {
		return err
	}

	j.L.Info("finished writing results")
	if ctErr != nil {
		return out.Write(OutputRow{
			ExitCode:       ptr.Int(ctErr.ExitCode),
			KilledBySignal: ctErr.KilledBySignal,
		})
	} else {
		return out.Write(OutputRow{ExitCode: ptr.Int(0)})
	}
}

func (j *Job) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	if err := j.initLog(); err != nil {
		return err
	}

	if err := j.runJob(out[0]); err != nil {
		j.L.Error("internal job error", log.Error(err))
		os.Exit(1)
	}

	return nil
}

func (r *Runner) CreateOutputDir(ctx context.Context) (ypath.Path, error) {
	path := r.Config.OutputPath.Child(guid.New().String())
	ttl := time.Duration(r.Config.OutputTTLHours) * time.Hour

	_, err := r.YT.CreateNode(ctx, path, yt.NodeMap, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{"expiration_time": yson.Time(time.Now().Add(ttl))},
	})

	return path, err
}

func attachFiles(us *spec.UserScript, fs *FS) {
	attachFile := func(h MD5, f *File) {
		us.FilePaths = append(us.FilePaths, spec.File{
			FileName:    filepath.Join("fs", h.String()),
			Executable:  f.Executable,
			CypressPath: f.CypressPath,
		})
	}

	for h, f := range fs.Files {
		attachFile(h, f)
	}
}

func downloadJobLog(ctx context.Context, yc yt.Client, stderrTable ypath.Path) error {
	jobLog, err := os.Create(yatest.OutputPath("job.log"))
	if err != nil {
		return err
	}

	r, err := blobtable.ReadBlobTable(ctx, yc, stderrTable)
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

type Runner struct {
	Config *Config
	YT     yt.Client
	L      log.Structured
}

func (r *Runner) RunJob() error {
	env, err := CaptureEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()

	job, s, outDir, err := r.PrepareJob(ctx, env)
	if err != nil {
		return err
	}

	mr := mapreduce.New(r.YT)
	op, err := mr.Vanilla(s, map[string]mapreduce.Job{"testtool": job})
	if err != nil {
		return err
	}

	if err := r.writeReadme(op.ID(), job, outDir); err != nil {
		return err
	}

	opErr := op.Wait()
	if err := downloadJobLog(ctx, r.YT, s.StderrTablePath); err != nil {
		return err
	}

	if opErr != nil {
		return opErr
	}

	outR, err := r.YT.ReadTable(ctx, outDir.Child(OutputTableName), nil)
	if err != nil {
		return err
	}
	defer outR.Close()

	exitRow, err := ReadOutputTable(outR, func(s string) string { return s }, os.Stdout, os.Stderr)
	if err != nil {
		return fmt.Errorf("error reading results: %w", err)
	}

	if exitRow != nil {
		if exitRow.KilledBySignal != 0 {
			// If job was killed by internal timeout, wait for corresponding signal from out parent.
			// Otherwise process will exit too early and CRASH/TIMEOUT statuses get confused.

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, exitRow.KilledBySignal)
			<-ch
		}

		os.Exit(*exitRow.ExitCode)
	}

	return fmt.Errorf("exit code is missing in output table")
}

const (
	YTRecipeOutput    = "ytrecipe_output"
	YTRecipeHDD       = "ytrecipe_hdd"
	YTRecipeCoreDumps = "ytrecipe_coredumps"

	OutputTableName = "output"
	stderrTableName = "stderr"
)

func (r *Runner) PrepareJob(ctx context.Context, env *Env) (job *Job, s *spec.Spec, outputDir ypath.Path, err error) {
	job = &Job{
		Config: r.Config,
		Env:    env,
		FS:     NewFS(),
	}

	if r.Config.JobTimeoutSeconds != 0 {
		deadline := time.Now().Add(time.Second * time.Duration(r.Config.JobTimeoutSeconds))
		job.JobDeadline = &deadline
	}

	job.FS.YTOutput = yatest.WorkPath(YTRecipeOutput)
	job.FS.YTHDD = yatest.WorkPath(YTRecipeHDD)
	job.FS.YTCoreDumps = yatest.WorkPath(YTRecipeCoreDumps)

	job.FS.Outputs[env.TraceFile] = struct{}{}
	job.FS.Outputs[env.OutputDir] = struct{}{}

	if err = job.FS.AddDir(env.BuildRoot); err != nil {
		return
	}

	job.FS.Dirs[env.TmpDir] = struct{}{}

	if err = job.FS.AddFile(env.TestTool); err != nil {
		return
	}

	if env.GDBPath != "" {
		if err = job.FS.AddFile(env.GDBPath); err != nil {
			return
		}
	}

	for _, path := range r.Config.UploadBinaries {
		if err = job.FS.AddFile(yatest.BuildPath(path)); err != nil {
			return
		}
	}

	for _, path := range r.Config.UploadWorkfile {
		delete(job.FS.Symlinks, filepath.Dir(yatest.WorkPath(path)))
		if err = job.FS.AddFile(yatest.WorkPath(path)); err != nil {
			return
		}
	}

	if err = r.uploadFS(ctx, job.FS, job.Env); err != nil {
		return
	}

	outputDir, err = r.CreateOutputDir(ctx)
	if err != nil {
		return
	}

	s = spec.Vanilla().
		AddVanillaTask("testtool", 1)
	s.MaxFailedJobCount = 1
	s.Pool = r.Config.Pool

	fileTitle := ""
	if env.TestFileFilter != "" {
		fileTitle = " [" + env.TestFileFilter + "]"
	}
	moduloTitle := ""
	if env.ModuloIndex != "" {
		moduloTitle = fmt.Sprintf(" [%s/%s]", env.ModuloIndex, env.Modulo)
	}
	s.Title = fmt.Sprintf("[TS] %s%s%s", env.ProjectPath, fileTitle, moduloTitle)

	hostname, _ := os.Hostname()
	s.StartedBy = map[string]interface{}{
		"command":  job.Env.Args,
		"hostname": hostname,
	}

	s.TimeLimit = yson.Duration(time.Duration(r.Config.JobTimeoutSeconds)*time.Second + operationTimeReserve)

	s.StderrTablePath = outputDir.Child(stderrTableName)
	if _, err = mapreduce.CreateStderrTable(ctx, r.YT, s.StderrTablePath); err != nil {
		return
	}

	us := s.Tasks["testtool"]
	attachFiles(us, job.FS)

	us.TmpfsPath = "tmpfs"
	us.MemoryLimit = int64(r.Config.ResourceLimits.MemoryLimit) + jobMemoryReserve
	us.MemoryReserveFactor = 1.0
	us.CPULimit = float32(r.Config.ResourceLimits.CPULimit)
	us.LayerPaths = []ypath.YPath{baseLayer}
	us.OutputTablePaths = []ypath.YPath{
		outputDir.Child(OutputTableName),
		outputDir.Child(YTRecipeOutput),
	}
	us.EnablePorto = "isolate"

	return
}
