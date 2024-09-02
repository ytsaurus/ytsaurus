// Фреймворк для построения тимбертрак приложений.
// Подразумевается следующее использование.

// Во первых, заводится структура конфига:
//
//   - первое поле должно быть заинлайненным `app.Config`
//
//   - дальше обычно идёт список пайплайнов для обработки
//
//     type MyConfig struct {
//     app.Config `yaml:",inline"`
//
//     YtToken string `yaml:"yt_token"`
//     }
package app

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"reflect"
	"runtime"
	"runtime/pprof"
	"slices"
	"syscall"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"

	"go.ytsaurus.tech/yt/admin/timbertruck/internal/misc"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
)

type Config struct {
	timbertruck.Config `yaml:",inline"`

	// File for logs of timbertruck itself
	LogFile string `yaml:"log_file"`

	// File to write pid info. Locked while timbertruck is running, so second instance cannot be launched.
	PidFile string `yaml:"pid_file"`

	// Hostname, determined automatically if not set.
	// Used to generate sessionId, so it must be unique inside cluster.
	Hostname string `yaml:"hostname"`
}

type App interface {
	AddPipeline(config timbertruck.StreamConfig, newFunc timbertruck.NewPipelineFunc)

	Logger() *slog.Logger
	Fatalf(format string, a ...any)

	Run() error
	Close() error
}

var argv0Stripped = path.Base(os.Args[0])
var argv0Prefix = argv0Stripped + ":"

func usageError(format string, a ...any) error {
	format = "%v: " + format
	return fmt.Errorf(format, argv0Stripped, a)
}

func MustNewApp[UserConfigType any]() (app App, userConfig *UserConfigType) {
	app, userConfig, err := NewApp[UserConfigType]()
	if err != nil {
		log.Fatalln(argv0Prefix, err)
	}
	return
}

func NewApp[UserConfigType any]() (app App, userConfig *UserConfigType, err error) {
	var configPath string
	var oneShotConfigPath string
	flag.StringVar(&configPath, "config", "", "path to configuration, timbertruck will be launched as daemon")
	flag.StringVar(&oneShotConfigPath, "one-shot-config", "", "path to task configuration, timbertruck executes tasks and exits")
	flag.Parse()

	if configPath != "" && oneShotConfigPath != "" {
		err = usageError("-config and -task-config are mutually exclusive")
		return
	}

	readConfiguration := func(configPath string, config *UserConfigType) (err error) {
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			err = usageError("cannot read configuration: %w", err)
			return
		}

		err = yaml.Unmarshal(configBytes, config)
		if err != nil {
			err = usageError("cannot parse configuration: %w", err)
			return
		}
		return
	}

	userConfig = new(UserConfigType)
	if configPath != "" {
		err = readConfiguration(configPath, userConfig)
		if err != nil {
			return
		}
		var appConfig *Config
		appConfig, err = resolveAppConfig(userConfig)
		if err != nil {
			return
		}
		app, err = newDaemonApp(*appConfig)
	} else if oneShotConfigPath != "" {
		err = readConfiguration(oneShotConfigPath, userConfig)
		if err != nil {
			return
		}
		app, err = newOneShotApp()
	} else {
		err = usageError("-config or -task-config must be specified")
		return
	}

	return
}

type daemonApp struct {
	config Config

	logger *slog.Logger

	ctx        context.Context
	cancelFunc context.CancelFunc

	closePid func()

	timberTruck *timbertruck.TimberTruck
}

func newDaemonApp(config Config) (app *daemonApp, err error) {
	app = &daemonApp{
		config: config,
	}

	defer func() {
		if err != nil {
			_ = app.Close()
		}
	}()

	err = os.MkdirAll(config.WorkDir, 0755)
	if err != nil {
		err = fmt.Errorf("cannot create work directory: %v", err)
		return
	}

	var pidFile string
	if config.PidFile != "" {
		pidFile = config.PidFile
	} else {
		pidFile = path.Join(config.WorkDir, "timbertruck.pid")
	}
	app.closePid, err = createPidFile(pidFile)
	if err != nil {
		err = fmt.Errorf("cannot write pid file: %w", err)
		return
	}

	var logFile io.Writer = os.Stderr
	if config.LogFile != "" {
		logFile, err = misc.NewLogrotatingFile(config.LogFile)
		if err != nil {
			err = fmt.Errorf("cannot open log file: %v", err)
			return
		}
	}
	app.logger = slog.New(slog.NewJSONHandler(logFile, nil))

	app.ctx, app.cancelFunc = context.WithCancel(context.Background())
	cancelOnSignals(app.cancelFunc)

	app.timberTruck, err = timbertruck.NewTimberTruck(config.Config, app.logger)
	if err != nil {
		return
	}

	return
}

func (app *daemonApp) Close() error {
	if app.cancelFunc != nil {
		app.cancelFunc()
		app.cancelFunc = nil
	}
	if app.closePid != nil {
		app.closePid()
		app.closePid = nil
	}
	return nil
}

func (app *daemonApp) AddPipeline(config timbertruck.StreamConfig, newFunc timbertruck.NewPipelineFunc) {
	app.timberTruck.AddStream(config, newFunc)
}

func (app *daemonApp) Run() error {
	stopF := startProfiling(app.logger)
	defer stopF()

	return app.timberTruck.Serve(app.ctx)
}

func (app *daemonApp) Logger() *slog.Logger {
	return app.logger
}

func (app *daemonApp) Fatalf(format string, a ...any) {
	fatalFImpl(app.logger, format, a...)
}

func createPidFile(path string) (close func(), err error) {
	pidF, err := os.Create(path)
	defer func() {
		if err != nil {
			_ = pidF.Close()
		}
	}()

	if err != nil {
		err = fmt.Errorf("cannot create pid file: %v", err)
		return
	}

	err = unix.Flock(int(pidF.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		err = fmt.Errorf("cannot lock pid file: %v", err)
		return
	}

	_, err = fmt.Fprintln(pidF, os.Getpid())
	if err != nil {
		err = fmt.Errorf("cannot write pid file: %v", err)
		return
	}

	close = func() {
		_ = pidF.Close()
	}
	return
}

func cancelOnSignals(cancel func()) {
	ch := make(chan os.Signal, 16)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for range ch {
			cancel()
		}
	}()
}

func resolveAppConfig(userConfig any) (appConfig *Config, err error) {
	typeOfUserConfigPtr := reflect.TypeOf(userConfig)
	if typeOfUserConfigPtr.Kind() != reflect.Pointer || typeOfUserConfigPtr.Elem().Kind() != reflect.Struct {
		err = fmt.Errorf("user config must be pointer to struct")
		return
	}
	typeOfUserConfig := typeOfUserConfigPtr.Elem()
	typeOfConfig := reflect.TypeOf(Config{})

	if typeOfUserConfig == typeOfConfig {
		var ok bool
		appConfig, ok = userConfig.(*Config)
		if !ok {
			panic(fmt.Sprintf("internal error, expected appConfig to be *Config, actually it is %v", typeOfUserConfigPtr))
		}
		return
	}

	fields := reflect.VisibleFields(typeOfUserConfig)
	valueOfUserConfig := reflect.ValueOf(userConfig).Elem()

	for _, f := range fields {
		if f.Type == typeOfConfig {
			appConfigAddr := valueOfUserConfig.FieldByIndex(f.Index).Addr()
			var ok bool
			appConfig, ok = appConfigAddr.Interface().(*Config)
			if !ok {
				panic(fmt.Sprintf("internal error, expected appConfig to be *Config, actually it is %v", appConfigAddr.Type()))
			}
			return
		}
	}

	err = fmt.Errorf("bad user config type %v: app.Config type is not found in the promoted fields", typeOfUserConfig)
	return
}

type oneShotAppTask struct {
	config  timbertruck.StreamConfig
	newFunc timbertruck.NewPipelineFunc
}

type oneShotAppTaskController struct {
	logger slog.Logger
}

func (c *oneShotAppTaskController) Logger() *slog.Logger {
	return &c.logger
}

func (c *oneShotAppTaskController) NotifyProgress(_ pipelines.FilePosition) {
}

type oneShotApp struct {
	tasks []oneShotAppTask

	ctx       context.Context
	ctxCancel context.CancelFunc

	logger *slog.Logger
}

func newOneShotApp() (a App, err error) {
	app := &oneShotApp{}
	app.ctx, app.ctxCancel = context.WithCancel(context.Background())
	cancelOnSignals(app.ctxCancel)
	app.logger = slog.Default()

	a = app
	return
}

func (app *oneShotApp) AddPipeline(config timbertruck.StreamConfig, newFunc timbertruck.NewPipelineFunc) {
	app.tasks = append(app.tasks, oneShotAppTask{config, newFunc})
}

func (app *oneShotApp) Logger() *slog.Logger {
	return app.logger
}

func (app *oneShotApp) Fatalf(format string, a ...any) {
	fatalFImpl(app.logger, format, a...)
}

func (app *oneShotApp) Run() error {
	stopF := startProfiling(app.logger)
	defer stopF()

	for i := range app.tasks {
		if app.ctx.Err() != nil {
			break
		}
		taskArgs := timbertruck.TaskArgs{
			Context:    app.ctx,
			Path:       app.tasks[i].config.LogFile,
			Controller: &oneShotAppTaskController{*app.logger},
		}
		pipeline, err := app.tasks[i].newFunc(taskArgs)
		if err != nil {
			return err
		}
		err = pipeline.Run(app.ctx)
		if err != nil {
			return err
		}
	}
	return app.ctx.Err()
}

func (app *oneShotApp) Close() error {
	if app.ctxCancel != nil {
		app.ctxCancel()
		app.ctxCancel = nil
	}
	return nil
}

func fatalFImpl(logger *slog.Logger, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	logger.Error(msg)
	log.Fatalln(argv0Prefix, msg)
}

func startProfiling(logger *slog.Logger) func() {
	memoryProfFile := os.Getenv("MEMORY_PROF")
	var cleanupFuncList []func()
	if memoryProfFile != "" {
		logger.Info("memory profiling enabled (activated with SIGUSR1)")
		ch := make(chan os.Signal, 16)
		signal.Notify(ch, syscall.SIGUSR1)

		go func() {
			for range ch {
				func() {
					f, err := os.Create(memoryProfFile)
					if err != nil {
						fatalFImpl(logger, "could not create memory profile: %v", err)
					}
					defer f.Close()
					runtime.GC()
					err = pprof.WriteHeapProfile(f)
					if err != nil {
						fatalFImpl(logger, "could not write memory profile: %v", err)
					}
				}()
			}
		}()
	}
	cpuProfFile := os.Getenv("CPU_PROF")
	if cpuProfFile != "" {
		logger.Info("CPU profiling enabled, writing profile to", "file", cpuProfFile)
		f, err := os.Create(cpuProfFile)
		if err != nil {
			fatalFImpl(logger, "could not create CPU profile: %v", err)
		}
		cleanupFuncList = append(cleanupFuncList, func() {
			err := f.Close()
			if err != nil {
				fatalFImpl(logger, "failed to close CPU profile: %v", err)
			}
		})

		if err := pprof.StartCPUProfile(f); err != nil {
			fatalFImpl(logger, "could not start CPU profile: %v", err)
		}
		cleanupFuncList = append(cleanupFuncList, func() {
			pprof.StopCPUProfile()
		})

	}
	slices.Reverse(cleanupFuncList)
	return func() {
		for _, f := range cleanupFuncList {
			f()
		}
	}
}
