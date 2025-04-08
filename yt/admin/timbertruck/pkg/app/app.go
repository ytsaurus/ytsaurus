// Framework is used for building timbertruck applications.
// The following usage is suggested:
//
// First, user of framework creates configuration struct for application and configuration struct for stream
// (or multiple configuration structs for different streams).
//
// Configuration of the application has following properties:
//   - the first field must be inlined field of type app.Config
//   - other fields describe configuration of the streams
//   - config might have other custom fields
//   - config is annotated with yaml tags
//
// Configuration of the particular stream has following properties:
//   - the first field must be inlined field of type timbertruck.StreamConfig
//   - config might have other custom fields
//   - config is annotated with yaml tags
//
// Example:
//
//		type MyStreamConfig struct {
//		     timbertruck.StreamConfig `yaml:",inline"`
//	         YtQueuePath string `yaml:"some_output_configuration"`
//		}
//		type MyConfig struct {
//		    app.Config `yaml:",inline"`
//		    MyStreams []MyStreamConfig `yaml:"my_streams"`
//		    YtToken   string `yaml:"yt_token"`
//		}
//
// Second, user of framework creates main function with following structure:
//
//	func main() {
//		// Create application instance, parse cmd line and config
//		app, config := app.MustNewApp[MyConfig]()
//		defer app.Close()
//
//		// Maybe do some stuff initialization
//		// ...
//		// Somehow register all streams
//		for i := range config.MyStreams {
//			app.AddPipeline(config.MyStreams[i].StreamConfig, func (task timbertruck.TaskArgs) (*pipelines.Pipeline, error) {
//				// custom pipeline handling
//			})
//		}
//		// Launch application and start to handle streams
//		err = app.Run()
//		if err != nil {
//			log.Fatalln(err)
//		}
//	}
package app

import (
	"context"
	"errors"
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

	"go.ytsaurus.tech/library/go/core/buildinfo"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/metrics/nop"
	"go.ytsaurus.tech/library/go/core/metrics/solomon"
	"go.ytsaurus.tech/yt/admin/timbertruck/internal/misc"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
)

type Config struct {
	timbertruck.Config `yaml:",inline"`

	// File for logs of timbertruck itself
	LogFile string `yaml:"log_file"`

	// File for error logs of timbertruck itself
	ErrorLogFile string `yaml:"error_log_file"`

	// File to write pid info. Locked while timbertruck is running, so second instance cannot be launched.
	PidFile string `yaml:"pid_file"`

	// Hostname, determined automatically if not set.
	// Used to generate sessionId, so it must be unique inside cluster.
	Hostname string `yaml:"hostname"`

	AdminPanel *AdminPanelConfig `yaml:"admin_panel"`
}

type App interface {
	AddStream(config timbertruck.StreamConfig, newFunc timbertruck.NewPipelineFunc)

	Logger() *slog.Logger
	Fatalf(format string, a ...any)

	Metrics() metrics.Registry

	Run() error
	Close() error
}

var argv0Prefix = path.Base(os.Args[0]) + ":"

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
	var version bool
	var isRestart bool
	flag.BoolVar(&version, "version", false, "print version and exit")
	flag.StringVar(&configPath, "config", "", "path to configuration, timbertruck will be launched as daemon")
	flag.StringVar(&oneShotConfigPath, "one-shot-config", "", "path to task configuration, timbertruck executes tasks and exits")
	flag.BoolVar(&isRestart, "is-restart", false, "indicates that the timbertruck daemon process has been restarted")
	flag.Parse()

	if version {
		fmt.Println(buildinfo.Info.ProgramVersion)
		os.Exit(0)
	}

	if configPath != "" && oneShotConfigPath != "" {
		err = errors.New("-config and -task-config are mutually exclusive")
		return
	}

	readConfiguration := func(configPath string, config *UserConfigType) (err error) {
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			err = fmt.Errorf("cannot read configuration: %w", err)
			return
		}

		err = yaml.Unmarshal(configBytes, config)
		if err != nil {
			err = fmt.Errorf("cannot parse configuration: %w", err)
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
		if appConfig.Hostname == "" {
			appConfig.Hostname, err = os.Hostname()
			if err != nil {
				err = fmt.Errorf("cannot resolve hostname: %w", err)
				return
			}
		}
		app, err = newDaemonApp(*appConfig, isRestart)
	} else if oneShotConfigPath != "" {
		err = readConfiguration(oneShotConfigPath, userConfig)
		if err != nil {
			return
		}
		app, err = newOneShotApp()
	} else {
		err = errors.New("-config or -task-config must be specified")
		return
	}

	return
}

//
// DAEMON APP
//

type daemonApp struct {
	config Config

	logger *slog.Logger

	metrics *solomon.Registry

	ctx        context.Context
	cancelFunc context.CancelFunc

	closePid      func()
	closeLogFiles func()

	timberTruck *timbertruck.TimberTruck
	adminPanel  *adminPanel
}

func newDaemonApp(config Config, isRestart bool) (app *daemonApp, err error) {
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

	app.metrics, err = createMetricsRegistry(config.AdminPanel)
	if err != nil {
		err = fmt.Errorf("failed to create metrics registry: %w", err)
		return
	}

	var logFile io.WriteCloser = os.Stderr
	closeLogFile := func() {}
	if config.LogFile != "" {
		logFile, err = misc.NewLogrotatingFile(config.LogFile)
		if err != nil {
			err = fmt.Errorf("cannot open log file: %v", err)
			return
		}
		closeLogFile = func() { _ = logFile.Close() }
	}

	var errorLogFile io.Writer = io.Discard
	closeErrorLogFile := func() {}
	if config.ErrorLogFile != "" {
		var logrotatingFile io.WriteCloser
		logrotatingFile, err = misc.NewLogrotatingFile(config.ErrorLogFile)
		if err != nil {
			err = fmt.Errorf("cannot open error log file: %v", err)
			return nil, err
		}
		errorLogFile = logrotatingFile
		closeErrorLogFile = func() { _ = logrotatingFile.Close() }
	}
	app.closeLogFiles = func() {
		closeLogFile()
		closeErrorLogFile()
	}

	errorCounter := app.metrics.Counter("tt.application.error_log_count")
	errorCounter.Add(0)
	app.logger = slog.New(
		newLogErrorTracker(
			slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: slog.LevelDebug}),
			slog.NewJSONHandler(errorLogFile, &slog.HandlerOptions{Level: slog.LevelInfo}),
			errorCounter,
		),
	)
	misc.SetLogrotatingLogger(app.logger)
	misc.LogLoggingStarted(app.logger)

	restartCounter := app.metrics.Counter("tt.application.restart_count")
	restartCounter.Add(0)
	if isRestart {
		restartCounter.Inc()
	}

	app.ctx, app.cancelFunc = context.WithCancel(context.Background())
	cancelOnSignals(app.cancelFunc)

	app.timberTruck, err = timbertruck.NewTimberTruck(config.Config, app.logger, app.metrics)
	if err != nil {
		return
	}

	if config.AdminPanel != nil {
		app.adminPanel, err = newAdminPanel(app.logger, app.metrics, *config.AdminPanel)
		if err != nil {
			err = fmt.Errorf("cannot create admin panel: %w", err)
			return
		}
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
	if app.closeLogFiles != nil {
		app.closeLogFiles()
		app.closeLogFiles = nil
	}
	return nil
}

func (app *daemonApp) AddStream(config timbertruck.StreamConfig, newFunc timbertruck.NewPipelineFunc) {
	app.timberTruck.AddStream(config, newFunc)
}

func (app *daemonApp) Run() error {
	defer app.Close()
	stopF := startProfiling(app.logger)
	defer stopF()

	if app.adminPanel != nil {
		go func() {
			_ = app.adminPanel.Run(app.ctx)
		}()
	}

	return app.timberTruck.Serve(app.ctx)
}

func (app *daemonApp) Logger() *slog.Logger {
	return app.logger
}

func (app *daemonApp) Fatalf(format string, a ...any) {
	fatalFImpl(app.logger, format, a...)
}

func (app *daemonApp) Metrics() metrics.Registry {
	return app.metrics
}

func createMetricsRegistry(config *AdminPanelConfig) (*solomon.Registry, error) {
	if config == nil {
		return solomon.NewRegistry(nil), nil
	}
	solomonRegistryOptions := solomon.NewRegistryOpts()

	if config.MonitoringTags != nil {
		solomonRegistryOptions.SetTags(config.MonitoringTags)
	}

	metricsFormat := config.MetricsFormat
	switch config.MetricsFormat {
	case "":
		metricsFormat = string(solomon.StreamSpack)
	case string(solomon.StreamJSON), string(solomon.StreamSpack):
	default:
		return nil, fmt.Errorf("unsupported metrics format: %s", metricsFormat)
	}
	solomonRegistryOptions.SetStreamFormat(solomon.StreamFormat(metricsFormat))

	return solomon.NewRegistry(solomonRegistryOptions), nil
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

//
// ONE SHOT APP
//

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

func (app *oneShotApp) AddStream(config timbertruck.StreamConfig, newFunc timbertruck.NewPipelineFunc) {
	app.tasks = append(app.tasks, oneShotAppTask{config, newFunc})
}

func (app *oneShotApp) Logger() *slog.Logger {
	return app.logger
}

func (app *oneShotApp) Fatalf(format string, a ...any) {
	fatalFImpl(app.logger, format, a...)
}

func (app *oneShotApp) Metrics() metrics.Registry {
	return nop.Registry{}
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

//
// LOG ERROR TRACKER
//

type logErrorTrackingHandler struct {
	underlying   slog.Handler
	errorHandler slog.Handler
	counter      metrics.Counter
}

func newLogErrorTracker(underlying, errorHandler slog.Handler, counter metrics.Counter) slog.Handler {
	return logErrorTrackingHandler{underlying: underlying, errorHandler: errorHandler, counter: counter}
}

func (t logErrorTrackingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return t.underlying.Enabled(ctx, level)
}

func (t logErrorTrackingHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level >= slog.LevelWarn || ctx.Value(&misc.LoggingStartedKey) != nil {
		if err := t.errorHandler.Handle(ctx, record); err != nil {
			return err
		}
	}
	if record.Level >= slog.LevelError {
		t.counter.Inc()
	}
	return t.underlying.Handle(ctx, record)
}

func (t logErrorTrackingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return logErrorTrackingHandler{
		underlying:   t.underlying.WithAttrs(attrs),
		errorHandler: t.errorHandler.WithAttrs(attrs),
		counter:      t.counter,
	}
}

func (t logErrorTrackingHandler) WithGroup(name string) slog.Handler {
	return logErrorTrackingHandler{
		underlying:   t.underlying.WithGroup(name),
		errorHandler: t.errorHandler.WithGroup(name),
		counter:      t.counter,
	}
}
