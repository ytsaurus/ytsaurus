package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc/status"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/ctxlog"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/yterrors"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytprof/api"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
)

const DefaultQueryLimit = 1000000
const DefaultSystem = "yt"
const ManualSystem = "manual"
const UIRequestPrefix = "/ui"
const UIManualRequestPrefix = "/manual/ui"
const UIBaseHost = "https://ytprof.yt.yandex-team.ru"

var NonUISystems = map[string]struct{}{ManualSystem: {}}

type App struct {
	l          *zap.Logger
	httpListen net.Listener
	yt         yt.Client
	ts         map[string]*storage.TableStorage
	config     Config
	systems    []string
}

type Config struct {
	HTTPEndpoint string `json:"http_endpoint" yaml:"http_endpoint"`
	Proxy        string `json:"proxy" yaml:"proxy"`
	FolderPath   string `json:"folder_path" yaml:"folder_path"`
	QueryLimit   int    `json:"query_limit" yaml:"query_limit"`
}

func NewApp(l *zap.Logger, config Config) *App {
	app := &App{
		l:      l,
		config: config,
		ts:     map[string]*storage.TableStorage{},
	}

	if app.config.QueryLimit == 0 {
		app.config.QueryLimit = DefaultQueryLimit
	}

	var err error

	ytConfig := yt.Config{
		Proxy:             config.Proxy,
		ReadTokenFromFile: true,
	}

	app.yt, err = ythttp.NewClient(&ytConfig)
	if err != nil {
		l.Fatal("YT client creation failed", log.Error(err))
	}

	err = app.yt.ListNode(context.Background(), ypath.Path(config.FolderPath), &app.systems, nil)
	if err != nil {
		l.Fatal("listing systems failed", log.Error(err))
	}

	l.Debug("systems listed successfully", log.Array("systems", app.systems))

	for _, system := range app.systems {
		app.ts[system], err = storage.NewTableStorageMigrate(app.yt, ypath.Path(config.FolderPath).Child(system), l)
		if err != nil {
			l.Fatal("storage creation or migration failed", log.Error(err), log.String("system", system))
		}
	}

	app.httpListen, err = net.Listen("tcp", config.HTTPEndpoint)
	if err != nil {
		l.Fatal("HTTP listener creation failed", log.Error(err))
	}
	l.Info("HTTP listener started", log.String("addr", app.httpListen.Addr().String()))

	r := chi.NewMux()
	r.HandleFunc(fmt.Sprintf("%s/{profileID}/*", UIRequestPrefix),
		func(w http.ResponseWriter, r *http.Request) {
			app.UIHandler(w, r, "")
		},
	)

	for _, system := range app.systems {
		uiSystemPath := fmt.Sprintf("/%s%s/{profileID}/*", system, UIRequestPrefix)
		l.Debug("HTTP UI listener started",
			log.String("addr", uiSystemPath),
			log.String("system", system),
		)
		curSystem := system
		r.HandleFunc(uiSystemPath,
			func(w http.ResponseWriter, r *http.Request) {
				app.UIHandler(w, r, curSystem)
			},
		)
	}

	httpServer := &http.Server{
		Addr:    config.HTTPEndpoint,
		Handler: r,
	}

	err = Register(r, app)
	if err != nil {
		l.Fatal("HTTP routes registration failed", log.Error(err))
	}

	go func() {
		l.Error("HTTP server stopped", log.Error(httpServer.Serve(app.httpListen)))
	}()

	return app
}

func (a *App) URL() string {
	return fmt.Sprintf("http://%s", a.httpListen.Addr().String())
}

func (a *App) TableStorage(system string) (*storage.TableStorage, bool) {
	if system == "" {
		tsc, ok := a.ts[DefaultSystem]
		return tsc, ok
	}
	tsc, ok := a.ts[system]
	return tsc, ok
}

func (a *App) Logger() *zap.Logger {
	return a.l
}

func (a *App) GetSystems() []string {
	return a.systems
}

func (a *App) Stop() error {
	return a.httpListen.Close()
}

func Register(r chi.Router, client *App) error {
	mux := runtime.NewServeMux(runtime.WithProtoErrorHandler(client.grpcErrorHandler))
	runtime.SetHTTPBodyMarshaler(mux)
	r.Mount("/api", mux)
	return api.RegisterYTProfServiceHandlerClient(context.Background(), mux, client)
}

func (a *App) grpcErrorHandler(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	var statusCode = 400

	if st, ok := status.FromError(err); ok {
		statusCode = runtime.HTTPStatusFromCode(st.Code())
	}

	ctxlog.Error(ctx, a.l.Logger(), "API request failed", log.Error(err))

	hostname, _ := os.Hostname()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(yterrors.Err(
		"API request failed",
		yterrors.Attr("url", r.URL.Path),
		yterrors.Attr("origin", hostname),
		err,
	))
}

func GetProfileLink(prefix string, guid guid.GUID) string {
	return fmt.Sprintf("%s%s/%s/", UIBaseHost, prefix, guid)
}
