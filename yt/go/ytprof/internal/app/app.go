package app

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytprof/api"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"

	"encoding/json"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc/status"

	"a.yandex-team.ru/library/go/core/log/ctxlog"
	"a.yandex-team.ru/yt/go/yterrors"
)

const DefaultQueryLimit = 1000000

type App struct {
	l          *zap.Logger
	httpListen net.Listener
	yt         yt.Client
	ts         *storage.TableStorage
	config     Config
}

type Config struct {
	HTTPEndpoint string `json:"http_endpoint" yaml:"http_endpoint"`
	Proxy        string `json:"proxy" yaml:"proxy"`
	TablePath    string `json:"table_path" yaml:"table_path"`
	QueryLimit   int    `json:"query_limit" yaml:"query_limit"`
}

func NewApp(l *zap.Logger, config Config) *App {
	ytConfig := yt.Config{
		Proxy:             config.Proxy,
		ReadTokenFromFile: true,
	}

	app := &App{
		l:      l,
		config: config,
	}

	if app.config.QueryLimit == 0 {
		app.config.QueryLimit = DefaultQueryLimit
	}

	var err error

	app.yt, err = ythttp.NewClient(&ytConfig)
	if err != nil {
		l.Fatal("creating YT client falied", log.Error(err))
	}

	app.ts, err = storage.NewTableStorageMigrate(app.yt, ypath.Path(config.TablePath), l)
	if err != nil {
		l.Fatal("creating storage or migrating failed", log.Error(err))
	}

	app.httpListen, err = net.Listen("tcp", config.HTTPEndpoint)
	if err != nil {
		l.Fatal("creating HTTP listener failed", log.Error(err))
	}
	l.Info("started HTTP listener", log.String("addr", app.httpListen.Addr().String()))

	r := chi.NewMux()

	httpServer := &http.Server{
		Addr:    config.HTTPEndpoint,
		Handler: r,
	}

	err = Register(r, app)
	if err != nil {
		l.Fatal("registering HTTP routes failed", log.Error(err))
	}

	go func() {
		l.Error("HTTP server has stopped", log.Error(httpServer.Serve(app.httpListen)))
	}()

	return app
}

func (a *App) URL() string {
	return fmt.Sprintf("http://%s", a.httpListen.Addr().String())
}

func (a *App) TableStorage() *storage.TableStorage {
	return a.ts
}

func (a *App) Logger() *zap.Logger {
	return a.l
}

func (a *App) Stop() error {
	return a.httpListen.Close()
}

func Register(r chi.Router, client *App) error {
	mux := runtime.NewServeMux(runtime.WithProtoErrorHandler(client.grpcErrorHandler))
	runtime.SetHTTPBodyMarshaler(mux)
	r.Mount("/ytprof/api", mux)
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
	))
}
