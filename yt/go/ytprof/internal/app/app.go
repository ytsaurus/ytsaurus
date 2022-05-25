package app

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytprof/api"

	"encoding/json"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc/status"

	"a.yandex-team.ru/library/go/core/log/ctxlog"
	"a.yandex-team.ru/yt/go/yterrors"
)

type App struct {
	l          *zap.Logger
	httpListen net.Listener
}

type Config struct {
	HTTPEndpoint string
	Proxy        string
}

func NewApp(l *zap.Logger, config Config) *App {
	ytConfig := yt.Config{
		Proxy: config.Proxy,
	}

	_, err := ythttp.NewClient(&ytConfig)
	if err != nil {
		l.Fatal("error creating YT client", log.Error(err))
	}

	app := &App{
		l: l,
	}

	app.httpListen, err = net.Listen("tcp", config.HTTPEndpoint)
	if err != nil {
		l.Fatal("error creating HTTP listener", log.Error(err))
	}
	l.Info("started HTTP listener", log.String("addr", app.httpListen.Addr().String()))

	r := chi.NewMux()

	httpServer := &http.Server{
		Addr:    config.HTTPEndpoint,
		Handler: r,
	}

	err = Register(r, app)
	if err != nil {
		l.Fatal("error registering HTTP routes", log.Error(err))
	}

	go func() {
		l.Error("HTTP server has stopped", log.Error(httpServer.Serve(app.httpListen)))
	}()

	return app
}

func (a *App) URL() string {
	return fmt.Sprintf("http://%s", a.httpListen.Addr().String())
}

func (a *App) Stop() error {
	return a.httpListen.Close()
}

func Register(r chi.Router, client *App) error {
	mux := runtime.NewServeMux(runtime.WithProtoErrorHandler(client.grpcErrorHandler))
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
