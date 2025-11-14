package app

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/errgroup"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/httputil/middleware/httpmetrics"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
	resourceusage "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/resource_usage"
)

const (
	httpServerGracefulStopTimeout = 30 * time.Second
)

type App struct {
	conf *Config
	l    log.Structured

	metrics *MetricsRegistry
}

func NewApp(c *Config, l log.Structured) *App {
	return &App{
		conf:    c,
		l:       l,
		metrics: NewMetricsRegistry(),
	}
}

func (a *App) Run(ctx context.Context) error {
	a.l.Info("Starting resource usage")
	defer a.l.Info("Stopped resource usage")

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		a.runHTTPServer(gctx, a.newDebugHTTPServer())
		return gctx.Err()
	})

	r := chi.NewMux()
	r.Use(httpmetrics.New(a.metrics.WithPrefix("http")))
	r.Use(timeout(a.conf.HTTPHandlerTimeout))
	r.Use(traceRequest(a.l))
	r.Use(ytmsvc.CORS(a.conf.CORS))

	api := NewAPI(
		a.l,
		a.getResourceUsageConfig(),
		a.getAccessConfig(),
	)

	g.Go(func() error {
		api.StartServingClusters(gctx)
		return gctx.Err()
	})

	apiRouter := r

	clusterMetrics := a.metrics.WithTags(map[string]string{"api": "resource_usage"})
	api.RegisterMetrics(clusterMetrics)
	apiRouter.Mount("/", api.Routes())
	api.SetReady()

	server := &http.Server{
		Addr:    a.conf.HTTPAddr,
		Handler: r,
	}

	g.Go(func() error {
		a.runHTTPServer(gctx, server)
		return gctx.Err()
	})

	return g.Wait()
}

func (a *App) runHTTPServer(ctx context.Context, s *http.Server) {
	a.l.Info("starting http server", log.String("addr", s.Addr))

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := s.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			a.l.Error("http server error", log.Error(err))
			panic(err)
		}
	}()

	<-ctx.Done()

	a.l.Info("waiting for http server to stop",
		log.String("addr", a.conf.HTTPAddr), log.Duration("timeout", httpServerGracefulStopTimeout))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpServerGracefulStopTimeout)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		if err == context.DeadlineExceeded {
			a.l.Warn("http server shutdown deadline exceeded",
				log.String("addr", a.conf.HTTPAddr))
		} else {
			panic(err)
		}
	}

	wg.Wait()

	a.l.Info("http server stopped", log.String("addr", s.Addr))
}

func (a *App) newDebugHTTPServer() *http.Server {
	debugRouter := chi.NewMux()
	debugRouter.Handle("/debug/*", http.DefaultServeMux)
	a.metrics.HandleMetrics(debugRouter)
	return &http.Server{
		Addr:    a.conf.DebugHTTPAddr,
		Handler: debugRouter,
	}
}

func (a *App) getResourceUsageConfig() *resourceusage.Config {
	return &resourceusage.Config{
		IncludedClusters:              a.conf.IncludedClusters,
		SnapshotRoot:                  ypath.Path(a.conf.SnapshotRoot),
		ExcludedFields:                a.conf.ExcludedFields,
		UpdateSnapshotsOnEveryRequest: a.conf.UpdateSnapshotsOnEveryRequest,
	}
}
