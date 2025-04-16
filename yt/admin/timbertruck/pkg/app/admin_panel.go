package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"go.ytsaurus.tech/library/go/core/metrics/solomon"
	"go.ytsaurus.tech/library/go/httputil/headers"
)

type AdminPanelConfig struct {
	Port           int               `yaml:"port"`
	MonitoringTags map[string]string `yaml:"monitoring_tags"`

	// MetricsFormat is metrics stream format.
	// Possible values: 'spack', 'json'.
	MetricsFormat string `yaml:"metrics_format"`
}

type adminPanel struct {
	config  AdminPanelConfig
	metrics *solomon.Registry
	logger  *slog.Logger
	server  *http.Server
}

func newAdminPanel(logger *slog.Logger, metrics *solomon.Registry, config AdminPanelConfig) (panel *adminPanel, err error) {
	defer func() {
		if err != nil {
			panel = nil
		}
	}()

	panel = &adminPanel{
		config:  config,
		metrics: metrics,
		logger:  logger.With("component", "AdminPanel"),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", panel.handleMetrics)
	mux.HandleFunc("/ping", panel.handlePing)
	// "For testing purposes"
	mux.HandleFunc("/log-error", panel.handleLogError)

	loggingMux := loggingMiddleware{
		panel.logger,
		mux,
	}

	panel.server = &http.Server{
		Handler: loggingMux,
		Addr:    fmt.Sprintf(":%v", config.Port),
	}
	return
}

func (p *adminPanel) handleMetrics(w http.ResponseWriter, r *http.Request) {
	logger := p.requestLogger(r)

	ctx := r.Context()
	if ctx.Err() != nil {
		logger.Warn("Request context was canceled", "error", ctx.Err())
		return
	}

	ctx, cancelF := context.WithTimeout(ctx, time.Second*10)
	defer cancelF()

	var buffer bytes.Buffer
	_, err := p.metrics.Stream(ctx, &buffer)
	if err != nil {
		logger.Warn("Error serializing metrics", "error", err)
		return
	}
	contentType := headers.TypeApplicationXSolomonSpack.String()
	if p.config.MetricsFormat == string(solomon.StreamJSON) {
		contentType = headers.TypeApplicationJSON.String()
	}
	w.Header().Set(headers.ContentTypeKey, contentType)
	_, _ = w.Write(buffer.Bytes())
}

func (p *adminPanel) handlePing(w http.ResponseWriter, r *http.Request) {
	logger := p.requestLogger(r)
	logger.Debug("Ping")
	_, _ = io.WriteString(w, "OK")
}

func (p *adminPanel) handleLogError(w http.ResponseWriter, r *http.Request) {
	logger := p.requestLogger(r)

	logger.Error("Log error requested")
	_, _ = io.WriteString(w, "Error is logged!")
}

func (p *adminPanel) requestLogger(r *http.Request) *slog.Logger {
	requestID := r.Context().Value(requestIDContextKey{})
	if requestID == nil {
		return p.logger
	} else {
		return p.logger.With("RequestID", requestID.(string))
	}
}

func (p *adminPanel) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		err := p.server.Close()
		if err != nil {
			p.logger.Error("Error closing admin panel server", "error", err)
		}
	}()
	p.logger.Info("Starting admin panel server", "address", p.server.Addr)
	return p.server.ListenAndServe()
}

type loggingMiddleware struct {
	logger *slog.Logger

	next http.Handler
}

type requestIDContextKey struct{}

func (h loggingMiddleware) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	requestID := uuid.New().String()
	msg := fmt.Sprintf("> request %v %v", req.Method, req.URL.Path)
	h.logger.Info(msg,
		"RequestID", requestID,
		"Method", req.Method,
		"Path", req.URL.Path,
		"Agent", req.UserAgent(),
		"IP", req.RemoteAddr)

	wrapped := loggingResponseWriter{
		next: w,
	}

	ctx := context.WithValue(req.Context(), requestIDContextKey{}, requestID)
	h.next.ServeHTTP(&wrapped, req.WithContext(ctx))

	msg = fmt.Sprintf("< response %v %v", wrapped.statusCode, req.URL.Path)

	h.logger.Info(msg,
		"RequestID", requestID,
		"Method", req.Method,
		"Path", req.URL.Path,
		"StatusCode", wrapped.statusCode)
}

type loggingResponseWriter struct {
	next       http.ResponseWriter
	statusCode int
}

func (w *loggingResponseWriter) Header() http.Header {
	return w.next.Header()
}

func (w *loggingResponseWriter) Write(data []byte) (int, error) {
	if w.statusCode == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.next.Write(data)
}

func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	w.next.WriteHeader(statusCode)
	w.statusCode = statusCode
}
