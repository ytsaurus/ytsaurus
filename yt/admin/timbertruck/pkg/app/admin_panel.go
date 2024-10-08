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
)

type AdminPanelConfig struct {
	Port           int               `yaml:"port"`
	MonitoringTags map[string]string `yaml:"monitoring_tags"`
}

type adminPanel struct {
	server *http.Server
	logger *slog.Logger
}

func newAdminPanel(logger *slog.Logger, metrics *solomon.Registry, config AdminPanelConfig) (panel *adminPanel, err error) {
	defer func() {
		if err != nil {
			panel = nil
		}
	}()

	panel = &adminPanel{}
	panel.logger = logger.With("component", "AdminPanel")

	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		ctx, cancelF := context.WithTimeout(context.Background(), time.Second*10)
		defer cancelF()

		buffer := bytes.NewBuffer(nil)
		_, err = metrics.StreamJSON(ctx, buffer)
		if err != nil {
			logger.Error("Error serializing metrics", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Header().Set("Content-Type", "application/text")
			_, _ = io.WriteString(w, fmt.Sprintf("error serializing metrics: %v", err))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(buffer.Bytes())
		}
	})
	// "For testing purposes"
	mux.HandleFunc("/log-error", func(w http.ResponseWriter, r *http.Request) {
		panel.logger.Error("Log error requested")
		_, _ = io.WriteString(w, "Error is logged!")
	})

	loggingMux := loggingMiddleware{
		logger,
		mux,
	}

	panel.server = &http.Server{
		Handler: loggingMux,
		Addr:    fmt.Sprintf(":%v", config.Port),
	}
	return
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

func (h loggingMiddleware) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	requestID := uuid.New().String()
	msg := fmt.Sprintf("> request %v %v", req.Method, req.URL.Path)
	h.logger.Info(msg,
		"RequestId", requestID,
		"Method", req.Method,
		"Path", req.URL.Path,
		"Agent", req.UserAgent(),
		"IP", req.RemoteAddr)

	wrapped := loggingResponseWriter{
		next: w,
	}

	h.next.ServeHTTP(&wrapped, req)

	msg = fmt.Sprintf("< response %v %v", wrapped.statusCode, req.URL.Path)

	h.logger.Info(msg,
		"RequestId", requestID,
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
