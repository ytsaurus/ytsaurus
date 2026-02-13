package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/atomic"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

const (
	xReqIDHTTPHeader   = "X-Req-Id"
	traceparentHeader  = "traceparent"
	xTraceIDHTTPHeader = "X-Trace-ID"
	xSpanIDHTTPHeader  = "X-Span-ID"
)

func waitReady(ready *atomic.Bool) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !ready.Load() {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "not ready, try later"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

var requestIDKey struct{}

func timeout(timeout time.Duration) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func traceRequest(l log.Structured) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			traceparent := r.Header.Get(traceparentHeader)
			var traceID, parentSpanID string

			traceparentParts := strings.Split(traceparent, "-")
			if len(traceparentParts) != 4 {
				traceID = ytmsvc.RandHexString(32)
			} else {
				traceID = traceparentParts[1]
				parentSpanID = traceparentParts[2]
			}
			spanID := ytmsvc.RandHexString(16)

			traceIDField := log.String("trace_id", traceID)
			parentSpanIDField := log.String("parent_span_id", parentSpanID)
			spanIDField := log.String("span_id", spanID)

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			ww.Tee(&bytes.Buffer{})

			const bodySizeLimit = 1024 * 1024
			body, err := io.ReadAll(io.LimitReader(r.Body, bodySizeLimit))
			if err != nil {
				l.Error("error reading request body", log.Error(err))
			}
			r.Body = io.NopCloser(bytes.NewBuffer(body))
			_ = r.ParseForm()
			r.Body = io.NopCloser(bytes.NewBuffer(body))

			l.Debug("HTTP request started",
				traceIDField,
				parentSpanIDField,
				spanIDField,
				log.String("method", r.Method),
				log.String("path", r.URL.Path),
				log.String("query", r.Form.Encode()),
				log.ByteString("body", body),
				log.String("origin", ytmsvc.GetUserIP(r)),
				log.String("l7_req_id", r.Header.Get(xReqIDHTTPHeader)),
			)

			t0 := time.Now()
			defer func() {
				l.Debug("HTTP request finished",
					traceIDField,
					parentSpanIDField,
					spanIDField,
					log.Int("status", ww.Status()),
					log.Int("bytes", ww.BytesWritten()),
					log.Duration("duration", time.Since(t0)))
			}()

			ww.Header().Set(xTraceIDHTTPHeader, traceID)
			ww.Header().Set(xSpanIDHTTPHeader, spanID)
			ctx := ctxlog.WithFields(r.Context(), traceIDField, parentSpanIDField, spanIDField)
			ctx = context.WithValue(ctx, &requestIDKey, traceID)
			next.ServeHTTP(ww, r.WithContext(ctx))
		})
	}
}

func basePathCleanupMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to read request body"})
			return
		}
		_ = r.Body.Close()

		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			r.Body = io.NopCloser(bytes.NewBuffer(body))
			next.ServeHTTP(w, r)
			return
		}

		if req["row_filter"] != nil {
			req["row_filter"].(map[string]interface{})["base_path"] = ""
		}

		modifiedBody, err := json.Marshal(req)
		if err != nil {
			r.Body = io.NopCloser(bytes.NewBuffer(body))
			next.ServeHTTP(w, r)
			return
		}

		r.Body = io.NopCloser(bytes.NewBuffer(modifiedBody))
		r.ContentLength = int64(len(modifiedBody))

		next.ServeHTTP(w, r)
	})
}

func basePathDefaultMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to read request body"})
			return
		}
		_ = r.Body.Close()

		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			r.Body = io.NopCloser(bytes.NewBuffer(body))
			next.ServeHTTP(w, r)
			return
		}

		if req["row_filter"] == nil {
			req["row_filter"] = map[string]interface{}{
				"base_path": "/",
			}
		} else {
			if _, ok := req["row_filter"].(map[string]interface{})["base_path"]; !ok {
				req["row_filter"].(map[string]interface{})["base_path"] = "/"
			}
		}

		modifiedBody, err := json.Marshal(req)
		if err != nil {
			r.Body = io.NopCloser(bytes.NewBuffer(body))
			next.ServeHTTP(w, r)
			return
		}

		r.Body = io.NopCloser(bytes.NewBuffer(modifiedBody))
		r.ContentLength = int64(len(modifiedBody))

		next.ServeHTTP(w, r)
	})
}
