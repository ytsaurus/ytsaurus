package access

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func checkViewAsLogin(r *http.Request) (string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	if len(body) == 0 {
		return "", nil
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewBuffer(body))
	var req map[string]interface{}
	if err := json.Unmarshal(body, &req); err != nil {
		return "", err
	}
	if viewAsLogin, ok := req["view_as_login"].(string); ok {
		return viewAsLogin, nil
	}
	return "", nil
}

func onAuthSuccess(
	w http.ResponseWriter,
	r *http.Request,
	next http.Handler,
	l log.Structured,
	authInfo ytmsvc.AuthInfo,
) {
	ctxlog.Info(r.Context(), l.Logger(), "Authentication success", log.String("user", authInfo.UserLogin), log.String("service", authInfo.ServiceLogin))
	ctx := context.WithValue(r.Context(), AuthInfoKey, authInfo)
	ctx = ctxlog.WithFields(ctx, log.Any(string(AuthInfoKey), authInfo))
	next.ServeHTTP(w, r.WithContext(ctx))
}
