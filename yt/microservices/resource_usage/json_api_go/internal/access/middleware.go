package access

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
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
	authInfo AuthInfo,
) {
	ctxlog.Info(r.Context(), l.Logger(), "Authentication success", log.String("user", authInfo.Login), log.Bool("is_service", authInfo.IsService))
	ctx := context.WithValue(r.Context(), AuthInfoKey, authInfo)
	ctx = ctxlog.WithFields(ctx, log.Any(string(AuthInfoKey), authInfo))
	next.ServeHTTP(w, r.WithContext(ctx))
}
