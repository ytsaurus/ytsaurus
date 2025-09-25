//go:build !internal
// +build !internal

package access

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/yt/go/proto/core/rpc"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

type cookieCredentials struct {
	cookie    *http.Cookie
	csrfToken string
}

func (c cookieCredentials) Set(r *http.Request) {
	r.AddCookie(c.cookie)
	r.Header.Set("X-Csrf-Token", c.csrfToken)
}

func (c cookieCredentials) SetExtension(req *rpc.TRequestHeader) {
	_ = proto.SetExtension(
		req,
		rpc.E_TCredentialsExt_CredentialsExt,
		&rpc.TCredentialsExt{SessionId: &c.cookie.Value},
	)
}

func getClusterFromRequest(r *http.Request) (string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}
	if cluster, ok := data["cluster"]; ok {
		if clusterStr, ok := cluster.(string); ok {
			return clusterStr, nil
		}
	}
	return "", errors.New("`cluster` not found in request body")
}

func (a *AccessChecker) UserAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cookie, err := r.Cookie(a.conf.AuthCookieName)
		if err != nil || cookie == nil {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Authentication failed [getting cookie]: " + err.Error()})
			return
		}

		clusterName, err := getClusterFromRequest(r)
		if err != nil || clusterName == "" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Authentication failed [getting cluster]: " + err.Error()})
			return
		}

		var proxy string
		for _, c := range a.conf.IncludedClusters {
			if c.ClusterName == clusterName {
				proxy = c.Proxy
				break
			}
		}
		if proxy == "" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Authentication failed [getting proxy from cluster]: " + err.Error()})
			return
		}

		yc, err := ythttp.NewClient(&yt.Config{
			Proxy:  proxy,
			Logger: a.l,
			Credentials: cookieCredentials{
				cookie: cookie,
			},
		})
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Authentication failed [create yt client]: " + err.Error()})
			return
		}
		user, err := yc.WhoAmI(ctx, nil)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Authentication failed [whoami]: " + err.Error()})
			return
		}
		authInfo := AuthInfo{
			Login:     user.Login,
			IsService: false,
		}

		// TODO(ilyaibraev): implement view_as_login check

		onAuthSuccess(w, r, next, a.l, authInfo)
	})
}
