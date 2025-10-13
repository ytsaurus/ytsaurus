//go:build !internal
// +build !internal

package main

import (
	"net/http"

	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/log/zap"
	lib "go.ytsaurus.tech/yt/microservices/lib/go"
)

type AuthorizerConfig struct {
	authCookieName string
	authProxy      string
}

type Authorizer struct {
	l      *zap.Logger
	config *AuthorizerConfig
}

func (a *Authorizer) getUser(r *http.Request) (*lib.User, *string, error) {
	return lib.GetUser(r, a.l, a.config.authCookieName, a.config.authProxy)
}

func newAuthorizerConfigFromCmd(cmd *cobra.Command) *AuthorizerConfig {
	return &AuthorizerConfig{
		authCookieName: lib.Must(cmd.Flags().GetString("auth-cookie-name")),
		authProxy:      lib.Must(cmd.Flags().GetString("auth-proxy")),
	}
}

func newAuthorizer(l *zap.Logger, config *AuthorizerConfig) *Authorizer {
	return &Authorizer{
		l:      l,
		config: config,
	}
}
