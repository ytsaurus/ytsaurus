//go:build !internal
// +build !internal

package main

import (
	"net/http"

	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

type AuthorizerConfig struct {
	authCookieName string
}

type Authorizer struct {
	l      *zap.Logger
	config *AuthorizerConfig
}

func (a *Authorizer) getUser(r *http.Request, authCluster string) (*ytmsvc.User, *string, error) {
	return ytmsvc.GetUser(r, a.l, a.config.authCookieName, authCluster)
}

func newAuthorizerConfigFromCmd(cmd *cobra.Command) *AuthorizerConfig {
	return &AuthorizerConfig{
		authCookieName: ytmsvc.Must(cmd.Flags().GetString("auth-cookie-name")),
	}
}

func newAuthorizer(l *zap.Logger, config *AuthorizerConfig) *Authorizer {
	return &Authorizer{
		l:      l,
		config: config,
	}
}
