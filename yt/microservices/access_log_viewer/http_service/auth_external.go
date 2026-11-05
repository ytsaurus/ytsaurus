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
	debugLogin     string
}

type Authorizer struct {
	l      *zap.Logger
	config *AuthorizerConfig
}

func (a *Authorizer) getUser(r *http.Request, authCluster string) (*ytmsvc.User, *string, error) {
	if a.config.debugLogin != "" {
		return &ytmsvc.User{Login: a.config.debugLogin}, nil, nil
	}
	return ytmsvc.GetUser(r, a.l, a.config.authCookieName, authCluster)
}

func newAuthorizerConfigFromCmd(cmd *cobra.Command) *AuthorizerConfig {
	debugLogin := ytmsvc.Must(cmd.Flags().GetString("debug-login"))
	if debugLogin != "" {
		logger.Warnf("User authentication is disabled; using debug login %q", debugLogin)
	}
	return &AuthorizerConfig{
		authCookieName: ytmsvc.Must(cmd.Flags().GetString("auth-cookie-name")),
		debugLogin:     debugLogin,
	}
}

func newAuthorizer(l *zap.Logger, config *AuthorizerConfig) *Authorizer {
	return &Authorizer{
		l:      l,
		config: config,
	}
}
