//go:build !internal
// +build !internal

package main

import (
	"errors"
	"net/http"

	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/yterrors"
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
	user, ticket, err := ytmsvc.GetUser(r, a.l, a.config.authCookieName, authCluster)
	if err != nil && isInvalidCredentialsError(err) {
		return nil, nil, yterrors.Err(yterrors.CodeInvalidCredentials, "user credentials are missing or invalid", err)
	}
	return user, ticket, err
}

// getUserAndService authenticates the user; the external build has no caller service.
func (a *Authorizer) getUserAndService(r *http.Request, authCluster string) (*ytmsvc.User, *string, string, error) {
	user, ticket, err := a.getUser(r, authCluster)
	return user, ticket, "", err
}

func isInvalidCredentialsError(err error) bool {
	var httpError *yterrors.HTTPError
	return errors.Is(err, http.ErrNoCookie) ||
		(errors.As(err, &httpError) && httpError.StatusCode == http.StatusUnauthorized) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeAuthenticationError) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeInvalidCredentials)
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
