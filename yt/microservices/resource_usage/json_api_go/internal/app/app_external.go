//go:build !internal
// +build !internal

package app

import (
	"go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/access"
)

func (a *App) getAccessConfig() *access.Config {
	return &access.Config{
		ConfigBase: access.ConfigBase{
			DebugLogin:        a.conf.DebugLogin,
			DisableACL:        a.conf.DisableACL,
			BulkACLCheckerURL: a.conf.BulkACLCheckerURL,
			IncludedClusters:  a.conf.IncludedClusters,
			TokenEnvVariable:  a.conf.TokenEnvVariable,
		},
		AuthCookieName: a.conf.AuthCookieName,
	}
}
