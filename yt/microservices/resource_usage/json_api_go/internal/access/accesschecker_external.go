//go:build !internal
// +build !internal

package access

import (
	"go.ytsaurus.tech/library/go/core/log"
	bulkaclcheckerclient "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/client_go"
)

func NewAccessChecker(l log.Structured, conf *Config) (*AccessChecker, error) {

	bulkACLCheckerClient, err := bulkaclcheckerclient.NewClient(&bulkaclcheckerclient.Config{
		BaseURL:       conf.BulkACLCheckerURL,
		GetACLRetries: 3,
	},
		l,
	)
	if err != nil {
		return nil, err
	}

	accessChecker := &AccessChecker{
		AccessCheckerBase: AccessCheckerBase{
			l:         l,
			conf:      conf,
			aclClient: bulkACLCheckerClient,
		},
	}
	return accessChecker, nil
}
