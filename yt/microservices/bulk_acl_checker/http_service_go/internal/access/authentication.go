package access

import (
	"net/http"

	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

// Authenticator establishes who calls Bulk ACL Checker.
type Authenticator interface {
	Authenticate(req *http.Request) (ytmsvc.AuthInfo, error)
}
