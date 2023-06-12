package yt

import (
	"context"
	"net/http"

	"github.com/golang/protobuf/proto"
	"go.ytsaurus.tech/yt/go/proto/core/rpc"
)

const (
	// XYaServiceTicket is http header that should be used for service ticket transfer.
	XYaServiceTicket = "X-Ya-Service-Ticket"
	// XYaUserTicket is http header that should be used for user ticket transfer.
	XYaUserTicket = "X-Ya-User-Ticket"
)

type Credentials interface {
	Set(r *http.Request)
	SetExtension(req *rpc.TRequestHeader)
}

type TokenCredentials struct {
	Token string
}

func (c *TokenCredentials) Set(r *http.Request) {
	r.Header.Add("Authorization", "OAuth "+c.Token)
}

func (c *TokenCredentials) SetExtension(req *rpc.TRequestHeader) {
	_ = proto.SetExtension(
		req,
		rpc.E_TCredentialsExt_CredentialsExt,
		&rpc.TCredentialsExt{Token: &c.Token},
	)
}

// UserTicketCredentials implements TVM user-tickets authentication.
type UserTicketCredentials struct {
	Ticket string
}

func (c *UserTicketCredentials) Set(r *http.Request) {
	r.Header.Set(XYaUserTicket, c.Ticket)
}

func (c *UserTicketCredentials) SetExtension(req *rpc.TRequestHeader) {
	_ = proto.SetExtension(
		req,
		rpc.E_TCredentialsExt_CredentialsExt,
		&rpc.TCredentialsExt{UserTicket: &c.Ticket},
	)
}

// ServiceTicketCredentials implements TVM service-tickets authentication.
type ServiceTicketCredentials struct {
	Ticket string
}

func (c *ServiceTicketCredentials) Set(r *http.Request) {
	r.Header.Set(XYaServiceTicket, c.Ticket)
}

func (c *ServiceTicketCredentials) SetExtension(req *rpc.TRequestHeader) {
	_ = proto.SetExtension(
		req,
		rpc.E_TCredentialsExt_CredentialsExt,
		&rpc.TCredentialsExt{ServiceTicket: &c.Ticket},
	)
}

type credentialsCtxKey struct{}

func ContextCredentials(ctx context.Context) Credentials {
	if v := ctx.Value(credentialsCtxKey{}); v != nil {
		return v.(Credentials)
	}

	return nil
}

// WithCredentials allows overriding client credentials on per-call basis.
func WithCredentials(ctx context.Context, credentials Credentials) context.Context {
	return context.WithValue(ctx, credentialsCtxKey{}, credentials)
}
