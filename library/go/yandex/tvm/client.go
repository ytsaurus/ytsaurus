package tvm

//go:generate ya tool mockgen -source=$GOFILE -destination=mocks/tvm.gen.go Client

import (
	"context"
	"fmt"
)

type ClientStatus int

// This constants must be in sync with EStatus from library/cpp/tvmauth/client/client_status.h
//
// # Check status
//
// You should check status of client with GetStatus():
//   - ClientOK - nothing to do here
//   - ClientWarning - you should trigger your monitoring alert.
//     Normal operation of TvmClient is still possible but there are problems with refreshing cache, so it is expiring.
//     Is tvm-api.yandex.net accessible?
//     Have you changed your TVM-secret or your backend (dst) deleted its TVM-client?
//   - ClientError - you should trigger your monitoring alert and close this instance for user-traffic.
//     TvmClient's cache is already invalid (expired) or soon will be: you can't check valid CheckedServiceTicket or be authenticated by your backends (dsts).
const (
	ClientOK ClientStatus = iota
	ClientWarning
	ClientError
)

func (s ClientStatus) String() string {
	switch s {
	case ClientOK:
		return "OK"
	case ClientWarning:
		return "Warning"
	case ClientError:
		return "Error"
	default:
		return fmt.Sprintf("Unknown%d", s)
	}
}

// # Check status
//
// You should check status of client with GetStatus():
//   - ClientOK - nothing to do here
//   - ClientWarning - you should trigger your monitoring alert.
//     Normal operation of TvmClient is still possible but there are problems with refreshing cache, so it is expiring.
//     Is tvm-api.yandex.net accessible?
//     Have you changed your TVM-secret or your backend (dst) deleted its TVM-client?
//   - ClientError - you should trigger your monitoring alert and close this instance for user-traffic.
//     TvmClient's cache is already invalid (expired) or soon will be: you can't check valid CheckedServiceTicket or be authenticated by your backends (dsts).
type ClientStatusInfo struct {
	Status ClientStatus

	// This message allows to trigger alert with useful message
	// It returns "OK" if Status==Ok
	LastError string
}

// Client allows to use aliases for ClientID.
//
// Alias is local label for ClientID which can be used to avoid this number in every checking case in code.
//
// # Check status
//
// You should check status of client with GetStatus():
//   - ClientOK - nothing to do here
//   - ClientWarning - you should trigger your monitoring alert.
//     Normal operation of TvmClient is still possible but there are problems with refreshing cache, so it is expiring.
//     Is tvm-api.yandex.net accessible?
//     Have you changed your TVM-secret or your backend (dst) deleted its TVM-client?
//   - ClientError - you should trigger your monitoring alert and close this instance for user-traffic.
//     TvmClient's cache is already invalid (expired) or soon will be: you can't check valid CheckedServiceTicket or be authenticated by your backends (dsts).
type Client interface {
	GetServiceTicketForAlias(ctx context.Context, alias string) (string, error)
	GetServiceTicketForID(ctx context.Context, dstID ClientID) (string, error)

	// CheckServiceTicket returns struct with SrcID: you should check it by yourself with ACL.
	// Implementations may return the same pointer to CheckedServiceTicket for the same input ticket.
	CheckServiceTicket(ctx context.Context, ticket string) (*CheckedServiceTicket, error)
	// CheckUserTicket authenticates TVM User Ticket.
	// Implementations may return the same pointer to CheckedUserTicket for the same input ticket.
	CheckUserTicket(ctx context.Context, ticket string, opts ...CheckUserTicketOption) (*CheckedUserTicket, error)
	GetRoles(ctx context.Context) (*Roles, error)

	// GetStatus returns current status of client:
	//  * you should trigger your monitoring if status is not Ok
	//  * it will be unable to operate if status is Invalid
	GetStatus(ctx context.Context) (ClientStatusInfo, error)
}

// # Check status
//
// You should check status of client with GetStatus():
//   - ClientOK - nothing to do here
//   - ClientWarning - you should trigger your monitoring alert.
//     Normal operation of TvmClient is still possible but there are problems with refreshing cache, so it is expiring.
//     Is tvm-api.yandex.net accessible?
//     Have you changed your TVM-secret or your backend (dst) deleted its TVM-client?
//   - ClientError - you should trigger your monitoring alert and close this instance for user-traffic.
//     TvmClient's cache is already invalid (expired) or soon will be: you can't check valid CheckedServiceTicket or be authenticated by your backends (dsts).
type ClientV2 interface {
	GetServiceTicket(ctx context.Context, alias string, opts ...GetServiceTicketV2Option) (string, error)
	CheckTicket(ctx context.Context, serviceTicket, userTicket string, opts ...CheckTicketV2Option) (*CheckedTickets, error)
	GetRoles(ctx context.Context) (*Roles, error)

	// GetStatus returns current status of client:
	//  * you should trigger your monitoring if status is not Ok
	//  * it will be unable to operate if status is Invalid
	GetStatus(ctx context.Context) (ClientStatusInfo, error)
}

type ClientMultiVersion interface {
	Client
	ClientV2
}

// Dynamic client allows to add dsts dynamically
type DynamicClient interface {
	Client

	GetOptionalServiceTicketForID(ctx context.Context, dstID ClientID) (*string, error)
	AddDsts(ctx context.Context, dsts []ClientID) error
}
