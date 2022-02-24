package yttvm

import (
	"context"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/yandex/tvm"
)

// ProxyTVMID is a common for all YT clusters tvm app identifier.
//
// One ID serves for both HTTP and RPC proxies.
const ProxyTVMID = tvm.ClientID(2031010)

// issueProxyServiceTicket returns service ticket for YT API.
func issueProxyServiceTicket(ctx context.Context, client tvm.Client) (string, error) {
	ticket, err := client.GetServiceTicketForID(ctx, ProxyTVMID)
	if err != nil {
		return "", xerrors.Errorf("failed to obtain service ticket for %v: %w", ProxyTVMID, err)
	}
	return ticket, nil
}

// TVMFn returns function that cat issue service tickets for YT API.
func TVMFn(client tvm.Client) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		return issueProxyServiceTicket(ctx, client)
	}
}
