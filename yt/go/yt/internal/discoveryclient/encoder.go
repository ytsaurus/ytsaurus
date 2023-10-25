package discoveryclient

import (
	"context"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/client/discovery_client"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/rpcclient"
)

type Encoder struct {
	StartCall func() *rpcclient.Call

	Invoke rpcclient.CallInvoker
}

func (e *Encoder) newCall(method rpcclient.Method, req rpcclient.Request, attachments [][]byte) *rpcclient.Call {
	call := e.StartCall()
	call.Method = method
	call.Req = req
	call.Attachments = attachments
	call.CallID = guid.New()
	return call
}

func (e *Encoder) ListMembers(
	ctx context.Context,
	groupID string,
	opts *yt.ListMembersOptions,
) (members []*yt.MemberInfo, err error) {
	if opts == nil {
		opts = &yt.ListMembersOptions{}
	}

	req := &discovery_client.TReqListMembers{
		GroupId: &groupID,
		Options: convertListMembersOptions(opts),
	}

	call := e.newCall(MethodListMembers, NewListMembersRequest(req), nil)

	var rsp discovery_client.TRspListMembers
	if err = e.Invoke(ctx, call, &rsp); err != nil {
		return
	}

	members = makeMembers(rsp.Members)
	return
}

func (e *Encoder) ListGroups(
	ctx context.Context,
	prefix string,
	opts *yt.ListGroupsOptions,
) (result *yt.ListGroupsResponse, err error) {
	if opts == nil {
		opts = &yt.ListGroupsOptions{}
	}

	req := &discovery_client.TReqListGroups{
		Prefix:  &prefix,
		Options: convertListGroupsOptions(opts),
	}

	call := e.newCall(MethodListGroups, NewListGroupsRequest(req), nil)

	var rsp discovery_client.TRspListGroups
	if err = e.Invoke(ctx, call, &rsp); err != nil {
		return
	}

	return makeListGroupsResponse(&rsp)
}

func (e *Encoder) GetGroupMeta(
	ctx context.Context,
	groupID string,
	opts *yt.GetGroupMetaOptions,
) (meta *yt.GroupMeta, err error) {
	req := &discovery_client.TReqGetGroupMeta{
		GroupId: &groupID,
	}

	call := e.newCall(MethodGetGroupMeta, NewGetGroupMetaRequest(req), nil)

	var rsp discovery_client.TRspGetGroupMeta
	if err = e.Invoke(ctx, call, &rsp); err != nil {
		return
	}

	meta = makeMeta(rsp.Meta)
	return
}

func (e *Encoder) Heartbeat(
	ctx context.Context,
	groupID string,
	memberInfo yt.MemberInfo,
	leaseTimeout int64,
	opts *yt.HeartbeatOptions,
) (err error) {
	req := &discovery_client.TReqHeartbeat{
		GroupId:      &groupID,
		MemberInfo:   convertMemberInfo(&memberInfo),
		LeaseTimeout: &leaseTimeout,
	}

	call := e.newCall(MethodHeartbeat, NewHeartbeatRequest(req), nil)

	var rsp discovery_client.TRspHeartbeat
	return e.Invoke(ctx, call, &rsp)
}
