package discoveryclient

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/proto/client/discovery_client"
)

type ListMembersRequest struct {
	*discovery_client.TReqListMembers
}

func NewListMembersRequest(r *discovery_client.TReqListMembers) *ListMembersRequest {
	return &ListMembersRequest{TReqListMembers: r}
}

func (r ListMembersRequest) Log() []log.Field {
	return []log.Field{
		log.String("group_id", r.GetGroupId()),
	}
}

func (r ListMembersRequest) Path() (string, bool) {
	return "", false
}

type ListGroupsRequest struct {
	*discovery_client.TReqListGroups
}

func NewListGroupsRequest(r *discovery_client.TReqListGroups) *ListGroupsRequest {
	return &ListGroupsRequest{TReqListGroups: r}
}

func (r ListGroupsRequest) Log() []log.Field {
	return []log.Field{
		log.String("prefix", r.GetPrefix()),
	}
}

func (r ListGroupsRequest) Path() (string, bool) {
	return "", false
}

type GetGroupMetaRequest struct {
	*discovery_client.TReqGetGroupMeta
}

func NewGetGroupMetaRequest(r *discovery_client.TReqGetGroupMeta) *GetGroupMetaRequest {
	return &GetGroupMetaRequest{TReqGetGroupMeta: r}
}

func (r GetGroupMetaRequest) Log() []log.Field {
	return []log.Field{
		log.String("group_id", r.GetGroupId()),
	}
}

func (r GetGroupMetaRequest) Path() (string, bool) {
	return "", false
}

type HeartbeatRequest struct {
	*discovery_client.TReqHeartbeat
}

func NewHeartbeatRequest(r *discovery_client.TReqHeartbeat) *HeartbeatRequest {
	return &HeartbeatRequest{TReqHeartbeat: r}
}

func (r HeartbeatRequest) Log() []log.Field {
	return []log.Field{
		log.String("group_id", r.GetGroupId()),
		log.Any("member_info", r.GetMemberInfo()),
		log.Int64("lease_timeout", r.GetLeaseTimeout()),
	}
}

func (r HeartbeatRequest) Path() (string, bool) {
	return "", false
}
