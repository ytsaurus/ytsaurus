package dq

import (
	"context"
	"math"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func discoveryGroupID(alias string) string {
	return "/dq/" + alias
}

var instanceAttributeKeys = []string{
	"host",
	"grpc_port",
	"interconnect_port",
	"monitoring_port",
	"node_id",
	"operation_id",
	"job_id",
}

func listDiscoveryMembers(ctx context.Context, dc yt.DiscoveryClient, alias string) ([]*yt.MemberInfo, error) {
	if dc == nil {
		return nil, nil
	}

	members, err := dc.ListMembers(ctx, discoveryGroupID(alias), &yt.ListMembersOptions{
		Limit:         ptr.Int32(math.MaxInt32),
		AttributeKeys: instanceAttributeKeys,
	})
	if err != nil && yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchGroup) {
		return nil, nil
	}
	return members, err
}
