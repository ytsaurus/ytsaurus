#include "helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TGossipMemberInfo* protoMemberInfo, const TGossipMemberInfo& memberInfo)
{
    ToProto(protoMemberInfo->mutable_member_info(), memberInfo.MemberInfo);
    protoMemberInfo->set_group_id(memberInfo.GroupId);
    protoMemberInfo->set_lease_deadline(NYT::ToProto(memberInfo.LeaseDeadline));
}

void FromProto(TGossipMemberInfo* memberInfo, const NProto::TGossipMemberInfo& protoMemberInfo)
{
    memberInfo->MemberInfo = NYT::FromProto<NDiscoveryClient::TMemberInfo>(protoMemberInfo.member_info());
    memberInfo->GroupId = protoMemberInfo.group_id();
    memberInfo->LeaseDeadline = NYT::FromProto<TInstant>(protoMemberInfo.lease_deadline());
}

void FromProto(TGossipMemberInfo* memberInfo, NProto::TGossipMemberInfo&& protoMemberInfo)
{
    memberInfo->MemberInfo = NYT::FromProto<NDiscoveryClient::TMemberInfo>(
        std::move(*protoMemberInfo.mutable_member_info()));
    memberInfo->GroupId = NYT::FromProto<TGroupId>(std::move(*protoMemberInfo.mutable_group_id()));
    memberInfo->LeaseDeadline = NYT::FromProto<TInstant>(protoMemberInfo.lease_deadline());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
