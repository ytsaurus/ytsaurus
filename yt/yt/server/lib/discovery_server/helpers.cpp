#include "helpers.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TGossipMemberInfo* protoMemberInfo, const TGossipMemberInfo& memberInfo)
{
    ToProto(protoMemberInfo->mutable_member_info(), memberInfo.MemberInfo);
    protoMemberInfo->set_group_id(memberInfo.GroupId);
    protoMemberInfo->set_lease_deadline(NYT::ToProto<i64>(memberInfo.LeaseDeadline));
}

void FromProto(TGossipMemberInfo* memberInfo, const NProto::TGossipMemberInfo& protoMemberInfo)
{
    memberInfo->MemberInfo = NYT::FromProto<NDiscoveryClient::TMemberInfo>(protoMemberInfo.member_info());
    memberInfo->GroupId = protoMemberInfo.group_id();
    memberInfo->LeaseDeadline = NYT::FromProto<TInstant>(protoMemberInfo.lease_deadline());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
