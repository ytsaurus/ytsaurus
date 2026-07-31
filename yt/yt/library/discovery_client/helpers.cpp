#include "helpers.h"
#include "discovery_client_service_proxy.h"
#include "private.h"

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDiscoveryClient {

using namespace NYTree;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableDiscoveryServerError(const TError& error) {
    return
        error.FindMatching(NDiscoveryClient::EErrorCode::InvalidGroupId) ||
        error.FindMatching(NDiscoveryClient::EErrorCode::InvalidMemberId) ||
        error.FindMatching(NDiscoveryClient::EErrorCode::MemberLimitExceeded) ||
        error.FindMatching(NDiscoveryClient::EErrorCode::GroupLimitExceeded) ||
        error.FindMatching(NDiscoveryClient::EErrorCode::NodeLimitExceeded) ||
        error.FindMatching(NDiscoveryClient::EErrorCode::DepthLimitExceeded);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TMemberInfo* protoMemberInfo, const TMemberInfo& memberInfo)
{
    protoMemberInfo->set_id(memberInfo.Id);
    protoMemberInfo->set_priority(memberInfo.Priority);
    if (memberInfo.Attributes) {
        NYTree::ToProto(protoMemberInfo->mutable_attributes(), *memberInfo.Attributes);
    }
    protoMemberInfo->set_revision(memberInfo.Revision);
}

void FromProto(TMemberInfo* memberInfo, const NProto::TMemberInfo& protoMemberInfo)
{
    memberInfo->Id = protoMemberInfo.id();
    memberInfo->Priority = protoMemberInfo.priority();
    if (protoMemberInfo.has_attributes()) {
        memberInfo->Attributes = NYTree::FromProto(protoMemberInfo.attributes());
    }
    memberInfo->Revision = protoMemberInfo.revision();
}

void ToProto(NProto::TGroupMeta* protoGroupMeta, const TGroupMeta& groupMeta)
{
    protoGroupMeta->set_member_count(groupMeta.MemberCount);
}

void FromProto(TGroupMeta* groupMeta, const NProto::TGroupMeta& protoGroupMeta)
{
    groupMeta->MemberCount = protoGroupMeta.member_count();
}

void ToProto(NProto::TListMembersOptions* protoListMembersOptions, const TListMembersOptions& listMembersOptions)
{
    protoListMembersOptions->set_limit(listMembersOptions.Limit);
    for (const auto& key : listMembersOptions.AttributeKeys) {
        protoListMembersOptions->add_attribute_keys(key);
    }
}

void FromProto(TListMembersOptions* listMembersOptions, const NProto::TListMembersOptions& protoListMembersOptions)
{
    listMembersOptions->Limit = protoListMembersOptions.limit();
    for (const auto& key : protoListMembersOptions.attribute_keys()) {
        listMembersOptions->AttributeKeys.push_back(key);
    }
}


void ToProto(NProto::TListGroupsOptions* protoListGroupsOptions, const TListGroupsOptions& listGroupsOptions)
{
    protoListGroupsOptions->set_limit(listGroupsOptions.Limit);
}

void FromProto(TListGroupsOptions* listSubgroupsOptions, const NProto::TListGroupsOptions& protoListGroupsOptions)
{
    listSubgroupsOptions->Limit = protoListGroupsOptions.limit();
}

////////////////////////////////////////////////////////////////////////////////

bool IsMemberSystemAttribute(TStringBuf key)
{
    return key == PriorityAttribute ||
        key == RevisionAttribute ||
        key == LastHeartbeatTimeAttribute ||
        key == LastAttributesUpdateTimeAttribute;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
