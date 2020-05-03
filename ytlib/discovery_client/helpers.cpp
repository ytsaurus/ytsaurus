#include "helpers.h"
#include "discovery_client_service_proxy.h"
#include "private.h"

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/algorithm_helpers.h>

namespace NYT::NDiscoveryClient {

using namespace NYTree;
using namespace NRpc;
using namespace NConcurrency;

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

////////////////////////////////////////////////////////////////////////////////

bool IsMemberSystemAttribute(const TString& key)
{
    return key == PriorityAttribute ||
        key == RevisionAttribute ||
        key == LastHeartbeatTimeAttribute ||
        key == LastAttributesUpdateTimeAttribute;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
