#pragma once

#include "public.h"
#include "discovery_client_service_proxy.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/public.h>

#include <yt/core/ytree/attributes.h>

#include <yt/core/rpc/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

struct TListMembersOptions
{
    int Limit = 1000;
    std::vector<TString> AttributeKeys;
};

struct TMemberInfo
{
    TMemberId Id;
    i64 Priority = 0;
    std::unique_ptr<NYTree::IAttributeDictionary> Attributes;
    i64 Revision = 0;
};

struct TGroupMeta
{
    int MemberCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TMemberInfo* protoMemberInfo, const TMemberInfo& MemberInfo);
void FromProto(TMemberInfo* memberInfo, const NProto::TMemberInfo& protoMemberInfo);

void ToProto(NProto::TGroupMeta* protoGroupMeta, const TGroupMeta& groupMeta);
void FromProto(TGroupMeta* groupMeta, const NProto::TGroupMeta& protoGroupMeta);

void ToProto(NProto::TListMembersOptions* protoListMembersOptions, const TListMembersOptions& listMembersOptions);
void FromProto(TListMembersOptions* listMembersOptions, const NProto::TListMembersOptions& protoListMembersOptions);

////////////////////////////////////////////////////////////////////////////////

bool IsMemberSystemAttribute(const TString& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

