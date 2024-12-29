#pragma once

#include "discovery_server_service_proxy.h"

#include <yt/yt/server/lib/discovery_server/config.h>

#include <yt/yt/ytlib/discovery_client/helpers.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/rpc/public.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct TGossipMemberInfo
{
    NDiscoveryClient::TMemberInfo MemberInfo;
    TGroupId GroupId;
    TInstant LeaseDeadline;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TGossipMemberInfo* protoMemberInfo, const TGossipMemberInfo& MemberInfo);
void FromProto(TGossipMemberInfo* memberInfo, const NProto::TGossipMemberInfo& protoMemberInfo);

////////////////////////////////////////////////////////////////////////////////

struct TGroupManagerInfo
{
    const TDiscoveryServerConfigPtr Config;
    int GroupTreeSize = 0;
    int GroupCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

