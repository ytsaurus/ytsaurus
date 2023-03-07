#pragma once

#include "discovery_server_service_proxy.h"

#include <yt/core/ytree/attributes.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/discovery_client/helpers.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct TGossipMemberInfo
{
    NDiscoveryClient::TMemberInfo MemberInfo;
    TString GroupId;
    TInstant LeaseDeadline;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TGossipMemberInfo* protoMemberInfo, const TGossipMemberInfo& MemberInfo);
void FromProto(TGossipMemberInfo* memberInfo, const NProto::TGossipMemberInfo& protoMemberInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

