#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryServerConfig
    : public NYTree::TYsonStruct
{
    std::vector<std::string> ServerAddresses;
    TDuration GossipPeriod;
    TDuration AttributesUpdatePeriod;
    int MaxMembersPerGossip;
    int GossipBatchSize;
    TDuration DiscoveryServerRpcTimeout;

    // It is not guaranteed that these limits won't be exceeded.
    // A server won't accept a heartbeat from a new member if adding it would exceed one of the limits,
    // but it will add new members through gossip regardless of any limits.
    // This is to keep the discovery servers at least somewhat consistent with each other.
    std::optional<int> MaxMembersPerGroup;
    std::optional<int> MaxGroupCount;
    std::optional<int> MaxGroupTreeSize;
    std::optional<int> MaxGroupTreeDepth;

    REGISTER_YSON_STRUCT(TDiscoveryServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
