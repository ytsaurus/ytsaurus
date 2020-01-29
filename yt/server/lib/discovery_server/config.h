#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    std::vector<TString> ServerAddresses;
    TDuration GossipPeriod;
    TDuration AttributesUpdatePeriod;
    int MaxMembersPerGossip;
    int GossipBatchSize;

    TDiscoveryServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
