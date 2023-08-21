#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServerConfig
    : public NYTree::TYsonStruct
{
public:
    std::vector<TString> ServerAddresses;
    TDuration GossipPeriod;
    TDuration AttributesUpdatePeriod;
    int MaxMembersPerGossip;
    int GossipBatchSize;

    REGISTER_YSON_STRUCT(TDiscoveryServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
