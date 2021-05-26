#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosPeerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TString> AlienCluster;

    TChaosPeerConfig()
    {
        RegisterParameter("alien_cluster", AlienCluster)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosPeerConfig)

class TChaosHydraConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TChaosPeerConfigPtr> Peers;

    TChaosHydraConfig()
    {
        RegisterParameter("peers", Peers);
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosHydraConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChaosManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDynamicChaosManagerConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TDynamicChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
