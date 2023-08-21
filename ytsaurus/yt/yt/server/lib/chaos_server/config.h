#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosPeerConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TString> AlienCluster;

    REGISTER_YSON_STRUCT(TChaosPeerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosPeerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChaosHydraConfig
    : public NYTree::TYsonStruct
{
public:
    std::vector<TChaosPeerConfigPtr> Peers;

    REGISTER_YSON_STRUCT(TChaosHydraConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosHydraConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
