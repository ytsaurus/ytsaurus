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

class TAlienCellSynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    TDuration SyncPeriod;
    TDuration FullSyncPeriod;

    REGISTER_YSON_STRUCT(TAlienCellSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlienCellSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChaosManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TAlienCellSynchronizerConfigPtr AlienCellSynchronizer;

    REGISTER_YSON_STRUCT(TDynamicChaosManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
