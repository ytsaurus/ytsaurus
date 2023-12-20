#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

//! These options are directly controllable via object attributes.
class TTabletCellOptions
    : public NHydra::TRemoteSnapshotStoreOptions
    , public NHydra::TRemoteChangelogStoreOptions
{
public:
    int PeerCount;
    bool IndependentPeers;
    NApi::TClusterTag ClockClusterTag;

    REGISTER_YSON_STRUCT(TTabletCellOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellOptions)

////////////////////////////////////////////////////////////////////////////////

//! These options can be changed in runtime.
class TDynamicTabletCellOptions
    : public NYTree::TYsonStruct
{
public:
    std::optional<double> CpuPerTabletSlot;
    std::optional<bool> SuppressTabletCellDecommission;
    double ForcedRotationMemoryRatio;
    int DynamicMemoryPoolWeight;
    bool EnableTabletDynamicMemoryLimit;
    std::optional<TString> SolomonTag;
    std::optional<double> MaxBackingStoreMemoryRatio;
    // COMPAT(akozhikhov): YT-14187
    bool IncreaseUploadReplicationFactor;
    std::optional<TString> BanMessage;

    REGISTER_YSON_STRUCT(TDynamicTabletCellOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletCellOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
