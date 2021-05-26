#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

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

    TTabletCellOptions()
    {
        RegisterParameter("peer_count", PeerCount)
            .Default(1)
            .InRange(1, MaxPeerCount);
        RegisterParameter("independent_peers", IndependentPeers)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletCellOptions)

////////////////////////////////////////////////////////////////////////////////

//! These options can be changed in runtime.

class TDynamicTabletCellOptions
    : public NYTree::TYsonSerializable
{
public:
    std::optional<double> CpuPerTabletSlot;
    std::optional<bool> SuppressTabletCellDecommission;
    double ForcedRotationMemoryRatio;
    // TODO(babenko): either drop or make always false.
    bool EnableForcedRotationBackingMemoryAccounting;
    int DynamicMemoryPoolWeight;
    bool EnableTabletDynamicMemoryLimit;
    std::optional<TString> SolomonTag;
    std::optional<double> MaxBackingStoreMemoryRatio;
    // COMPAT(akozhikhov): YT-14187
    bool IncreaseUploadReplicationFactor;

    TDynamicTabletCellOptions()
    {
        RegisterParameter("cpu_per_tablet_slot", CpuPerTabletSlot)
            .Optional();
        RegisterParameter("suppress_tablet_cell_decommission", SuppressTabletCellDecommission)
            .Optional();
        RegisterParameter("forced_rotation_memory_ratio", ForcedRotationMemoryRatio)
            .InRange(0.0, 1.0)
            .Default(0.8);
        RegisterParameter("enable_forced_rotation_backing_memory_accounting", EnableForcedRotationBackingMemoryAccounting)
            .Default(true);
        RegisterParameter("dynamic_memory_pool_weight", DynamicMemoryPoolWeight)
            .InRange(1, MaxDynamicMemoryPoolWeight)
            .Default(1);
        RegisterParameter("enable_tablet_dynamic_memory_limit", EnableTabletDynamicMemoryLimit)
            .Default(true);
        RegisterParameter("solomon_tag", SolomonTag)
            .Optional()
            .DontSerializeDefault();
        RegisterParameter("max_backing_store_memory_ratio", MaxBackingStoreMemoryRatio)
            .Default();
        RegisterParameter("increase_upload_replication_factor", IncreaseUploadReplicationFactor)
            .Default(false);

        RegisterPostprocessor([&] {
            if (!EnableForcedRotationBackingMemoryAccounting &&
                MaxBackingStoreMemoryRatio &&
                *MaxBackingStoreMemoryRatio + ForcedRotationMemoryRatio >= 1.0)
            {
                THROW_ERROR_EXCEPTION("\"max_backing_store_memory_ratio\" + "
                    "\"forced_rotation_memory_ratio\" should be less than 1"
                    " if \"enable_forced_rotation_backing_memory_accounting\" is false");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletCellOptions)

////////////////////////////////////////////////////////////////////////////////

class TTabletCellConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<std::optional<TString>> Addresses;

    TTabletCellConfig()
    {
        RegisterParameter("addresses", Addresses);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
