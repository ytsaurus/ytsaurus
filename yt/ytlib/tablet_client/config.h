#pragma once

#include "public.h"

#include <yt/ytlib/hydra/config.h>

#include <yt/client/tablet_client/config.h>

#include <yt/core/misc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

//! These options are directly controllable via object attributes.
class TTabletCellOptions
    : public NHydra::TRemoteSnapshotStoreOptions
    , public NHydra::TRemoteChangelogStoreOptions
{
public:
    int PeerCount;

    TTabletCellOptions()
    {
        RegisterParameter("peer_count", PeerCount)
            .Default(1)
            .InRange(1, MaxPeerCount);
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

    TDynamicTabletCellOptions()
    {
        RegisterParameter("cpu_per_tablet_slot", CpuPerTabletSlot)
            .Optional();
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
