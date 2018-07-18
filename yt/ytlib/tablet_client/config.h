#pragma once

#include "public.h"

#include <yt/ytlib/hydra/config.h>

#include <yt/client/tablet_client/config.h>

#include <yt/core/misc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletClient {

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

class TTabletCellConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TNullable<TString>> Addresses;

    TTabletCellConfig()
    {
        RegisterParameter("addresses", Addresses);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
