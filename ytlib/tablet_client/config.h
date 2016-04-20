#pragma once

#include "public.h"

#include <yt/ytlib/hydra/config.h>

#include <yt/core/misc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

//! These options are directly controllable via object attributes.
class TTabletCellOptions
    : public NHydra::TRemoteSnapshotStoreOptions
    , public NHydra::TRemoteChangelogStoreOptions
{ };

DEFINE_REFCOUNTED_TYPE(TTabletCellOptions)

///////////////////////////////////////////////////////////////////////////////

class TTabletCellConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TNullable<Stroka>> Addresses;

    TTabletCellConfig()
    {
        RegisterParameter("addresses", Addresses);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletCellConfig)

///////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public TExpiringCacheConfig
{  };

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
