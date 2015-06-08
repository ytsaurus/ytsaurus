#pragma once

#include "public.h"

#include <core/misc/config.h>

#include <core/ytree/yson_serializable.h>

#include <ytlib/hydra/config.h>

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
