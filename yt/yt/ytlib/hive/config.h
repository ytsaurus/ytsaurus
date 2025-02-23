#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct TCellDirectoryConfig
    : public NRpc::TBalancingChannelConfigBase
{
    REGISTER_YSON_STRUCT(TCellDirectoryConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClusterDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    //! TTL for GetClusterMeta request.
    TDuration ExpireAfterSuccessfulUpdateTime;
    TDuration ExpireAfterFailedUpdateTime;

    REGISTER_YSON_STRUCT(TClusterDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    //! Splay for directory updates period.
    TDuration SyncPeriodSplay;

    //! Timeout for SyncCells requests.
    TDuration SyncRpcTimeout;

    //! If |true| and cluster is multicell, SyncCells request
    //! will be sent to some secondary master.
    bool SyncCellsWithSecondaryMasters;

    REGISTER_YSON_STRUCT(TCellDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDownedCellTrackerConfig
    : public NYTree::TYsonStruct
{
    //! Time for cell tracker to forget downed chaos cells.
    TDuration ChaosCellExpirationTime;

    //! Time for cell tracker to forget downed cells of other types.
    TDuration TabletCellExpirationTime;

    REGISTER_YSON_STRUCT(TDownedCellTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDownedCellTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
