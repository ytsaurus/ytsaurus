#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryConfig
    : public NRpc::TBalancingChannelConfigBase
{ };

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    //! TTL for GetClusterMeta request.
    TDuration ExpireAfterSuccessfulUpdateTime;
    TDuration ExpireAfterFailedUpdateTime;

    TClusterDirectorySynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Seconds(60));

        RegisterParameter("expire_after_successful_update_time", ExpireAfterSuccessfulUpdateTime)
            .Alias("success_expiration_time")
            .Default(TDuration::Seconds(15));
        RegisterParameter("expire_after_failed_update_time", ExpireAfterFailedUpdateTime)
            .Alias("failure_expiration_time")
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    //! Splay for directory updates period.
    TDuration SyncPeriodSplay;

    //! Timeout for SyncCells requests.
    TDuration SyncRpcTimeout;

    //! If |true| and cluster is multicell, SyncCells request
    //! will be sent to some secondary master.
    bool SyncCellsWithSecondaryMasters;

    TCellDirectorySynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("sync_period_splay", SyncPeriodSplay)
            .Default(TDuration::Seconds(5));
        RegisterParameter("sync_rpc_timeout", SyncRpcTimeout)
            .Default(TDuration::Seconds(60));

        RegisterParameter("sync_cells_with_secondary_masters", SyncCellsWithSecondaryMasters)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
