#pragma once

#include "public.h"

#include <yt/core/rpc/config.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryConfig
    : public NRpc::TBalancingChannelConfigBase
{
public:
    //! Timeout for RPC requests in TCellDirectory::Synchronize.
    TDuration SyncRpcTimeout;

    TCellDirectoryConfig()
    {
        RegisterParameter("sync_rpc_timeout", SyncRpcTimeout)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Interval between consequent SyncCells requests to the primary Hive Manager.
    TDuration SyncPeriod;

    TClusterDirectorySynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
