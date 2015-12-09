#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/server/scheduler/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/core/rpc/config.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

class TCellSchedulerConfig
    : public TServerConfig
{
public:
    //! Orchid cache expiration timeout.
    TDuration OrchidCacheExpirationTime;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    //! Node-to-master connection.
    NApi::TConnectionConfigPtr ClusterConnection;

    NScheduler::TSchedulerConfigPtr Scheduler;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TCellSchedulerConfig()
    {
        RegisterParameter("orchid_cache_expiration_time", OrchidCacheExpirationTime)
            .Default(TDuration::Seconds(1));
        RegisterParameter("rpc_port", RpcPort)
            .Default(9001);
        RegisterParameter("monitoring_port", MonitoringPort)
            .Default(10001);
        RegisterParameter("cluster_connection", ClusterConnection);
        RegisterParameter("scheduler", Scheduler)
            .DefaultNew();
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();

        RegisterInitializer([&] () {
            ResponseKeeper->EnableWarmup = false;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TCellSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
