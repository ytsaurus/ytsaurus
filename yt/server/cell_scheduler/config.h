#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/server/scheduler/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/node_tracker_client/config.h>

#include <yt/core/rpc/config.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

class TCellSchedulerConfig
    : public TServerConfig
{
public:
    //! Node-to-master connection.
    NApi::TNativeConnectionConfigPtr ClusterConnection;

    //! Node directory synchronization.
    NNodeTrackerClient::TNodeDirectorySynchronizerConfigPtr NodeDirectorySynchronizer;

    NScheduler::TSchedulerConfigPtr Scheduler;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    //! Known scheduler addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    TCellSchedulerConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);
        RegisterParameter("node_directory_synchronizer", NodeDirectorySynchronizer)
            .DefaultNew();
        RegisterParameter("scheduler", Scheduler)
            .DefaultNew();
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
        RegisterParameter("addresses", Addresses)
            .Default();

        RegisterPreprocessor([&] () {
            ResponseKeeper->EnableWarmup = false;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TCellSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
