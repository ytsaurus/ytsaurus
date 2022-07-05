#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/security_client/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

void TStandaloneTabletBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("worker_thread_pool_size", &TThis::WorkerThreadPoolSize)
        .Default(3);
    registrar.Parameter("tablet_action_expiration_time", &TThis::TabletActionExpirationTime)
        .Default(TDuration::Minutes(20));
    registrar.Parameter("tablet_action_polling_period", &TThis::TabletActionPollingPeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancerServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("tablet_balancer", &TThis::TabletBalancer)
        .DefaultNew();
    registrar.Parameter("cluster_connection", &TThis::ClusterConnection);
    registrar.Parameter("cluster_user", &TThis::ClusterUser)
        .Default(NSecurityClient::TabletBalancerUserName);
    registrar.Parameter("root_path", &TThis::RootPath)
        .Default("//sys/tablet_balancer");
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (auto& lockPath = config->ElectionManager->LockPath; lockPath.empty()) {
            lockPath = config->RootPath + "/leader_lock";
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
