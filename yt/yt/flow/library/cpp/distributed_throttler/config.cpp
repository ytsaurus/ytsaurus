#include "config.h"

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

void TDistributedThrottlerServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Default();
    registrar.Parameter("queue_timeout", &TThis::QueueTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("drain_period", &TThis::DrainPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDistributedThrottlerClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("server_address", &TThis::ServerAddress)
        .Default();
    registrar.Parameter("throttler_name", &TThis::ThrottlerName)
        .Default();
    registrar.Parameter("client_id", &TThis::ClientId)
        .Default();
    registrar.Parameter("prefetching_config", &TThis::PrefetchingConfig)
        .DefaultNew();
    registrar.Parameter("retrying_channel", &TThis::RetryingChannel)
        .DefaultNew();
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
