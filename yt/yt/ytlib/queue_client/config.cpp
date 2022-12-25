#include "config.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NQueueClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void TQueueConsumerRegistrationManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/queue_agents");
    registrar.Parameter("cache_refresh_period", &TThis::CacheRefreshPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("user", &TThis::User)
        .Default(RootUserName);

    registrar.Postprocessor([] (TThis* config) {
        config->Root = config->Root.Normalize();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
    registrar.Parameter("queue_consumer_registration_manager", &TThis::QueueConsumerRegistrationManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
