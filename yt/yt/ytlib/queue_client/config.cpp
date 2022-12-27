#include "config.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NQueueClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentDynamicStateConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/queue_agents");
    registrar.Parameter("consumer_registration_table_path", &TThis::ConsumerRegistrationTablePath)
        .Default("//sys/queue_agents/consumer_registrations");

    registrar.Postprocessor([] (TThis* config) {
        config->ConsumerRegistrationTablePath = config->ConsumerRegistrationTablePath.Normalize();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueueConsumerRegistrationManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("state_path", &TThis::TablePath)
        .Default("//sys/queue_agents/consumer_registrations");
    registrar.Parameter("cache_refresh_period", &TThis::CacheRefreshPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("user", &TThis::User)
        .Default(RootUserName);

    registrar.Postprocessor([] (TThis* config) {
        config->TablePath = config->TablePath.Normalize();
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
