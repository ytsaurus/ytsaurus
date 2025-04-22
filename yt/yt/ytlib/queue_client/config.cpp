#include "config.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NQueueClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentStageChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_request_timeout", &TThis::DefaultRequestTimeout)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentDynamicStateConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/queue_agents");
    registrar.Parameter("consumer_registration_table_path", &TThis::ConsumerRegistrationTablePath)
        .Default("//sys/queue_agents/consumer_registrations");
    registrar.Parameter("replicated_table_mapping_table_path", &TThis::ReplicatedTableMappingTablePath)
        .Default("//sys/queue_agents/replicated_table_mapping");
}

////////////////////////////////////////////////////////////////////////////////

void TQueueConsumerRegistrationManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("state_write_path", &TThis::StateWritePath)
        .Default("//sys/queue_agents/consumer_registrations");
    registrar.Parameter("state_read_path", &TThis::StateReadPath)
        .Default("//sys/queue_agents/consumer_registrations");
    registrar.Parameter("replicated_table_mapping_read_path", &TThis::ReplicatedTableMappingReadPath)
        .Default("//sys/queue_agents/replicated_table_mapping");
    registrar.Parameter("bypass_caching", &TThis::BypassCaching)
        .Default(false);
    registrar.Parameter("cache_refresh_period", &TThis::CacheRefreshPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("configuration_refresh_period", &TThis::ConfigurationRefreshPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("user", &TThis::User)
        .Default(RootUserName);
    registrar.Parameter("resolve_symlinks", &TThis::ResolveSymlinks)
        .Default(false);
    registrar.Parameter("resolve_replicas", &TThis::ResolveReplicas)
        .Default(false);
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
