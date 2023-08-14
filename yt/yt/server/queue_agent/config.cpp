#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/ytlib/queue_client/config.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NQueueAgent {

using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

void TAlertManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("alert_collection_period", &TThis::AlertCollectionPeriod)
        .Default(TDuration::MilliSeconds(500));
}

////////////////////////////////////////////////////////////////////////////////

void TCypressSynchronizerConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TCypressSynchronizerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pass_period", &TThis::PassPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("policy", &TThis::Policy)
        .Default(ECypressSynchronizerPolicy::Polling);
    registrar.Parameter("clusters", &TThis::Clusters)
        .Default();
    registrar.Parameter("poll_replicated_objects", &TThis::PollReplicatedObjects)
        .Default(false);
    registrar.Parameter("write_registration_table_mapping", &TThis::WriteReplicatedTableMapping)
        .Default(false);
    registrar.Parameter("chaos_replicated_table_queue_agent_stage", &TThis::ChaosReplicatedTableQueueAgentStage)
        .Default(NQueueClient::ProductionStage);
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();
    registrar.Parameter("stage", &TThis::Stage)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TQueueControllerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pass_period", &TThis::PassPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("enable_automatic_trimming", &TThis::EnableAutomaticTrimming)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pass_period", &TThis::PassPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("controller_thread_count", &TThis::ControllerThreadCount)
        .Default(4);
    registrar.Parameter("controller", &TThis::Controller)
        .DefaultNew();
    registrar.Parameter("handle_replicated_objects", &TThis::HandleReplicatedObjects)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentShardingManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pass_period", &TThis::PassPeriod)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_agent", &TThis::QueueAgent)
        .DefaultNew();
    registrar.Parameter("cypress_synchronizer", &TThis::CypressSynchronizer)
        .DefaultNew();
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("user", &TThis::User)
        .Default(QueueAgentUserName);
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("dynamic_state", &TThis::DynamicState)
        .DefaultNew();
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (auto& lockPath = config->ElectionManager->LockPath; lockPath.empty()) {
            lockPath = config->DynamicState->Root + "/leader_lock";
        }
        if (auto& dynamicConfigPath = config->DynamicConfigPath; dynamicConfigPath.empty()) {
            dynamicConfigPath = config->DynamicState->Root + "/config";
        }
    });
};

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentServerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("member_client", &TThis::MemberClient)
        .DefaultNew();
    registrar.Parameter("discovery_client", &TThis::DiscoveryClient)
        .DefaultNew();

    registrar.Parameter("alert_manager", &TThis::AlertManager)
        .DefaultNew();
    registrar.Parameter("queue_agent_sharding_manager", &TThis::QueueAgentShardingManager)
        .DefaultNew();
    registrar.Parameter("queue_agent", &TThis::QueueAgent)
        .DefaultNew();
    registrar.Parameter("cypress_synchronizer", &TThis::CypressSynchronizer)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
