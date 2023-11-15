#include "config.h"

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

void TReplicatedTableTrackerServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("rtt_service_request_timeout", &TThis::RttServiceRequestTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Postprocessor([] (TThis* config) {
        if (auto& lockPath = config->ElectionManager->LockPath; lockPath.empty()) {
            lockPath = "//sys/replicated_table_tracker/leader_lock";
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
