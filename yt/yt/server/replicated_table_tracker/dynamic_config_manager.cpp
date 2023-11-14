#include "dynamic_config_manager.h"

#include <yt/yt/server/lib/tablet_server/config.h>

namespace NYT::NReplicatedTableTracker {

using namespace NApi::NNative;
using namespace NDynamicConfig;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    TDynamicConfigManagerConfigPtr config,
    IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TDynamicReplicatedTableTrackerConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = "//sys/@config/tablet_manager/replicated_table_tracker",
            .Name = "ReplicatedTableTracker",
            .ConfigIsTagged = false,
        },
        std::move(config),
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
