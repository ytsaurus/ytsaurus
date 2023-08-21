#include "node_tracker_log.h"

#include "config.h"
#include "node.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/core/logging/fluent_log.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NLogging;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void LogNodeState(
    TBootstrap* bootstrap,
    TNode* node)
{
    const auto& hydraManager = bootstrap->GetHydraFacade()->GetHydraManager();
    if (!bootstrap->GetConfigManager()->GetConfig()->NodeTracker->EnableStructuredLog ||
        hydraManager->IsLeader() ||
        hydraManager->IsRecovery())
    {
        return;
    }

    LogStructuredEventFluently(NodeTrackerServerStructuredLogger, ELogLevel::Info)
        .Item("node_id").Value(node->GetId())
        .Item("node_address").Value(node->GetDefaultAddress())
        .Item("node_state").Value(node->GetAggregatedState());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
