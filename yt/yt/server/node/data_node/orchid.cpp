#include "orchid.h"

#include "ally_replica_manager.h"
#include "job_controller.h"
#include "location_manager.h"
#include "session_manager.h"

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/server/node/data_node/ytree_integration.h>

namespace NYT::NDataNode {

using namespace NJobAgent;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr GetOrchidService(const IBootstrap* bootstrap)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto mapService = New<TCompositeMapService>();

    mapService->AddChild(
        "job_resource_manager",
        bootstrap->GetJobResourceManager()->GetOrchidService());
    mapService->AddChild(
        "job_controller",
        bootstrap->GetJobController()->GetOrchidService());
    mapService->AddChild(
        "location_manager",
        bootstrap->GetDiskChangeChecker()->GetOrchidService());
    mapService->AddChild(
        "session_manager",
        bootstrap->GetSessionManager()->GetOrchidService());
    mapService->AddChild(
        "stored_chunks",
        CreateStoredChunkMapService(
            bootstrap->GetChunkStore(),
            bootstrap->GetAllyReplicaManager())
            ->Via(bootstrap->GetControlInvoker()));
    mapService->AddChild(
        "ally_replica_manager",
        bootstrap->GetAllyReplicaManager()->GetOrchidService());

    return mapService;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
