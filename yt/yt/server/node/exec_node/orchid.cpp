#include "orchid.h"
#include "job_controller.h"
#include "gpu_manager.h"
#include "slot_manager.h"

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/server/node/data_node/ytree_integration.h>

namespace NYT::NExecNode {

using namespace NJobAgent;
using namespace NYson;
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
        "gpu_manager",
        bootstrap->GetGpuManager()->GetOrchidService());
    mapService->AddChild(
        "slot_manager",
        bootstrap->GetSlotManager()->GetOrchidService());
    mapService->AddChild(
        "cached_chunks",
        NDataNode::CreateCachedChunkMapService(
            bootstrap->GetChunkCache())
                ->Via(bootstrap->GetControlInvoker()));
    mapService->AddChild(
        "job_proxy_sensors",
        bootstrap->GetJobProxySolomonExporter()->GetSensorService());

    return mapService;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
