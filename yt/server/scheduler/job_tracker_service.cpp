#include "stdafx.h"
#include "job_tracker_service.h"
#include "scheduler.h"
#include "private.h"

#include <ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public TServiceBase
{
public:
    TJobTrackerService(TBootstrap* bootstrap)
        : NRpc::TServiceBase(
            bootstrap->GetControlInvoker(),
            TJobTrackerServiceProxy::GetServiceName(),
            SchedulerLogger)
        , Bootstrap(bootstrap)
    {
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetRequestHeavy(true)
                .SetResponseHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4)
                .SetInvoker(bootstrap->GetControlInvoker(EControlQueue::Heartbeat))
                .SetMaxQueueSize(50));
    }

private:
    TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NJobTrackerClient::NProto, Heartbeat)
    {
        auto addresses = FromProto<TAddressMap>(request->addresses());
        const auto& resourceLimits = request->resource_limits();
        const auto& resourceUsage = request->resource_usage();

        context->SetRequestInfo("Address: %v, ResourceUsage: {%v}",
            GetDefaultAddress(addresses),
            FormatResourceUsage(resourceUsage, resourceLimits));

        // NB: Don't call ValidateConnected.
        // ProcessHeartbeat can be called even in disconnected state to update cell statistics.
        auto scheduler = Bootstrap->GetScheduler();
        auto node = scheduler->GetOrRegisterNode(addresses);
        if (node->GetMasterState() == ENodeState::Offline) {
            THROW_ERROR_EXCEPTION("Node is offline");
        }

        scheduler->ProcessHeartbeat(node, context);
    }

};

IServicePtr CreateJobTrackerService(TBootstrap* bootstrap)
{
    return New<TJobTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

