#include "allocation_tracker_service.h"

#include "private.h"
#include "scheduler.h"
#include "bootstrap.h"

#include <yt/yt/server/lib/scheduler/allocation_tracker_service_proxy.h>

namespace NYT::NScheduler {

using namespace NRpc;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TAllocationTrackerService
    : public TServiceBase
{
public:
    explicit TAllocationTrackerService(TBootstrap* bootstrap)
        : TServiceBase(
            GetSyncInvoker(),
            TAllocationTrackerServiceProxy::GetDescriptor(),
            SchedulerLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4)
                .SetPooled(false));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto::NNode, Heartbeat)
    {
        const auto& scheduler = Bootstrap_->GetScheduler();
        scheduler->ProcessNodeHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateAllocationTrackerService(TBootstrap* bootstrap)
{
    return New<TAllocationTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
