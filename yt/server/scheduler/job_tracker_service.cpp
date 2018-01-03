#include "job_tracker_service.h"
#include "private.h"
#include "scheduler.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public TServiceBase
{
public:
    TJobTrackerService(TBootstrap* bootstrap)
        : NRpc::TServiceBase(
            GetSyncInvoker(),
            TJobTrackerServiceProxy::GetDescriptor(),
            SchedulerLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4));
    }

private:
    TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NJobTrackerClient::NProto, Heartbeat)
    {
        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateAcceptsHeartbeats();
        scheduler->ProcessNodeHeartbeat(context);
    }
};

IServicePtr CreateJobTrackerService(TBootstrap* bootstrap)
{
    return New<TJobTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

