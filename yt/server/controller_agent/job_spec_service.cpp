#include "private.h"

#include "controller_agent.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/job_tracker_client/job_spec_service_proxy.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/ytlib/api/native_client.h>

#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/permission.h>

namespace NYT {
namespace NControllerAgent {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////

class TJobSpecService
    : public TServiceBase
{
public:
    explicit TJobSpecService(TBootstrap* bootstrap)
        : TServiceBase(
            // TODO(babenko): better queue
            bootstrap->GetControlInvoker(EControlQueue::Default),
            TJobSpecServiceProxy::GetDescriptor(),
            ControllerAgentLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobSpecs));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NJobTrackerClient::NProto, GetJobSpecs)
    {
        auto controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        std::vector<std::pair<TOperationId, TJobId>> jobSpecRequests;
        for (const auto& jobSpecRequest : request->requests()) {
            if (!jobSpecRequest.has_operation_id()) {
                THROW_ERROR_EXCEPTION("Malformed request: all job spec subrequests must contain operation id");
            }
            auto operationId = FromProto<TOperationId>(jobSpecRequest.operation_id());
            auto jobId = FromProto<TJobId>(jobSpecRequest.job_id());
            jobSpecRequests.emplace_back(operationId, jobId);
        }

        auto jobSpecResults = controllerAgent->GetJobSpecs(jobSpecRequests);

        std::vector<TSharedRef> jobSpecs;
        jobSpecs.reserve(jobSpecRequests.size());

        for (const auto& errorOrValue : jobSpecResults) {
            auto* jobSpecResponse = response->add_responses();
            ToProto(jobSpecResponse->mutable_error(), errorOrValue);
            if (errorOrValue.IsOK()) {
                jobSpecs.push_back(errorOrValue.Value());
            } else {
                jobSpecs.push_back(TSharedRef());
            }
        }

        response->Attachments() = jobSpecs;

        context->Reply();
    }
};

DEFINE_REFCOUNTED_TYPE(TJobSpecService)

IServicePtr CreateJobSpecService(TBootstrap* bootstrap)
{
    return New<TJobSpecService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

