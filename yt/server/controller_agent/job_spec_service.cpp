#include "job_spec_service.h"
#include "controller_agent.h"
#include "bootstrap.h"
#include "private.h"

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/job_tracker_client/job_spec_service_proxy.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/misc/format.h>
#include <yt/core/ytree/permission.h>

namespace NYT {
namespace NControllerAgent {

using namespace NRpc;
using namespace NScheduler;
using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NJobTrackerClient;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////

class TJobSpecService
    : public TServiceBase
{
public:
    explicit TJobSpecService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
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

        std::vector<TJobSpecRequest> jobSpecRequests;
        for (const auto& jobSpecRequest : request->requests()) {
            jobSpecRequests.emplace_back(TJobSpecRequest{
                FromProto<TOperationId>(jobSpecRequest.operation_id()),
                FromProto<TJobId>(jobSpecRequest.job_id())
            });
        }

        context->SetRequestInfo("JobSpecRequests: %v",
            MakeFormattableRange(jobSpecRequests, [] (TStringBuilder* builder, const TJobSpecRequest& req) {
                FormatValue(builder, req.JobId, TStringBuf());
            }));

        auto results = WaitFor(controllerAgent->ExtractJobSpecs(jobSpecRequests))
            .ValueOrThrow();

        std::vector<TSharedRef> jobSpecs;
        jobSpecs.reserve(jobSpecRequests.size());
        for (size_t index = 0; index < jobSpecRequests.size(); ++index) {
            const auto& subrequest = jobSpecRequests[index];
            const auto& subresponse = results[index];
            auto* protoSubresponse = response->add_responses();
            if (subresponse.IsOK() && subresponse.Value()) {
                jobSpecs.push_back(subresponse.Value());
            } else {
                jobSpecs.emplace_back();
                auto error = !subresponse.IsOK()
                    ? static_cast<TError>(subresponse)
                    : TError("Controller returned empty job spec (has controller crashed?)");
                LOG_DEBUG(error, "Failed to extract job spec (OperationId: %v, JobId: %v)",
                    subrequest.OperationId,
                    subrequest.JobId);

                ToProto(protoSubresponse->mutable_error(), error);
            }
        }

        response->Attachments() = std::move(jobSpecs);
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

