#include "job_spec_service.h"
#include "config.h"
#include "controller_agent.h"
#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/lib/controller_agent/job_spec_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NControllerAgent {

using namespace NRpc;
using namespace NScheduler;
using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NCypressClient;
using namespace NConcurrency;
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
            ControllerAgentLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobSpecs)
            .SetConcurrencyLimit(100000)
            .SetPooled(false));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobSpecs)
    {
        auto controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        std::vector<TJobSpecRequest> jobSpecRequests;
        jobSpecRequests.reserve(request->requests_size());
        for (const auto& jobSpecRequest : request->requests()) {
            jobSpecRequests.emplace_back(TJobSpecRequest{
                FromProto<TOperationId>(jobSpecRequest.operation_id()),
                FromProto<TJobId>(jobSpecRequest.job_id())
            });
        }

        context->SetRequestInfo("JobSpecRequests: %v",
            MakeFormattableView(jobSpecRequests, [] (TStringBuilderBase* builder, const TJobSpecRequest& req) {
                FormatValue(builder, req.JobId, TStringBuf());
            }));

        auto future = controllerAgent->ExtractJobSpecs(jobSpecRequests);

        future.Subscribe(BIND(
            [
                =,
                this,
                this_ = MakeStrong(this),
                jobSpecRequests = std::move(jobSpecRequests)
            ] (const TErrorOr<std::vector<TErrorOr<TSharedRef>>>& resultsOrError)
            {
                const auto& results = resultsOrError.ValueOrThrow();
                std::vector<TSharedRef> jobSpecs;
                jobSpecs.reserve(jobSpecRequests.size());
                for (size_t index = 0; index < jobSpecRequests.size(); ++index) {
                    const auto& subrequest = jobSpecRequests[index];
                    const auto& subresponse = results[index];
                    auto* protoSubresponse = response->add_responses();
                    ToProto(protoSubresponse->mutable_job_id(), jobSpecRequests[index].JobId);
                    if (subresponse.IsOK() && subresponse.Value()) {
                        jobSpecs.push_back(std::move(subresponse.Value()));
                    } else {
                        jobSpecs.emplace_back();
                        auto error = !subresponse.IsOK()
                            ? static_cast<TError>(subresponse)
                            : TError("Controller returned empty job spec (has controller crashed?)");
                        YT_LOG_DEBUG(error, "Failed to extract job spec (OperationId: %v, JobId: %v)",
                            subrequest.OperationId,
                            subrequest.JobId);

                        ToProto(protoSubresponse->mutable_error(), error);
                    }
                }

                response->Attachments() = std::move(jobSpecs);
                context->Reply();
            }).Via(GetCurrentInvoker()));
    }
};

DEFINE_REFCOUNTED_TYPE(TJobSpecService)

IServicePtr CreateJobSpecService(TBootstrap* bootstrap)
{
    return New<TJobSpecService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

