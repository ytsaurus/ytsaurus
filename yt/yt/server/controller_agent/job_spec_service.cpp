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

        std::vector<TSettleJobRequest> settleJobRequests;
        settleJobRequests.reserve(request->requests_size());
        for (const auto& jobSpecRequest : request->requests()) {
            settleJobRequests.emplace_back(TSettleJobRequest{
                .OperationId = FromProto<TOperationId>(jobSpecRequest.operation_id()),
                .AllocationId = FromProto<TAllocationId>(jobSpecRequest.allocation_id())
            });
        }

        context->SetRequestInfo("SettleJobRequests: %v",
            MakeFormattableView(settleJobRequests, [] (TStringBuilderBase* builder, const TSettleJobRequest& req) {
                FormatValue(builder, req.AllocationId, TStringBuf());
            }));

        auto future = controllerAgent->SettleJobs(settleJobRequests);

        future.Subscribe(BIND(
            [
                =,
                this,
                this_ = MakeStrong(this),
                settleJobRequests = std::move(settleJobRequests)
            ] (const TErrorOr<std::vector<TErrorOr<TJobStartInfo>>>& resultsOrError)
            {
                const auto& results = resultsOrError.ValueOrThrow();
                std::vector<TSharedRef> jobSpecBlobs;
                jobSpecBlobs.reserve(settleJobRequests.size());
                for (int index = 0; index < std::ssize(settleJobRequests); ++index) {
                    const auto& subrequest = settleJobRequests[index];
                    const auto& subresponse = results[index];
                    auto* protoSubresponse = response->add_responses();
                    if (subresponse.IsOK() && subresponse.Value().JobSpecBlob) {
                        jobSpecBlobs.push_back(std::move(subresponse.Value().JobSpecBlob));
                        ToProto(protoSubresponse->mutable_job_id(), subresponse.Value().JobId);
                    } else {
                        jobSpecBlobs.emplace_back();
                        auto error = !subresponse.IsOK()
                            ? static_cast<TError>(subresponse)
                            : TError("Controller returned empty job spec (has controller crashed?)");
                        YT_LOG_DEBUG(
                            error,
                            "Failed to extract job spec (OperationId: %v, AllocationId: %v)",
                            subrequest.OperationId,
                            subrequest.AllocationId);

                        ToProto(protoSubresponse->mutable_error(), error);
                    }
                }

                response->Attachments() = std::move(jobSpecBlobs);
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

