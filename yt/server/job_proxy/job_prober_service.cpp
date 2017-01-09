#include "job_prober_service.h"
#include "private.h"
#include "job_proxy.h"

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NJobAgent;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    explicit TJobProberService(TJobProxyPtr jobProxy)
        : TServiceBase(
            jobProxy->GetControlInvoker(),
            TJobProberServiceProxy::GetDescriptor(),
            JobProxyLogger)
        , JobProxy_(jobProxy)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Strace));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SignalJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Interrupt));
    }

private:
    const TJobProxyPtr JobProxy_;


    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        auto chunkIds = JobProxy_->DumpInputContext(jobId);
        context->SetResponseInfo("ChunkIds: %v", chunkIds);

        ToProto(response->mutable_chunk_ids(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetStderr)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        auto stderrData = JobProxy_->GetStderr(jobId);

        response->set_stderr_data(stderrData);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Strace)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto trace = JobProxy_->Strace(jobId);

        ToProto(response->mutable_trace(), trace.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, SignalJob)
    {
        Y_UNUSED(response);

        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& signalName = request->signal_name();

        context->SetRequestInfo("JobId: %v, SignalName: %v",
            jobId,
            signalName);

        JobProxy_->SignalJob(jobId, signalName);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, PollJobShell)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto parameters = TYsonString(request->parameters());

        context->SetRequestInfo("JobId: %v, Parameters: %v",
            jobId,
            ConvertToYsonString(parameters, EYsonFormat::Text));

        auto result = JobProxy_->PollJobShell(jobId, parameters);

        ToProto(response->mutable_result(), result.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Interrupt)
    {
        Y_UNUSED(response);

        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v",
            jobId);

        JobProxy_->Interrupt(jobId);

        context->Reply();
    }
};

IServicePtr CreateJobProberService(TJobProxyPtr jobProxy)
{
    return New<TJobProberService>(jobProxy);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
