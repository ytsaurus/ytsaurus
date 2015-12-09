#include "job_prober_service.h"
#include "private.h"
#include "job_proxy.h"

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/core/misc/common.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    TJobProberService(TJobProxy* jobProxy)
        : TServiceBase(
            jobProxy->GetControlInvoker(),
            TJobProberServiceProxy::GetServiceName(),
            JobProxyLogger,
            TJobProberServiceProxy::GetProtocolVersion())
        , JobProxy_(jobProxy)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Strace));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SignalJob));
    }

private:
    TJobProxy* JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        auto chunkIds = JobProxy_->DumpInputContext(jobId);
        context->SetResponseInfo("ChunkId: [%v]", JoinToString(chunkIds));

        ToProto(response->mutable_chunk_id(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Strace)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto trace = JobProxy_->Strace(jobId);

        context->SetResponseInfo("Trace: %Qv", trace.Data());

        ToProto(response->mutable_trace(), trace.Data());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, SignalJob)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto signalName = FromProto<Stroka>(request->signal_name());

        context->SetRequestInfo("JobId: %v, SignalName: %v",
            jobId,
            signalName);

        JobProxy_->SignalJob(jobId, signalName);

        context->Reply();
    }
};

IServicePtr CreateJobProberService(TJobProxy* jobProxy)
{
    return New<TJobProberService>(jobProxy);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
