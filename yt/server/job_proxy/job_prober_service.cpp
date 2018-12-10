#include "job_prober_service.h"
#include "private.h"
#include "job_proxy.h"
#include "user_job.h"

#include <yt/server/shell/shell_manager.h>

#include <yt/server/exec_agent/public.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/core/tools/tools.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/signaler.h>
#include <yt/core/misc/fs.h>

#include <util/system/fs.h>

namespace NYT::NJobProxy {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NJobAgent;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NTools;
using namespace NShell;

////////////////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    TJobProberService(IJobProbePtr jobProxy, IInvokerPtr controlInvoker)
        : TServiceBase(
            controlInvoker,
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Fail));
    }

private:
    const IJobProbePtr JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto chunkIds = JobProxy_->DumpInputContext();
        context->SetResponseInfo("ChunkIds: %v", chunkIds);

        ToProto(response->mutable_chunk_ids(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetStderr)
    {
        auto stderrData = JobProxy_->GetStderr();

        response->set_stderr_data(stderrData);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Strace)
    {
        auto trace = JobProxy_->StraceJob();

        ToProto(response->mutable_trace(), trace.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, SignalJob)
    {
        Y_UNUSED(response);

        const auto& signalName = request->signal_name();

        context->SetRequestInfo("SignalName: %v",
            signalName);

        JobProxy_->SignalJob(signalName);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, PollJobShell)
    {
        auto parameters = TYsonString(request->parameters());

        context->SetRequestInfo("Parameters: %v",
            ConvertToYsonString(parameters, EYsonFormat::Text));

        auto result = JobProxy_->PollJobShell(parameters);

        ToProto(response->mutable_result(), result.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Interrupt)
    {
        Y_UNUSED(response);

        JobProxy_->Interrupt();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Fail)
    {
        Y_UNUSED(response);

        JobProxy_->Fail();

        context->Reply();
    }
};

IServicePtr CreateJobProberService(TJobProxyPtr jobProxy)
{
    return New<TJobProberService>(jobProxy, jobProxy->GetControlInvoker());
}

IServicePtr CreateJobProberService(IJobProbePtr jobProbe, IInvokerPtr controlInvoker)
{
    return New<TJobProberService>(jobProbe, controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
