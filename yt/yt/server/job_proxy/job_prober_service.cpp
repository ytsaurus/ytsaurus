#include "job_prober_service.h"

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/server/tools/tools.h>

#include <yt/yt/server/lib/job_proxy/job_probe.h>
#include <yt/yt/server/lib/job_proxy/job_prober_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/fs.h>

#include <util/system/fs.h>

namespace NYT::NJobProber {

using namespace NConcurrency;
using namespace NJobProxy;
using namespace NRpc;
using namespace NTools;
using namespace NYson;
using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger JobProberLogger("JobProber");

////////////////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    TJobProberService(IJobProbePtr jobProxy, IInvokerPtr controlInvoker)
        : TServiceBase(
            controlInvoker,
            TJobProberServiceProxy::GetDescriptor(),
            JobProberLogger)
        , JobProxy_(jobProxy)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Interrupt));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GracefulAbort));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Fail));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpSensors));
    }

private:
    const TWeakPtr<IJobProbe> JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, DumpInputContext)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        context->SetRequestInfo("TransactionId: %v", transactionId);

        auto chunkIds = GetJobProxy()->DumpInputContext(transactionId);
        ToProto(response->mutable_chunk_ids(), chunkIds);

        context->SetResponseInfo("ChunkIds: %v", chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, GetStderr)
    {
        context->SetRequestInfo();

        auto stderrData = GetJobProxy()->GetStderr();
        response->set_stderr_data(stderrData);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, PollJobShell)
    {
        auto parameters = TYsonString(request->parameters());

        NJobProberClient::TJobShellDescriptor jobShellDescriptor;
        jobShellDescriptor.Subcontainer = request->subcontainer();

        context->SetRequestInfo("Parameters: %v, Subcontainer: %v",
            ConvertToYsonString(parameters, EYsonFormat::Text),
            jobShellDescriptor.Subcontainer);

        auto pollJobShellResponse = GetJobProxy()->PollJobShell(jobShellDescriptor, parameters);
        response->set_result(pollJobShellResponse.Result.ToString());
        if (pollJobShellResponse.LoggingContext) {
            response->set_logging_context(pollJobShellResponse.LoggingContext.ToString());
            context->SetResponseInfo(
                "LoggingContext: %v",
                pollJobShellResponse.LoggingContext);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, Interrupt)
    {
        Y_UNUSED(response);

        context->SetRequestInfo();

        GetJobProxy()->Interrupt();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, Fail)
    {
        Y_UNUSED(response);

        context->SetRequestInfo();

        GetJobProxy()->Fail();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, GracefulAbort)
    {
        Y_UNUSED(response);

        auto error = FromProto<TError>(request->error());

        context->SetRequestInfo("AbortError: %v", error);

        GetJobProxy()->GracefulAbort(std::move(error));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, DumpSensors)
    {
        context->SetRequestInfo();

        response->Attachments().push_back(GetJobProxy()->DumpSensors());

        context->Reply();
    }

    IJobProbePtr GetJobProxy()
    {
        auto jobProxy = JobProxy_.Lock();
        if (!jobProxy) {
            THROW_ERROR_EXCEPTION("Job is missing");
        }
        return jobProxy;
    }
};

IServicePtr CreateJobProberService(IJobProbePtr jobProbe, IInvokerPtr controlInvoker)
{
    return New<TJobProberService>(jobProbe, controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProber
