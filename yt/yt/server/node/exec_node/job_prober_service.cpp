#include "job_prober_service.h"

#include "bootstrap.h"
#include "job.h"
#include "job_controller.h"
#include "job_proxy_log_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/job_prober_client/job_prober_service_proxy.h>
#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NExecNode {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NJobAgent;
using namespace NYson;
using namespace NScheduler;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    explicit TJobProberService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetJobInvoker(),
            TJobProberServiceProxy::GetDescriptor(),
            ExecNodeLogger(),
            TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetFailContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell)
            .SetInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Interrupt));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Abort));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpJobProxyLog));

        YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
    }

private:
    IBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        context->SetRequestInfo("JobId: %v, TransactionId: %v", jobId, transactionId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto chunkIds = job->DumpInputContext(transactionId);

        context->SetResponseInfo("ChunkIds: %v", chunkIds);
        ToProto(response->mutable_chunk_ids(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetStderr)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v, Limit: %v, Offset: %v", jobId, request->limit(), request->offset());

        auto job = Bootstrap_->GetJobController()->FindRecentlyRemovedJob(jobId);
        if (!job) {
            job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        }

        auto rsp = job->GetStderr({.Limit = request->limit(), .Offset = request->offset()})
                            .value_or(NApi::TGetJobStderrResponse{});
        response->set_stderr_data(rsp.Data.data(), rsp.Data.size());
        response->set_total_size(rsp.TotalSize);
        response->set_end_offset(rsp.EndOffset);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetFailContext)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->FindRecentlyRemovedJob(jobId);
        if (!job) {
            job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        }

        auto failContextData = job->GetFailContext();

        response->set_fail_context_data(failContextData.value_or(TString()));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetSpec)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->FindRecentlyRemovedJob(jobId);
        if (!job) {
            job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        }
        response->mutable_spec()->CopyFrom(job->GetSpec());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, PollJobShell)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto jobId = FromProto<TJobId>(request->job_id());
        auto parameters = TYsonString(request->parameters());
        auto subcontainer = request->subcontainer();
        TJobShellDescriptor jobShellDescriptor;
        jobShellDescriptor.Subcontainer = subcontainer;

        context->SetRequestInfo(
            "JobId: %v, Subcontainer: %v",
            jobId,
            subcontainer);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto pollShellResponse = job->PollJobShell(jobShellDescriptor, parameters);

        response->set_result(pollShellResponse.Result.ToString());
        if (pollShellResponse.LoggingContext) {
            response->set_logging_context(pollShellResponse.LoggingContext.ToString());
            context->SetResponseInfo("LoggingContext: %v", pollShellResponse.LoggingContext);
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Interrupt)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());

        auto timeout = FromProto<TDuration>(request->timeout());

        context->SetRequestInfo(
            "JobId: %v, InterruptionTimeout: %v",
            jobId, timeout);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);

        if (!job->IsInterruptible()) {
            THROW_ERROR_EXCEPTION(
                "Cannot interrupt job %v of type %Qlv "
                "because it does not support interruption or \"interruption_signal\" is not set",
                jobId,
                job->GetType());
        }

        EInterruptionReason interruptionReason = EInterruptionReason::None;
        if (request->has_interruption_reason()) {
            interruptionReason = FromProto<EInterruptionReason>(request->interruption_reason());
        }

        job->Interrupt(
            timeout,
            interruptionReason,
            /*preemptionReason*/ {},
            /*preemptedFor*/ {});

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Abort)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto error = FromProto<TError>(request->error());

        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        job->Abort(error);

        if (job->GetPhase() < EJobPhase::WaitingForCleanup) {
            THROW_ERROR_EXCEPTION("Failed to abort job %v", jobId)
                << TErrorAttribute("job_state", job->GetState())
                << TErrorAttribute("job_phase", job->GetPhase());
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpJobProxyLog)
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto path = FromProto<NYPath::TYPath>(request->path());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo(
            "JobId: %v, Path: %v, TransactionId: %v",
            jobId,
            path,
            transactionId);

        const auto& jobProxyLogManager = Bootstrap_->GetJobProxyLogManager();
        if (!jobProxyLogManager) {
            THROW_ERROR_EXCEPTION(
                "Job proxy log manager is disabled")
                << TErrorAttribute("job_proxy_logging_mode", Bootstrap_->GetConfig()->ExecNode->JobProxy->JobProxyLogging->Mode);
        }

        WaitFor(jobProxyLogManager->DumpJobProxyLog(jobId, path, transactionId))
            .ThrowOnError();

        context->Reply();
    }
};

IServicePtr CreateJobProberService(IBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
