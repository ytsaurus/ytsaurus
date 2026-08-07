#include "companion_service.h"

#include "job.h"
#include "job_registry.h"
#include "resource_store.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/core/concurrency/context_switch.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <util/system/datetime.h>
#include <util/system/getpid.h>

namespace NYT::NFlow::NCompanionServer {

using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

void InitializeResponseMetrics(NProto::NCompanion::TResponseMetrics* metrics)
{
    // Parity with the Python companion, whose reporter also leaves this zero.
    metrics->set_allocated_bytes(0);
    // ProcessBatch fills the CPU time measured around the batch on success.
    metrics->set_cpu_time_ns(0);
}

//! Result of one serialized batch invocation.
struct TBatchOutcome
{
    NProto::NCompanion::EResponseStatus Status = NProto::NCompanion::RS_OK;
    ui64 CpuTimeNs = 0;
};

//! Runs |callback| on the current fiber and returns the CPU time it consumed,
//! in nanoseconds, accounted across suspensions. The checkpoint/guard pattern
//! follows worker/traced_invoker.cpp: a resumed fiber runs on a single OS
//! thread until it next suspends, so the checkpoint (set at start/resume) and
//! the delta (taken at suspend/finish) always read the same thread's clock.
ui64 RunWithCpuAccounting(const std::function<void()>& callback)
{
    ui64 cpuTimeMicros = 0;
    ui64 checkpoint = ThreadCPUTime();
    auto accumulate = [&] {
        auto now = ThreadCPUTime();
        cpuTimeMicros += now - checkpoint;
        checkpoint = now;
    };
    NConcurrency::TContextSwitchGuard cpuTimeGuard(
        /*out*/ accumulate,
        /*in*/ [&] {
            checkpoint = ThreadCPUTime();
        });
    callback();
    accumulate();
    return cpuTimeMicros * 1000;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCompanionService
    : public TServiceBase
{
public:
    TCompanionService(
        TPipeline pipeline,
        IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            NCompanion::TCompanionProxy::GetDescriptor(),
            CompanionServerLogger())
        , Pipeline_(std::move(pipeline))
        , CompanionInfoPayload_(Pipeline_.BuildCompanionInfoPayload())
        , JobRegistry_(New<TJobRegistry>(GetDefaultInvoker()))
        , ResourceStore_(New<TResourceStore>(
            Pipeline_.GetResourceClassNames(),
            GetDefaultInvoker()))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProcessBatch));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CompanionInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListJobs));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResourceExecute));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJfr));
    }

private:
    const TPipeline Pipeline_;
    const NYson::TYsonString CompanionInfoPayload_;
    const TJobRegistryPtr JobRegistry_;
    const TResourceStorePtr ResourceStore_;

    TJobPtr CreateJob(
        const TJobId& jobId,
        const TComputationId& computationId,
        const NProto::NCompanion::TJobInfo& jobInfo)
    {
        THROW_ERROR_EXCEPTION_UNLESS(Pipeline_.HasComputation(computationId),
            "Computation %Qv is not registered in this companion",
            computationId);
        return New<TJob>(jobId, computationId, jobInfo, ResourceStore_);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, ProcessBatch);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, CompanionInfo);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, PutJob);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, RemoveJob);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, ListJobs);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, ResourceExecute);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, GetJfr);
};

DEFINE_RPC_SERVICE_METHOD(TCompanionService, ProcessBatch)
{
    auto jobId = FromProto<TJobId>(request->job_id());
    auto computationId = TComputationId(request->computation_id());
    context->SetRequestInfo("JobId: %v, ComputationId: %v, MessageCount: %v, TimerCount: %v",
        jobId,
        computationId,
        request->messages_size(),
        request->timers_size());

    // An abandoned request must not register a job nobody will remove, and
    // there is no point running the batch for it either.
    if (context->IsCanceled()) {
        context->Reply(TError(NYT::EErrorCode::Canceled, "Request is canceled"));
        return;
    }

    *response->mutable_request_id() = request->request_id();
    *response->mutable_job_id() = request->job_id();

    InitializeResponseMetrics(response->mutable_metrics());

    if (request->has_job_info()) {
        JobRegistry_->PutJob(CreateJob(jobId, computationId, request->job_info()));
    }
    auto execution = JobRegistry_->AcquireJob(jobId);
    if (!execution) {
        // The worker retries with the job info attached: this process was
        // restarted, or a re-forked fan-out sibling is serving the channel.
        response->set_status(NProto::NCompanion::RS_JOB_NOT_FOUND);
        context->Reply();
        return;
    }

    // Count queued callbacks too: a timed-out RPC may be retried while its
    // original handler is still running, and both must keep using the same
    // serializing invoker.
    auto releaseGuard = Finally([&] {
        JobRegistry_->ReleaseJob(jobId);
    });

    // Run on the per-job-id invoker: it admits one batch at a time
    // (a plain mutex would be held across user code that may WaitFor
    // and migrate threads) and, unlike anything owned by the job
    // instance, survives job replacement by a retry carrying job info.
    // NB: User-code and decode failures escape as RPC errors (parity with
    // the Java and Python companions): the worker absorbs them with retries
    // and sees the error text; only RS_JOB_NOT_FOUND stays in-band.
    // The CPU time is measured inside the serialized callback, so an
    // overlapping retry cannot skew it and the value is complete before
    // the batch future is set.
    auto outcome = NConcurrency::WaitFor(
        BIND([
            job = execution->Job,
            resourceStore = ResourceStore_,
            jobId,
            Logger = Logger,
            request,
            data = response->mutable_data()
        ] () -> TBatchOutcome {
            // Validate exact references after this batch reaches the head of the
            // per-job queue. A lifecycle command may advance a resource while
            // the batch waits behind an earlier invocation.
            std::vector<TResourceId> uninitializedResourceIds;
            for (const auto& reference : resourceStore->FindUninitialized(
                job->GetCompanionResources()))
            {
                uninitializedResourceIds.push_back(reference.ResourceId);
            }
            if (!uninitializedResourceIds.empty()) {
                YT_TLOG_DEBUG("Companion resources are not initialized, rejecting batch")
                    .With("JobId", jobId)
                    .With("ResourceIds", uninitializedResourceIds);
                return {.Status = NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED};
            }

            bool processed = false;
            auto cpuTimeNs = RunWithCpuAccounting([&] {
                processed = job->ProcessBatch(*request, data);
            });
            if (!processed) {
                // A lifecycle command advanced a resource between the check
                // above and the job's own acquisition.
                YT_TLOG_DEBUG("Companion resources are not initialized, rejecting batch")
                    .With("JobId", jobId);
                return {.Status = NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED};
            }
            return {.Status = NProto::NCompanion::RS_OK, .CpuTimeNs = cpuTimeNs};
        })
            .AsyncVia(execution->Invoker)
            .Run())
        .ValueOrThrow();
    response->set_status(outcome.Status);
    if (outcome.Status == NProto::NCompanion::RS_OK) {
        response->mutable_metrics()->set_cpu_time_ns(outcome.CpuTimeNs);
    }

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TCompanionService, CompanionInfo)
{
    context->SetRequestInfo();

    response->set_payload(CompanionInfoPayload_.ToString());
    response->set_status(NProto::NCompanion::RS_OK);
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TCompanionService, PutJob)
{
    auto jobId = FromProto<TJobId>(request->job_id());
    auto computationId = TComputationId(request->computation_id());
    context->SetRequestInfo("JobId: %v, ComputationId: %v",
        jobId,
        computationId);

    // An abandoned request must not register a job nobody will remove.
    if (context->IsCanceled()) {
        context->Reply(TError(NYT::EErrorCode::Canceled, "Request is canceled"));
        return;
    }

    *response->mutable_request_id() = request->request_id();
    *response->mutable_job_id() = request->job_id();

    InitializeResponseMetrics(response->mutable_metrics());
    JobRegistry_->PutJob(CreateJob(jobId, computationId, request->job_info()));
    response->set_status(NProto::NCompanion::RS_OK);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TCompanionService, RemoveJob)
{
    auto jobId = FromProto<TJobId>(request->job_id());
    context->SetRequestInfo("JobId: %v", jobId);

    *response->mutable_request_id() = request->request_id();
    *response->mutable_job_id() = request->job_id();

    JobRegistry_->RemoveJob(jobId);
    response->set_status(NProto::NCompanion::RS_OK);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TCompanionService, ListJobs)
{
    context->SetRequestInfo();

    *response->mutable_request_id() = request->request_id();
    ToProto(response->mutable_job_ids(), JobRegistry_->ListJobIds());
    response->set_process_id(GetPID());
    response->set_status(NProto::NCompanion::RS_OK);

    context->SetResponseInfo("JobCount: %v", response->job_ids_size());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TCompanionService, ResourceExecute)
{
    auto resourceId = TResourceId(request->resource_id());
    auto command = static_cast<NCompanion::ECompanionResourceCommand>(request->command());
    context->SetRequestInfo("ResourceId: %v, Command: %v",
        resourceId,
        command);

    *response->mutable_request_id() = request->request_id();

    auto argument = request->has_argument()
        ? NYson::TYsonString(FromProto<TString>(request->argument()))
        : NYson::TYsonString();

    // The store maps user-code failures to in-band statuses; the future fails
    // only on companion bugs, surfacing as an RPC error the worker retries.
    auto outcome = NConcurrency::WaitFor(
        ResourceStore_->Execute(resourceId, command, argument))
        .ValueOrThrow();

    response->set_status(static_cast<NProto::NCompanion::EResourceExecuteStatus>(outcome.Status));
    if (!outcome.Error.IsOK()) {
        ToProto(response->mutable_error(), outcome.Error);
    }
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TCompanionService, GetJfr)
{
    context->SetRequestInfo();

    response->set_status(NProto::NCompanion::RS_ERROR);
    response->set_error_message("JFR is not supported by C++ companion");
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCompanionService(
    TPipeline pipeline,
    IInvokerPtr invoker)
{
    return New<TCompanionService>(
        std::move(pipeline),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
