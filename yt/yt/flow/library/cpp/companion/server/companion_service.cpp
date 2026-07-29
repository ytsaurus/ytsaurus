#include "companion_service.h"

#include "job.h"
#include "job_registry.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/core/concurrency/context_switch.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <util/system/datetime.h>

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
        NCompanion::TCompanionExecutionConfigPtr config,
        IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            NCompanion::TCompanionProxy::GetDescriptor(),
            CompanionServerLogger())
        , Pipeline_(std::move(pipeline))
        , CompanionInfoPayload_(Pipeline_.BuildCompanionInfoPayload())
        , JobRegistry_(New<TJobRegistry>(
            TDuration::Seconds(config->JobTtlSeconds),
            GetDefaultInvoker()))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProcessBatch));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CompanionInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJfr));
    }

private:
    const TPipeline Pipeline_;
    const NYson::TYsonString CompanionInfoPayload_;
    const TJobRegistryPtr JobRegistry_;

    TJobPtr CreateJob(
        const TJobId& jobId,
        const TComputationId& computationId,
        const NProto::NCompanion::TJobInfo& jobInfo)
    {
        THROW_ERROR_EXCEPTION_UNLESS(Pipeline_.HasComputation(computationId),
            "Computation %Qv is not registered in this companion",
            computationId);
        return New<TJob>(jobId, computationId, jobInfo);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, ProcessBatch);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, CompanionInfo);
    DECLARE_RPC_SERVICE_METHOD(NProto::NCompanion, PutJob);
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

    *response->mutable_request_id() = request->request_id();
    *response->mutable_job_id() = request->job_id();

    InitializeResponseMetrics(response->mutable_metrics());

    if (request->has_job_info()) {
        JobRegistry_->PutJob(CreateJob(jobId, computationId, request->job_info()));
    }
    auto execution = JobRegistry_->AcquireJob(jobId);
    if (!execution) {
        // The worker retries with the job info attached.
        response->set_status(NProto::NCompanion::RS_JOB_NOT_FOUND);
        context->Reply();
        return;
    }

    // Count queued callbacks too: a timed-out RPC may be retried while its
    // original handler is still running, and both must keep using the same
    // serializing invoker even when the job TTL elapses.
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
    auto cpuTimeNs = NConcurrency::WaitFor(
        BIND([job = execution->Job, request, data = response->mutable_data()] {
            return RunWithCpuAccounting([&] {
                job->ProcessBatch(*request, data);
            });
        })
            .AsyncVia(execution->Invoker)
            .Run())
        .ValueOrThrow();
    response->mutable_metrics()->set_cpu_time_ns(cpuTimeNs);
    response->set_status(NProto::NCompanion::RS_OK);

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

    *response->mutable_request_id() = request->request_id();
    *response->mutable_job_id() = request->job_id();

    InitializeResponseMetrics(response->mutable_metrics());
    JobRegistry_->PutJob(CreateJob(jobId, computationId, request->job_info()));
    response->set_status(NProto::NCompanion::RS_OK);

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
    NCompanion::TCompanionExecutionConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TCompanionService>(
        std::move(pipeline),
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
