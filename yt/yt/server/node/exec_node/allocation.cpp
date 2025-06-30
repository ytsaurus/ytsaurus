#include "allocation.h"

#include "controller_agent_connector.h"
#include "job_controller.h"
#include "slot.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/library/profiling/public.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <library/cpp/yt/error/error_helpers.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NExecNode {

using namespace NYTree;
using namespace NYson;
using namespace NClusterNode;
using namespace NJobAgent;
using namespace NScheduler;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NGpu;

using TJobStartInfo = TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo;

////////////////////////////////////////////////////////////////////////////////

class TAllocationProfiler
{
public:
    TAllocationProfiler(const NProfiling::TProfiler& profiler)
        : Profiler_(profiler.WithPrefix("/allocations"))
        , AllocationJobCounts_(Profiler_
            .RateHistogram("/job_count_in_allocation", {0, 1, 3, 5, 10}))
        , SettleJobDuration_(Profiler_
            .TimeHistogram("/settle_job_duration", {
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(500),
                TDuration::Seconds(1),
                TDuration::Seconds(5),
                TDuration::Seconds(10),
            }))
            , SettlementSucceededCounters_{
                Profiler_
                    .WithTag("is_job_first", "false")
                    .Counter("/settlement_requests_succeeded"),
                Profiler_
                    .WithTag("is_job_first", "true")
                    .Counter("/settlement_requests_succeeded")
            }
    { }

    void OnAllocationFinished(int jobCount, EAllocationFinishReason finishReason)
    {
        bool isSingleJobAllocation = jobCount == 1;

        AllocationJobCounts_.Add(jobCount);

        auto* finishReasonCounter = FinishReasonCounters_[isSingleJobAllocation].Find(finishReason);
        if (!finishReasonCounter) {
            bool inserted;
            std::tie(finishReasonCounter, inserted) = FinishReasonCounters_[isSingleJobAllocation].FindOrEmplace(
                finishReason,
                Profiler_
                    .WithTag("reason", FormatEnum(finishReason))
                    .WithTag("is_single_job_allocation", std::string(FormatBool(isSingleJobAllocation)))
                    .Counter("/finished"));
        }

        YT_VERIFY(finishReasonCounter);
        finishReasonCounter->Increment();
    }

    void OnSettleJobFinished(TDuration duration, std::optional<EScheduleFailReason> failReason, bool isJobFirst)
    {
        SettleJobDuration_.Record(duration);

        if (failReason) {
            auto* failReasonCounter = ScheduleFailReasonCounters_[isJobFirst].Find(*failReason);
            if (!failReasonCounter) {
                bool inserted;
                std::tie(failReasonCounter, inserted) = ScheduleFailReasonCounters_[isJobFirst].FindOrEmplace(
                    *failReason,
                    Profiler_
                        .WithTag("fail_reason", FormatEnum(*failReason))
                        .WithTag("is_job_first", std::string(FormatBool(isJobFirst)))
                        .Counter("/settlement_requests_failed"));
            }

            YT_VERIFY(failReasonCounter);
            failReasonCounter->Increment();
        } else {
            SettlementSucceededCounters_[isJobFirst].Increment();
        }
    }

private:
    TProfiler Profiler_;

    TRateHistogram AllocationJobCounts_;
    TEventTimer SettleJobDuration_;

    std::array<TSyncMap<EScheduleFailReason, TCounter>, 2> ScheduleFailReasonCounters_{};

    std::array<TCounter, 2> SettlementSucceededCounters_{};

    std::array<TSyncMap<EAllocationFinishReason, TCounter>, 2> FinishReasonCounters_{};
};

std::optional<TAllocationProfiler> AllocationProfiler;

////////////////////////////////////////////////////////////////////////////////

namespace {

NClusterNode::TJobResources PatchJobResources(
    NClusterNode::TJobResources initial,
    const TAllocationAttributes& attributes,
    i64 minRequiredDiskSpace)
{
    const auto& diskRequest = attributes.DiskRequest;

    initial.DiskSpaceRequest = diskRequest.DiskSpace.value_or(minRequiredDiskSpace);
    initial.InodeRequest = diskRequest.InodeCount.value_or(0);

    return initial;
}

void RecalculateCpu(TNonNullPtr<NClusterNode::TJobResources> jobResources, double cpuToVCpuFactor)
{
    jobResources->VCpu = jobResources->Cpu;
    jobResources->Cpu = static_cast<double>(NVectorHdrf::TCpuResource(jobResources->VCpu / cpuToVCpuFactor));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void InitAllocationProfiler(const NProfiling::TProfiler& profiler)
{
    static TSpinLock lock;
    // This lock should protect initialization of AllocationProfiler in case of multidaemon.
    // No need to acquire lock in code that uses AllocationProfiler, since there is a happens before relation between
    // initialization of AllocationProfiler and usage of it.
    auto guard = Guard(lock);
    if (!AllocationProfiler) {
        AllocationProfiler.emplace(profiler);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAllocation::TEvent::Fire() noexcept
{
    Fired_ = true;
}

bool TAllocation::TEvent::Consume() noexcept
{
    return std::exchange(Fired_, false);
}

////////////////////////////////////////////////////////////////////////////////

TAllocation::TAllocation(
    TAllocationId id,
    TOperationId operationId,
    const NClusterNode::TJobResources& resourceDemand,
    NScheduler::TAllocationAttributes attributes,
    std::optional<TNetworkPriority> networkPriority,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap)
    : TResourceOwner(
        id.Underlying(),
        bootstrap->GetJobResourceManager().Get(),
        EResourcesConsumerType::SchedulerAllocation,
        resourceDemand)
    , Bootstrap_(bootstrap)
    , Id_(id)
    , OperationId_(operationId)
    , Logger(ExecNodeLogger().WithTag(
        "AllocationId: %v, OperationId: %v",
        id,
        operationId))
    , RequestedGpu_(resourceDemand.Gpu)
    , RequestedCpu_(resourceDemand.Cpu)
    , RequestedMemory_(resourceDemand.UserMemory)
    , InitialResourceDemand_(resourceDemand)
    , Attributes_(std::move(attributes))
    , ControllerAgentInfo_(std::move(agentDescriptor))
    , NetworkPriority_(networkPriority)
    , ControllerAgentConnector_(
        Bootstrap_->GetControllerAgentConnectorPool()->GetControllerAgentConnector(ControllerAgentInfo_.GetDescriptor()))
{
    YT_VERIFY(bootstrap);

    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
}

TAllocation::~TAllocation()
{
    YT_LOG_DEBUG("Allocation destroyed");
}

TAllocationId TAllocation::GetId() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Id_;
}

TOperationId TAllocation::GetOperationId() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return OperationId_;
}

EAllocationState TAllocation::GetState() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return State_;
}

const TError& TAllocation::GetFinishError() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return FinishError_;
}

int TAllocation::GetRequestedGpu() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return RequestedGpu_;
}

double TAllocation::GetRequestedCpu() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return RequestedCpu_;
}

i64 TAllocation::GetRequestedMemory() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return RequestedMemory_;
}

std::optional<TNetworkPriority> TAllocation::GetNetworkPriority() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return NetworkPriority_;
}

void TAllocation::Start()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    PrepareAllocation();

    // NB(eshcherbin): Do not propagate scheduler heartbeat's trace context to SettleJob.
    NTracing::TNullTraceContextGuard guard;

    SettleJob(/*isJobFirst*/ true);
}

void TAllocation::Cleanup()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(State_ == EAllocationState::Finished);
}

TJobPtr TAllocation::EvictJob() noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Job_);

    YT_LOG_DEBUG("Job evicted from allocation (JobId: %v)", Job_->GetId());

    Job_->OnEvictedFromAllocation();

    auto job = std::move(Job_);

    return job;
}

const TJobPtr& TAllocation::GetJob() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return Job_;
}

void TAllocation::UpdateControllerAgentDescriptor(TControllerAgentDescriptor descriptor)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TForbidContextSwitchGuard guard;

    if (ControllerAgentInfo_.GetDescriptor() == descriptor) {
        return;
    }

    YT_LOG_DEBUG(
        "Update controller agent for allocation (ControllerAgentAddress: %v -> %v, ControllerAgentIncarnationId: %v)",
        ControllerAgentInfo_.GetDescriptor().Address,
        descriptor.Address,
        descriptor.IncarnationId);

    ControllerAgentInfo_.SetDescriptor(std::move(descriptor));

    ControllerAgentConnector_ = Bootstrap_
        ->GetControllerAgentConnectorPool()
        ->GetControllerAgentConnector(ControllerAgentInfo_.GetDescriptor());

    if (Job_) {
        Job_->UpdateControllerAgentDescriptor(ControllerAgentInfo_.GetDescriptor());
    }
}

const TControllerAgentDescriptor& TAllocation::GetControllerAgentDescriptor() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return ControllerAgentInfo_.GetDescriptor();
}

NClusterNode::TJobResources TAllocation::GetResourceUsage(bool excludeReleasing) const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (ResourceHolder_) {
        return ResourceHolder_->GetResourceUsage(excludeReleasing);
    }

    return {};
}

void TAllocation::Abort(TError error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (std::exchange(State_, EAllocationState::Finished) != EAllocationState::Finished) {
        YT_LOG_INFO(error, "Aborting allocation (CurrentState: %v)", State_);
    } else {
        YT_LOG_DEBUG(
            "Skip allocation abortion request since allocation is already finished (AbortionError: %v)",
            error);
        return;
    }

    FinishError_ = std::move(error);

    if (Job_) {
        TransferResourcesToJob();
        auto job = EvictJob();
        job->Abort(FinishError_);
    } else {
        YT_LOG_DEBUG("Empty allocation aborted");
    }

    OnAllocationFinished(EAllocationFinishReason::Aborted);
}

void TAllocation::Complete(EAllocationFinishReason finishReason)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (std::exchange(State_, EAllocationState::Finished) != EAllocationState::Finished) {
        YT_LOG_INFO("Completing allocation");
    } else {
        YT_LOG_DEBUG("Skip allocation completion request since allocation is already finished");
        return;
    }

    State_ = EAllocationState::Finished;

    if (Job_) {
        TransferResourcesToJob();
        EvictJob();
    } else {
        YT_LOG_DEBUG("Empty allocation completed");
    }

    OnAllocationFinished(finishReason);
}

void TAllocation::Preempt(
    TDuration timeout,
    TString preemptionReason,
    const std::optional<NScheduler::TPreemptedFor>& preemptedFor)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (State_ == EAllocationState::Finished) {
        YT_LOG_DEBUG("Ignore allocation preemption request since allocation is already finished");
        return;
    } else {
        YT_LOG_INFO(
            "Preempting allocation (PreemptionReason: %v, PreemptedFor: %v, Timeout: %v)",
            preemptionReason,
            preemptedFor,
            timeout);
    }

    Preempted_ = true;

    if (!Job_) {
        YT_LOG_DEBUG("Allocation is empty; aborting it");

        auto error = TError("Allocation preempted")
            << TErrorAttribute("preemption_reason", preemptionReason)
            << TErrorAttribute("interruption_reason", EInterruptionReason::Preemption)
            << TErrorAttribute("abort_reason", EAbortReason::Preemption);

        Abort(std::move(error));
        return;
    }

    Job_->PrepareResourcesRelease();
    Job_->Interrupt(
        timeout,
        EInterruptionReason::Preemption,
        std::move(preemptionReason),
        preemptedFor);
}

bool TAllocation::IsResourceUsageOverdraftOccurred() const
{
    return GetResourceUsage().UserMemory > GetRequestedMemory();
}

bool TAllocation::IsEmpty() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return !Job_;
}

const TAllocationConfigPtr& TAllocation::GetConfig() const noexcept
{
    return Bootstrap_->GetJobController()->GetDynamicConfig()->Allocation;
}

void TAllocation::SettleJob(bool isJobFirst)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(!Job_);

    auto controllerAgentConnector = ControllerAgentConnector_.Lock();
    YT_LOG_FATAL_UNLESS(controllerAgentConnector, "Job owned by outdated controller agent");

    YT_LOG_INFO(
        "Requesting controller agent to settle new job (ControllerAgentDescriptor: %v)",
        ControllerAgentInfo_.GetDescriptor());

    TWallTimer timer;

    controllerAgentConnector->SettleJob(OperationId_, Id_, LastJobId_)
        .SubscribeUnique(BIND(&TAllocation::OnSettledJobReceived, MakeStrong(this), TWallTimer(), isJobFirst)
            .Via(Bootstrap_->GetJobInvoker()));
}

void TAllocation::OnSettledJobReceived(
    const TWallTimer& timer,
    bool isJobFirst,
    TErrorOr<TJobStartInfo>&& jobInfoOrError)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TForbidContextSwitchGuard guard;

    if (!jobInfoOrError.IsOK()) {
        auto maybeFailReason = FindAttributeRecursive<EScheduleFailReason>(jobInfoOrError, "fail_reason");
        AllocationProfiler->OnSettleJobFinished(timer.GetElapsedTime(), maybeFailReason, isJobFirst);

        if (maybeFailReason) {
            YT_LOG_INFO(
                jobInfoOrError,
                "Job was not settled in allocation; completing allocation (ScheduleJobFailReason: %v, IsJobFirst: %v)",
                *maybeFailReason,
                isJobFirst);

            Complete(EAllocationFinishReason::NoNewJobSettled);
            return;
        }


        auto error = TError("Failed to get job spec")
            << jobInfoOrError;

        YT_LOG_INFO(error, "Failed to settle job in allocation; aborting allocation");

        error <<= TErrorAttribute("abort_reason", EAbortReason::GetSpecFailed);

        Abort(std::move(error));
        return;
    }

    AllocationProfiler->OnSettleJobFinished(timer.GetElapsedTime(), std::nullopt, isJobFirst);

    auto& jobInfo = jobInfoOrError.Value();

    if (State_ == EAllocationState::Finished) {
        // Job will be aborted by controller agent in this case.

        YT_LOG_INFO(
            "Received settled job for aborted allocation; ignore it (JobId: %v, IsJobFirst: %v)",
            jobInfo.JobId,
            isJobFirst);
        return;
    }

    YT_VERIFY(State_ == EAllocationState::Running || State_ == EAllocationState::Waiting);

    YT_LOG_DEBUG("Creating and settling job (JobId: %v)", jobInfo.JobId);

    auto resourceLimits = YT_OPTIONAL_FROM_PROTO(jobInfo.JobSpec, resource_limits);
    auto jobNumber = TotalJobCount_;

    try {
        CreateAndSettleJob(jobInfo.JobId, std::move(jobInfo.JobSpec));
    } catch (const std::exception& ex) {
        TError error(ex);
        YT_LOG_WARNING(
            error,
            "Failed to create job (JobId: %v, OperationId: %v)",
            jobInfo.JobId,
            OperationId_);

        Abort(TError("Failed to create job") << error);
    } catch (...) {
        YT_LOG_FATAL(
            "Unexpected failure during job creation (JobId: %v, OperationId: %v)",
            jobInfo.JobId,
            OperationId_);
    }

    YT_VERIFY(ResourceHolder_);
    // We are making efforts to ensure that the node retains the cpuToVcpuFactor
    // from the moment the heartbeat is sent to the shader until the response is received,
    // so as not to receive an allocationsToStart with a cpu overcommit.
    // So we do not update first job resources.
    if (jobNumber != 0) {
        // COMPAT(pogorelov): Remove this after all CAs are 25.2.
        if (resourceLimits) {
            auto newDemand = FromNodeResources(*resourceLimits);
            RecalculateCpu(GetPtr(newDemand), Bootstrap_->GetJobResourceManager()->GetCpuToVCpuFactor());
            YT_LOG_INFO("Got new demand for allocation, setting it (ResourceLimits: %v)", newDemand);
            if (!ResourceHolder_->TrySetBaseResourceUsage(newDemand)) {
                YT_LOG_DEBUG(
                    "Failed to set new job resources to allocation; Aborting allocation (PreviousResourceUsage: %v, NewResourceUsage: %v)",
                    ResourceHolder_->GetResourceUsage(),
                    newDemand);
                Abort(TError("Failed to set new job resources to allocation")
                    << TErrorAttribute("new_job_resources", newDemand)
                    << TErrorAttribute("previous_job_resources", ResourceHolder_->GetResourceUsage())
                    << TErrorAttribute("abort_reason", EAbortReason::NodeResourceOvercommit));
                return;
            }
        } else {
            YT_LOG_INFO(
                "No new demand received in spec, restoring resources to initial demand (InitialDemand: %v)",
                InitialResourceDemand_);
            ResourceHolder_->RestoreResources();
        }
    }

    YT_LOG_DEBUG("Resources reset; starting job (JobId: %v)", jobInfo.JobId);
    if (State_ == EAllocationState::Running) {
        Job_->Start();
    }
}

void TAllocation::CreateAndSettleJob(
    TJobId jobId,
    NControllerAgent::NProto::TJobSpec&& jobSpec)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    const auto& resourceHolder = GetResourceHolder();
    if (auto state = resourceHolder->GetState(); state == EResourcesState::Acquired) {
        StaticPointerCast<IUserSlot>(GetResourceHolder()->GetUserSlot())->Prepare();
    } else {
        YT_VERIFY(state == EResourcesState::Pending);
    }

    auto job = CreateJob(
        jobId,
        OperationId_,
        MakeStrong(this),
        std::move(jobSpec),
        ControllerAgentInfo_.GetDescriptor(),
        Bootstrap_,
        Bootstrap_->GetJobController()->GetDynamicConfig()->JobCommon);

    job->SubscribeJobPrepared(
        BIND_NO_PROPAGATE(&TAllocation::OnJobPrepared, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));
    job->SubscribeJobFinished(
        BIND_NO_PROPAGATE(&TAllocation::OnJobFinished, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));

    YT_VERIFY(!std::exchange(Job_, std::move(job)));

    LastJobId_ = jobId;

    ++TotalJobCount_;

    JobSettled_.Fire(Job_);

    YT_LOG_INFO(
        "Job created (JobId: %v, OperationId: %v)",
        jobId,
        OperationId_);
}

void TAllocation::OnResourcesAcquired() noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Resources acquired; starting job");

    State_ = EAllocationState::Running;

    // NB(arkady-e1ppa): In non-legacy version of
    // allocation preparation resources are acquired
    // immediately. That is, before the spec
    // was received and job created. Thus,
    // we will simply start the job when it
    // is created.
    if (Job_) {
        Job_->Start();
    }
}

const NJobAgent::TResourceHolderPtr& TAllocation::GetResourceHolder() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(ResourceHolder_);

    return ResourceHolder_;
}

IYPathServicePtr TAllocation::GetOrchidService()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto jobService =  New<TCompositeMapService>();

    if (Job_) {
        jobService->AddChild("job", Job_->GetOrchidService());
    }

    auto staticAllocationService = GetStaticOrchidService();

    return New<TServiceCombiner>(
        std::vector<IYPathServicePtr>{
            std::move(jobService),
            std::move(staticAllocationService),
        });
}

NYTree::IYPathServicePtr TAllocation::GetStaticOrchidService()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return IYPathService::FromProducer(BIND([this, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
            auto [baseResourceUsage, additionalResourceUsage] = ResourceHolder_
                ? ResourceHolder_->GetDetailedResourceUsage()
                : std::make_pair(NClusterNode::TJobResources(), NClusterNode::TJobResources());
            BuildYsonFluently(consumer).BeginMap()
                .Item("initial_resource_demand").Value(InitialResourceDemand_)
                .Item("base_resource_usage").Value(baseResourceUsage)
                .Item("additional_resource_usage").Value(additionalResourceUsage)
                .Item("state").Value(State_)
                .Item("last_job_id").Value(LastJobId_)
                .Item("operation_id").Value(OperationId_)
            .EndMap();
        }))->Via(Bootstrap_->GetJobInvoker());
}

bool TAllocation::IsRunning() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return State_ == EAllocationState::Running;
}

bool TAllocation::IsFinished() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return State_ == EAllocationState::Finished;
}

void TAllocation::AbortJob(TError error, bool graceful, bool requestNewJob)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Job_->Abort(std::move(error), graceful);
    if (requestNewJob) {
        YT_LOG_DEBUG("Requested to abort job and settle new one");
        SettlementNewJobOnAbortRequested_.Fire();
    }
}

void TAllocation::InterruptJob(NScheduler::EInterruptionReason interruptionReason, TDuration interruptionTimeout)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Job_->Interrupt(
        interruptionTimeout,
        interruptionReason,
        /*preemptionReason*/ std::nullopt,
        /*preemptedFor*/ std::nullopt);
}

void TAllocation::OnAllocationFinished(EAllocationFinishReason finishReason)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(!Job_);

    AllocationFinished_.Fire(MakeStrong(this));

    if (ResourceHolder_) {
        ResourceHolder_->ResetOwner({});
    }

    ResourceHolder_.Reset();

    AllocationProfiler->OnAllocationFinished(TotalJobCount_, finishReason);
}

void TAllocation::OnJobPrepared(TJobPtr job)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    JobPrepared_.Fire(std::move(job));
}

void TAllocation::OnJobFinished(TJobPtr job)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto settlementNewJobOnJobAbortRequested = SettlementNewJobOnAbortRequested_.Consume();

    std::optional<EAllocationFinishReason> finishReason;

    auto settleNewJob = [&] {
        if (Preempted_) {
            YT_LOG_INFO(
                "Job finished and allocation is preempted, completing allocation (JobId: %v)",
                job->GetId());
            finishReason = EAllocationFinishReason::Preempted;
            return false;
        }

        bool enableMultipleJobs = GetConfig()->EnableMultipleJobs && Attributes_.EnableMultipleJobs;

        if (enableMultipleJobs && job->GetState() == EJobState::Completed) {
            YT_LOG_INFO(
                "Job completed and multiple jobs in allocation enabled, waiting for storing and clenup job to settle new one (JobId: %v)",
                job->GetId());
            return true;
        }

        if (settlementNewJobOnJobAbortRequested && job->GetState() == EJobState::Aborted) {
            YT_LOG_INFO(
                "Job aborted and new job settlement requested, waiting for storing and clenup job to settle new one (JobId: %v)",
                job->GetId());
            return true;
        }

        YT_LOG_INFO(
            "Job finished, new job settlement disabled; completing allocation (JobId: %v)",
            job->GetId());

        if (!enableMultipleJobs) {
            finishReason = EAllocationFinishReason::MultipleJobsDisabled;
            return false;
        }

        YT_VERIFY(job->IsFinishedUnsuccessfully());
        finishReason = EAllocationFinishReason::JobFinishedUnsuccessfully;

        return false;
    }();

    if (settleNewJob) {
        AllSet(std::vector{job->GetCleanupFinishedEvent(), job->GetStoredEvent()})
            .SubscribeUnique(BIND([
                jobId = job->GetId(),
                this,
                this_ = MakeStrong(this)
            ] (const TError& error) {
                TForbidContextSwitchGuard guard;
                YT_LOG_FATAL_UNLESS(
                    error.IsOK(),
                    error,
                    "Failed to store or cleanup job (JobId: %v)",
                    jobId);

                if (State_ == EAllocationState::Finished) {
                    YT_LOG_INFO(
                        "Controller agent requested to settle job but allocation is already finished, settling job skipped (JobId: %v)",
                        jobId);
                    return;
                }

                if (Preempted_) {
                    YT_LOG_INFO(
                        "Controller agent requested to settle job but allocation is preempted, settling job skipped (JobId: %v)",
                        jobId);

                    Complete(EAllocationFinishReason::Preempted);

                    return;
                }

                if (!ControllerAgentInfo_.GetDescriptor()) {
                    YT_LOG_INFO(
                        "Allocation is not assigned to controller agent, skip new job settlement (JobId: %v)",
                        jobId);

                    Complete(EAllocationFinishReason::AgentDisconnected);

                    return;
                }

                YT_LOG_INFO(
                    "Job cleanup finished and job stored; evicting previous and settling new job (PreviousJobId: %v)",
                    jobId);

                EvictJob();
                SettleJob(/*isJobFirst*/ false);
            })
                .Via(Bootstrap_->GetJobInvoker()));
    } else {
        YT_VERIFY(finishReason);
        Complete(*finishReason);
    }

    JobFinished_.Fire(std::move(job));
}

void TAllocation::TransferResourcesToJob()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Job_);

    OnResourcesTransferred();
}

void TAllocation::PrepareAllocation()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto jobControllerConfig = Bootstrap_->GetJobController()->GetDynamicConfig();
    auto resources = PatchJobResources(
        GetResourceUsage(),
        Attributes_,
        jobControllerConfig->MinRequiredDiskSpace);

    ResourceHolder_->UpdateResourceDemand(
        resources,
        Attributes_);

    AllocationPrepared_.Fire(
        MakeStrong(this),
        Attributes_.WaitingForResourcesOnNodeTimeout.value_or(jobControllerConfig->WaitingForResourcesTimeout));
}

TAllocationPtr CreateAllocation(
    TAllocationId id,
    TOperationId operationId,
    const NClusterNode::TJobResources& resourceDemand,
    NScheduler::TAllocationAttributes attributes,
    std::optional<TNetworkPriority> networkPriority,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap)
{
    return NewWithOffloadedDtor<TAllocation>(
        bootstrap->GetJobInvoker(),
        id,
        operationId,
        resourceDemand,
        std::move(attributes),
        networkPriority,
        std::move(agentDescriptor),
        bootstrap);
}

void FillStatus(NScheduler::NProto::TAllocationStatus* status, const TAllocationPtr& allocation)
{
    using NYT::ToProto;

    ToProto(status->mutable_allocation_id(), allocation->GetId());

    status->set_state(ToProto(allocation->GetState()));

    ToProto(status->mutable_operation_id(), allocation->GetOperationId());

    status->set_status_timestamp(ToProto(TInstant::Now()));

    ToProto(status->mutable_result()->mutable_error(), allocation->GetFinishError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
