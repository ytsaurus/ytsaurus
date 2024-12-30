#include "allocation.h"

#include "controller_agent_connector.h"
#include "job_controller.h"
#include "slot.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

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

using TJobStartInfo = TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo;

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

TAllocationAttributes BuildAttributesFromJobSpec(const TJobSpecExt* jobSpecExt)
{
    const auto& userJobSpec = jobSpecExt->user_job_spec();
    TAllocationAttributes attributes{
        .DiskRequest = {
            .InodeCount = userJobSpec.disk_request().inode_count(),
        },
        .AllowIdleCpuPolicy = jobSpecExt->allow_idle_cpu_policy(),
        .PortCount = userJobSpec.port_count(),
    };
    if (userJobSpec.disk_request().has_medium_index()) {
        attributes.DiskRequest.MediumIndex = userJobSpec.disk_request().medium_index();
    }

    if (userJobSpec.has_cuda_toolkit_version()) {
        attributes.CudaToolkitVersion = userJobSpec.cuda_toolkit_version();
    }
    if (jobSpecExt->has_waiting_job_timeout()) {
        attributes.WaitingForResourcesOnNodeTimeout = FromProto<TDuration>(jobSpecExt->waiting_job_timeout());
    }
    return attributes;
}

} // namespace

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
    std::optional<NScheduler::TAllocationAttributes> attributes,
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
    , ControllerAgentDescriptor_(std::move(agentDescriptor))
    , ControllerAgentConnector_(
        Bootstrap_->GetControllerAgentConnectorPool()->GetControllerAgentConnector(ControllerAgentDescriptor_))
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

void TAllocation::Start()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    if (Attributes_) {
        PrepareAllocationFromAttributes(*Attributes_);
    }

    // NB(eshcherbin): Do not propagate scheduler heartbeat's trace context to SettleJob.
    NTracing::TNullTraceContextGuard guard;

    SettleJob();
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

    if (ControllerAgentDescriptor_ == descriptor) {
        return;
    }

    YT_LOG_DEBUG(
        "Update controller agent for allocation (ControllerAgentAddress: %v -> %v, ControllerAgentIncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        descriptor.Address,
        descriptor.IncarnationId);

    ControllerAgentDescriptor_ = std::move(descriptor);

    ControllerAgentConnector_ = Bootstrap_
        ->GetControllerAgentConnectorPool()
        ->GetControllerAgentConnector(ControllerAgentDescriptor_);

    if (Job_) {
        Job_->UpdateControllerAgentDescriptor(ControllerAgentDescriptor_);
    }
}

const TControllerAgentDescriptor& TAllocation::GetControllerAgentDescriptor() const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return ControllerAgentDescriptor_;
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

    OnAllocationFinished();
}

void TAllocation::Complete()
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

    OnAllocationFinished();
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
            << TErrorAttribute("interruption_reason", EInterruptReason::Preemption)
            << TErrorAttribute("abort_reason", EAbortReason::Preemption);

        Abort(std::move(error));
        return;
    }

    Job_->PrepareResourcesRelease();
    Job_->Interrupt(
        timeout,
        EInterruptReason::Preemption,
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

void TAllocation::SettleJob()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(!Job_);

    auto controllerAgentConnector = ControllerAgentConnector_.Lock();
    YT_LOG_FATAL_UNLESS(controllerAgentConnector, "Job owned by outdated controller agent");

    YT_LOG_INFO(
        "Requesting controller agent to settle new job (ControllerAgentDescriptor: %v)",
        ControllerAgentDescriptor_);

    controllerAgentConnector->SettleJob(OperationId_, Id_, LastJobId_)
        .SubscribeUnique(BIND(&TAllocation::OnSettledJobReceived, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));
}

void TAllocation::OnSettledJobReceived(
    TErrorOr<TJobStartInfo>&& jobInfoOrError)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    TForbidContextSwitchGuard guard;

    if (!jobInfoOrError.IsOK()) {
        if (auto maybeFailReason = FindAttributeRecursive<EScheduleFailReason>(jobInfoOrError, "fail_reason")) {
            YT_LOG_INFO(
                jobInfoOrError,
                "Job was not settled in allocation; completing allocation (ScheduleJobFailReason: %v)",
                *maybeFailReason);

            Complete();
            return;
        }

        auto error = TError("Failed to get job spec")
            << jobInfoOrError;

        YT_LOG_INFO(error, "Failed to settle job in allocation; aborting allocation");

        error <<= TErrorAttribute("abort_reason", EAbortReason::GetSpecFailed);

        Abort(std::move(error));
        return;
    }

    auto& jobInfo = jobInfoOrError.Value();

    if (State_ == EAllocationState::Finished) {
        // Job will be aborted by controller agent in this case.

        YT_LOG_INFO("Received settled job for aborted allocation; ignore it (JobId: %v)", jobInfo.JobId);
        return;
    }

    // NB(arkady-e1ppa): Waiting is legacy. Remove when
    // sched and CA are updated to 24.2.
    YT_VERIFY(
        State_ == EAllocationState::Waiting ||
        State_ == EAllocationState::Running);

    if (!Attributes_.has_value()) {
        LegacyPrepareAllocationFromStartInfo(jobInfo);
    }

    YT_VERIFY(ResourceHolder_);
    ResourceHolder_->RestoreResources();

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
        ControllerAgentDescriptor_,
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

    // COMPAT(arkady-e1ppa): Non-legacy version
    // always has state == Running at this point.
    // Remove branch when sched and CA are 24.2.
    if (State_ == EAllocationState::Running) {
        Job_->Start();
    }

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

    auto service = New<TServiceCombiner>(
        std::vector<IYPathServicePtr>{
            std::move(jobService),
            std::move(staticAllocationService)});

    return service;
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

void TAllocation::InterruptJob(NScheduler::EInterruptReason interruptionReason, TDuration interruptionTimeout)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    Job_->Interrupt(
        interruptionTimeout,
        interruptionReason,
        /*preemptionReason*/ std::nullopt,
        /*preemptedFor*/ std::nullopt);
}

void TAllocation::OnAllocationFinished()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(!Job_);

    AllocationFinished_.Fire(MakeStrong(this));

    if (ResourceHolder_) {
        ResourceHolder_->ResetOwner({});
    }

    ResourceHolder_.Reset();
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

    auto settleNewJob = [&] {
        if (Preempted_) {
            YT_LOG_INFO(
                "Job finished and allocation is preempted, completing allocation (JobId: %v)",
                job->GetId());
            return false;
        }

        if (GetConfig()->EnableMultipleJobs && job->GetState() == EJobState::Completed) {
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

                    Complete();

                    return;
                }

                if (!ControllerAgentDescriptor_) {
                    YT_LOG_INFO(
                        "Allocation is not assigned to controller agent, skip new job settlement (JobId: %v)",
                        jobId);

                    Complete();

                    return;
                }

                YT_LOG_INFO(
                    "Job cleanup finished and job stored; evicting previous and settling new job (PreviousJobId: %v)",
                    jobId);

                EvictJob();
                SettleJob();
            })
                .Via(Bootstrap_->GetJobInvoker()));
    } else {
        Complete();
    }

    JobFinished_.Fire(std::move(job));
}

void TAllocation::TransferResourcesToJob()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    YT_VERIFY(Job_);

    OnResourcesTransferred();
}

void TAllocation::PrepareAllocationFromAttributes(
    const NScheduler::TAllocationAttributes& attributes)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto jobControllerConfig = Bootstrap_->GetJobController()->GetDynamicConfig();
    auto resources = PatchJobResources(
        GetResourceUsage(),
        attributes,
        jobControllerConfig->MinRequiredDiskSpace);

    ResourceHolder_->UpdateResourceDemand(
        resources,
        attributes);

    AllocationPrepared_.Fire(
        MakeStrong(this),
        attributes
            .WaitingForResourcesOnNodeTimeout
            .value_or(jobControllerConfig->WaitingForResourcesTimeout));
}

void TAllocation::LegacyPrepareAllocationFromStartInfo(TJobStartInfo& jobInfo)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto jobSpecExtId = TJobSpecExt::job_spec_ext;
    YT_VERIFY(jobInfo.JobSpec.HasExtension(jobSpecExtId));
    auto* jobSpecExt = &jobInfo.JobSpec.GetExtension(jobSpecExtId);

    auto attributes = BuildAttributesFromJobSpec(jobSpecExt);
    PrepareAllocationFromAttributes(attributes);
}

TAllocationPtr CreateAllocation(
    TAllocationId id,
    TOperationId operationId,
    const NClusterNode::TJobResources& resourceUsage,
    std::optional<NScheduler::TAllocationAttributes> attributes,
    TControllerAgentDescriptor agentDescriptor,
    IBootstrap* bootstrap)
{
    return NewWithOffloadedDtor<TAllocation>(
        bootstrap->GetJobInvoker(),
        id,
        operationId,
        resourceUsage,
        std::move(attributes),
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
