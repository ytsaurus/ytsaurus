#include "job_controller.h"
#include "private.h"
#include "config.h"

#include <limits>

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/master_connector.h>
#include <yt/server/data_node/chunk_cache.h>

#include <yt/server/exec_agent/slot_manager.h>

#include <yt/ytlib/job_tracker_client/job_spec_service_proxy.h>
#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/fs.h>

namespace NYT {
namespace NJobAgent {

using namespace NRpc;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NYson;
using namespace NYTree;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobTrackerServerLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TJobController::TImpl
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(), ResourcesUpdated);

public:
    TImpl(
        TJobControllerConfigPtr config,
        TBootstrap* bootstrap);

    void Initialize();

    void RegisterFactory(
        EJobType type,
        TJobFactory factory);

    IJobPtr FindJob(const TJobId& jobId) const;
    IJobPtr GetJobOrThrow(const TJobId& jobId) const;
    std::vector<IJobPtr> GetJobs() const;

    TNodeResources GetResourceLimits() const;
    TNodeResources GetResourceUsage(bool includeWaiting = false) const;
    TDiskResources GetDiskInfo() const;
    void SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits);

    void SetDisableSchedulerJobs(bool value);

    void PrepareHeartbeatRequest(
        TCellTag cellTag,
        EObjectType jobObjectType,
        const TReqHeartbeatPtr& request);

    void ProcessHeartbeatResponse(
        const TRspHeartbeatPtr& response,
        EObjectType jobObjectType,
        NRpc::IChannelPtr jobSpecsProxyChannel);

    NYTree::IYPathServicePtr GetOrchidService();

private:
    const TJobControllerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    THashMap<EJobType, TJobFactory> Factories_;
    THashMap<TJobId, IJobPtr> Jobs_;

    THashSet<TJobId> SpeclessJobIds_;

    bool StartScheduled_ = false;

    bool DisableSchedulerJobs_ = false;

    IThroughputThrottlerPtr StatisticsThrottler_;

    TNodeResourceLimitsOverrides ResourceLimitsOverrides_;

    TNullable<TInstant> UserMemoryOverdraftInstant_;
    TNullable<TInstant> CpuOverdraftInstant_;

    TProfiler ResourceLimitsProfiler_;
    TProfiler ResourceUsageProfiler_;
    TEnumIndexedVector<TTagId, EJobOrigin> JobOriginToTag_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr ResourceAdjustmentExecutor_;

    bool IncludeStoredJobsInNextSchedulerHeartbeat_ = false;
    TInstant LastStoredJobsSendTime_;

    std::unique_ptr<TMemoryUsageTracker<EMemoryCategory>> ExternalMemoryUsageTracker_;

    //! Starts a new job.
    IJobPtr CreateJob(
        const TJobId& jobId,
        const TOperationId& operationId,
        const TNodeResources& resourceLimits,
        TJobSpec&& jobSpec);

    //! Stops a job.
    /*!
     *  If the job is running, aborts it.
     */
    void AbortJob(IJobPtr job);

    void FailJob(IJobPtr job);

    //! Interrupts a job.
    /*!
     *  If the job is running, interrupts it.
     */
    void InterruptJob(IJobPtr job);

    //! Removes the job from the map.
    /*!
     *  It is illegal to call #Remove before the job is stopped.
     */
    void RemoveJob(IJobPtr job);

    TJobFactory GetFactory(EJobType type) const;

    void ScheduleStart();

    void OnWaitingJobTimeout(TWeakPtr<IJob> weakJob);

    void OnResourcesUpdated(
        TWeakPtr<IJob> job,
        const TNodeResources& resourceDelta);

    void StartWaitingJobs();

    //! Compares new usage with resource limits. Detects resource overdraft.
    bool CheckResourceUsageDelta(const TNodeResources& delta);

    //! Returns |true| if a job with given #jobResources can be started.
    //! Takes special care with ReplicationDataSize and RepairDataSize enabling
    // an arbitrary large overdraft for the
    //! first job.
    bool HasEnoughResources(
        const TNodeResources& jobResources,
        const TNodeResources& usedResources);

    void BuildOrchid(IYsonConsumer* consumer) const;

    void OnProfiling();

    void AdjustResources();

    TMemoryUsageTracker<EMemoryCategory>* GetUserMemoryUsageTracker();
    TMemoryUsageTracker<EMemoryCategory>* GetSystemMemoryUsageTracker();

    const TMemoryUsageTracker<EMemoryCategory>* GetUserMemoryUsageTracker() const;
    const TMemoryUsageTracker<EMemoryCategory>* GetSystemMemoryUsageTracker() const;

    TEnumIndexedVector<std::vector<IJobPtr>, EJobOrigin> GetJobsByOrigin() const;
};

////////////////////////////////////////////////////////////////////////////////

TJobController::TImpl::TImpl(
    TJobControllerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(Config_->StatisticsThrottler))
    , ResourceLimitsProfiler_(Profiler.GetPathPrefix() + "/resource_limits")
    , ResourceUsageProfiler_(Profiler.GetPathPrefix() + "/resource_usage")
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TJobController::TImpl::Initialize()
{
    for (auto origin : TEnumTraits<EJobOrigin>::GetDomainValues()) {
        JobOriginToTag_[origin] = TProfileManager::Get()->RegisterTag("origin", FormatEnum(origin));
    }

    if (Bootstrap_->GetExecSlotManager()->ExternalJobMemory()) {
        LOG_INFO("Using external user job memory");
        ExternalMemoryUsageTracker_ = std::make_unique<TNodeMemoryTracker>(
            0,
            std::vector<std::pair<EMemoryCategory, i64>>{},
            Logger,
            TProfiler("/exec_agent/external_memory_usage"));
    }

    GetUserMemoryUsageTracker()->SetCategoryLimit(
        EMemoryCategory::UserJobs,
        Config_->ResourceLimits->UserMemory);

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TImpl::OnProfiling, MakeWeak(this)),
        ProfilingPeriod);
    ProfilingExecutor_->Start();

    ResourceAdjustmentExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TImpl::AdjustResources, MakeWeak(this)),
        Config_->ResourceAdjustmentPeriod);
    ResourceAdjustmentExecutor_->Start();
}

void TJobController::TImpl::RegisterFactory(EJobType type, TJobFactory factory)
{
    YCHECK(Factories_.insert(std::make_pair(type, factory)).second);
}

TJobFactory TJobController::TImpl::GetFactory(EJobType type) const
{
    auto it = Factories_.find(type);
    YCHECK(it != Factories_.end());
    return it->second;
}

IJobPtr TJobController::TImpl::FindJob(const TJobId& jobId) const
{
    auto it = Jobs_.find(jobId);
    return it == Jobs_.end() ? nullptr : it->second;
}

IJobPtr TJobController::TImpl::GetJobOrThrow(const TJobId& jobId) const
{
    auto job = FindJob(jobId);
    if (!job) {
        // We can get here only when job exists in scheduler, but job proxy is not yet started.
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::NoSuchJob,
            "Job %v has not yet started",
            jobId);
    }
    return job;
}

std::vector<IJobPtr> TJobController::TImpl::GetJobs() const
{
    std::vector<IJobPtr> result;
    for (const auto& pair : Jobs_) {
        result.push_back(pair.second);
    }
    return result;
}

TNodeResources TJobController::TImpl::GetResourceLimits() const
{
    TNodeResources result;

    // If chunk cache is disabled, we disable all scheduler jobs.
    result.set_user_slots(Bootstrap_->GetChunkCache()->IsEnabled() && !DisableSchedulerJobs_
        ? Bootstrap_->GetExecSlotManager()->GetSlotCount()
        : 0);

    #define XX(name, Name) \
        result.set_##name(ResourceLimitsOverrides_.has_##name() \
            ? ResourceLimitsOverrides_.name() \
            : Config_->ResourceLimits->Name);
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX

    const auto* userTracker = GetUserMemoryUsageTracker();
    result.set_user_memory(std::min(
        userTracker->GetLimit(EMemoryCategory::UserJobs),
        // NB: The sum of per-category limits can be greater than the total memory limit.
        // Therefore we need bound memory limit by actually available memory.
        userTracker->GetUsed(EMemoryCategory::UserJobs) + userTracker->GetTotalFree()));

    const auto* systemTracker = GetSystemMemoryUsageTracker();
    result.set_system_memory(std::min(
        systemTracker->GetLimit(EMemoryCategory::SystemJobs),
        systemTracker->GetUsed(EMemoryCategory::SystemJobs) + systemTracker->GetTotalFree()));

    auto maybeCpuLimit = Bootstrap_->GetExecSlotManager()->GetCpuLimit();
    if (maybeCpuLimit && !ResourceLimitsOverrides_.has_cpu()) {
        result.set_cpu(*maybeCpuLimit);
    }

    return result;
}

TNodeResources TJobController::TImpl::GetResourceUsage(bool includeWaiting) const
{
    auto result = ZeroNodeResources();
    for (const auto& pair : Jobs_) {
        const auto& job = pair.second;
        if (includeWaiting || job->GetState() != EJobState::Waiting) {
            result += job->GetResourceUsage();
        }
    }
    return result;
}

void TJobController::TImpl::AdjustResources()
{
    auto maybeMemoryLimit = Bootstrap_->GetExecSlotManager()->GetMemoryLimit();
    if (maybeMemoryLimit) {
        auto* tracker = GetUserMemoryUsageTracker();
        tracker->SetTotalLimit(*maybeMemoryLimit);
    }

    auto usage = GetResourceUsage(false);
    auto limits = GetResourceLimits();

    bool preemptMemoryOverdraft = false;
    bool preemptCpuOverdraft = false;
    if (usage.user_memory() > limits.user_memory()) {
        if (UserMemoryOverdraftInstant_) {
            preemptMemoryOverdraft = *UserMemoryOverdraftInstant_ + Config_->MemoryOverdraftTimeout <
                TInstant::Now();
        } else {
            UserMemoryOverdraftInstant_ = TInstant::Now();
        }
    } else {
        UserMemoryOverdraftInstant_ = Null;
    }

    if (usage.cpu() > limits.cpu()) {
        if (CpuOverdraftInstant_) {
            preemptCpuOverdraft = *CpuOverdraftInstant_+ Config_->CpuOverdraftTimeout <
                TInstant::Now();
        } else {
            CpuOverdraftInstant_ = TInstant::Now();
        }
    } else {
        CpuOverdraftInstant_ = Null;
    }

    LOG_DEBUG("Resource adjustment parameters (PreemptMemoryOverdraft: %v, PreemptCpuOverdraft: %v, "
        "MemoryOverdraftInstant: %v, CpuOverdraftInstant: %v)",
        preemptMemoryOverdraft,
        preemptCpuOverdraft,
        UserMemoryOverdraftInstant_,
        CpuOverdraftInstant_);

    if (preemptCpuOverdraft || preemptMemoryOverdraft) {
        std::vector<IJobPtr> schedulerJobs;
        for (const auto& pair : Jobs_) {
            if (TypeFromId(pair.first) == EObjectType::SchedulerJob && pair.second->GetState() == EJobState::Running) {
                schedulerJobs.push_back(pair.second);
            }
        }

        std::sort(schedulerJobs.begin(), schedulerJobs.end(), [] (const IJobPtr& lhs, const IJobPtr& rhs) {
            return lhs->GetStartTime() < rhs->GetStartTime();
        });

        while ((preemptCpuOverdraft && usage.cpu() > limits.cpu()) ||
            (preemptMemoryOverdraft && usage.user_memory() > limits.user_memory()))
        {
            if (schedulerJobs.empty()) {
                break;
            }

            usage -= schedulerJobs.back()->GetResourceUsage();
            schedulerJobs.back()->Abort(TError(
                NExecAgent::EErrorCode::ResourceOverdraft,
                "Resource usage overdraft adjustment"));
            schedulerJobs.pop_back();
        }

        UserMemoryOverdraftInstant_ = Null;
        CpuOverdraftInstant_ = Null;
    }
}

TDiskResources TJobController::TImpl::GetDiskInfo() const
{
    return Bootstrap_->GetExecSlotManager()->GetDiskInfo();
}

void TJobController::TImpl::SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits)
{
    ResourceLimitsOverrides_ = resourceLimits;
    auto* tracker = Bootstrap_->GetMemoryUsageTracker();
    if (ResourceLimitsOverrides_.has_user_memory()) {
        tracker->SetCategoryLimit(EMemoryCategory::UserJobs, ResourceLimitsOverrides_.user_memory());
    }

    if (ResourceLimitsOverrides_.has_system_memory()) {
        tracker->SetCategoryLimit(EMemoryCategory::SystemJobs, ResourceLimitsOverrides_.system_memory());
    }
}

void TJobController::TImpl::SetDisableSchedulerJobs(bool value)
{
    DisableSchedulerJobs_ = value;
}

void TJobController::TImpl::StartWaitingJobs()
{
    auto* tracker = Bootstrap_->GetMemoryUsageTracker();

    bool resourcesUpdated = false;

    {
        auto usedResources = GetResourceUsage();
        auto memoryToRelease = tracker->GetUsed(EMemoryCategory::UserJobs) - usedResources.user_memory();
        if (memoryToRelease > 0) {
            tracker->Release(EMemoryCategory::UserJobs, memoryToRelease);
            resourcesUpdated = true;
        }

        memoryToRelease = tracker->GetUsed(EMemoryCategory::SystemJobs) - usedResources.system_memory();
        if (memoryToRelease > 0) {
            tracker->Release(EMemoryCategory::SystemJobs, memoryToRelease);
            resourcesUpdated = true;
        }
    }

    for (const auto& pair : Jobs_) {
        auto job = pair.second;
        if (job->GetState() != EJobState::Waiting)
            continue;

        auto jobResources = job->GetResourceUsage();
        auto usedResources = GetResourceUsage();
        if (!HasEnoughResources(jobResources, usedResources)) {
            LOG_DEBUG("Not enough resources to start waiting job (JobId: %v, JobResources: %v, UsedResources: %v)",
                job->GetId(),
                FormatResources(jobResources),
                FormatResourceUsage(usedResources, GetResourceLimits()));
            continue;
        }

        if (jobResources.user_memory() > 0) {
            auto error = tracker->TryAcquire(EMemoryCategory::UserJobs, jobResources.user_memory());
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Not enough memory to start waiting job (JobId: %v)",
                    job->GetId());
                continue;
            }
        }

        if (jobResources.system_memory() > 0) {
            auto error = tracker->TryAcquire(EMemoryCategory::SystemJobs, jobResources.system_memory());
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Not enough memory to start waiting job (JobId: %v)",
                    job->GetId());
                continue;
            }
        }

        LOG_INFO("Starting job (JobId: %v)", job->GetId());

        job->SubscribeResourcesUpdated(
            BIND(&TImpl::OnResourcesUpdated, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetControlInvoker()));

        job->Start();

        resourcesUpdated = true;
    }

    if (resourcesUpdated) {
        ResourcesUpdated_.Fire();
    }

    StartScheduled_ = false;
}

IJobPtr TJobController::TImpl::CreateJob(
    const TJobId& jobId,
    const TOperationId& operationId,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec)
{
    auto type = EJobType(jobSpec.type());

    auto factory = GetFactory(type);

    auto job = factory.Run(
        jobId,
        operationId,
        resourceLimits,
        std::move(jobSpec));

    LOG_INFO("Job created (JobId: %v, OperationId: %v, JobType: %v)",
        jobId,
        operationId,
        type);

    YCHECK(Jobs_.insert(std::make_pair(jobId, job)).second);
    ScheduleStart();

    // Use #Apply instead of #Subscribe to match #OnWaitingJobTimeout signature.
    TDelayedExecutor::MakeDelayed(Config_->WaitingJobsTimeout)
        .Apply(BIND(&TImpl::OnWaitingJobTimeout, MakeWeak(this), MakeWeak(job))
        .Via(Bootstrap_->GetControlInvoker()));

    return job;
}

void TJobController::TImpl::OnWaitingJobTimeout(TWeakPtr<IJob> weakJob)
{
    auto strongJob = weakJob.Lock();
    if (!strongJob) {
        return;
    }

    if (strongJob->GetState() == EJobState::Waiting) {
        strongJob->Abort(TError(NExecAgent::EErrorCode::WaitingJobTimeout, "Job waiting has timed out")
            << TErrorAttribute("timeout", Config_->WaitingJobsTimeout));
    }
}

void TJobController::TImpl::ScheduleStart()
{
    if (!StartScheduled_) {
        Bootstrap_->GetControlInvoker()->Invoke(BIND(
            &TImpl::StartWaitingJobs,
            MakeWeak(this)));
        StartScheduled_ = true;
    }
}

void TJobController::TImpl::AbortJob(IJobPtr job)
{
    LOG_INFO("Job abort requested (JobId: %v)",
        job->GetId());

    job->Abort(TError(NExecAgent::EErrorCode::AbortByScheduler, "Job aborted by scheduler"));
}

void TJobController::TImpl::FailJob(IJobPtr job)
{
    LOG_INFO("Job fail requested (JobId: %v)",
        job->GetId());

    try {
        job->Fail();
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Failed to fail job (JobId: %v)", job->GetId());
    }
}

void TJobController::TImpl::InterruptJob(IJobPtr job)
{
    LOG_INFO("Job interrupt requested (JobId: %v)",
        job->GetId());

    try {
        job->Interrupt();
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Failed to interrupt job (JobId: %v)", job->GetId());
    }
}

void TJobController::TImpl::RemoveJob(IJobPtr job)
{
    LOG_INFO("Job removed (JobId: %v)", job->GetId());

    YCHECK(job->GetPhase() > EJobPhase::Cleanup);
    YCHECK(job->GetResourceUsage() == ZeroNodeResources());
    YCHECK(Jobs_.erase(job->GetId()) == 1);
}

void TJobController::TImpl::OnResourcesUpdated(TWeakPtr<IJob> job, const TNodeResources& resourceDelta)
{
    if (!CheckResourceUsageDelta(resourceDelta)) {
        auto job_ = job.Lock();
        if (job_) {
            job_->Abort(TError(
                NExecAgent::EErrorCode::ResourceOverdraft,
                "Failed to increase resource usage")
                << TErrorAttribute("resource_delta", FormatResources(resourceDelta)));
        }
        return;
    }

    if (!Dominates(resourceDelta, ZeroNodeResources())) {
        // Some resources decreased.
        ScheduleStart();
    }
}

bool TJobController::TImpl::CheckResourceUsageDelta(const TNodeResources& delta)
{
    // Nonincreasing resources cannot lead to overdraft.
    auto nodeLimits = GetResourceLimits();
    auto newUsage = GetResourceUsage() + delta;

    #define XX(name, Name) if (delta.name() > 0 && nodeLimits.name() < newUsage.name()) { return false; }
    ITERATE_NODE_RESOURCES(XX)
    #undef XX

    if (delta.user_memory() > 0) {
        auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        auto error = tracker->TryAcquire(EMemoryCategory::UserJobs, delta.user_memory());
        if (!error.IsOK()) {
            return false;
        }
    }

    return true;
}

bool TJobController::TImpl::HasEnoughResources(
    const TNodeResources& jobResources,
    const TNodeResources& usedResources)
{
    auto totalResources = GetResourceLimits();
    auto spareResources = MakeNonnegative(totalResources - usedResources);
    // Allow replication/repair data size overcommit.
    spareResources.set_replication_data_size(InfiniteNodeResources().replication_data_size());
    spareResources.set_repair_data_size(InfiniteNodeResources().repair_data_size());
    return Dominates(spareResources, jobResources);
}

void TJobController::TImpl::PrepareHeartbeatRequest(
    TCellTag cellTag,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    auto masterConnector = Bootstrap_->GetMasterConnector();
    request->set_node_id(masterConnector->GetNodeId());
    ToProto(request->mutable_node_descriptor(), masterConnector->GetLocalDescriptor());
    *request->mutable_resource_limits() = GetResourceLimits();
    *request->mutable_resource_usage() = GetResourceUsage(/* includeWaiting */ true);

    *request->mutable_disk_info() = GetDiskInfo();

    // A container for all scheduler jobs that are candidate to send statistics. This set contains
    // only the running jobs since all completed/aborted/failed jobs always send their statistics.
    std::vector<std::pair<IJobPtr, TJobStatus*>> runningJobs;

    i64 completedJobsStatisticsSize = 0;

    bool includeStoredJobs = false;
    if (jobObjectType == EObjectType::SchedulerJob) {
        auto now = TInstant::Now();
        includeStoredJobs =
            IncludeStoredJobsInNextSchedulerHeartbeat_ ||
            LastStoredJobsSendTime_ + Config_->StoredJobsSendPeriod < now;
        if (includeStoredJobs) {
            LastStoredJobsSendTime_ = now;
            LOG_INFO("Including all stored jobs in heartbeat");
        }
        request->set_stored_jobs_included(includeStoredJobs);
    }

    for (const auto& pair : Jobs_) {
        const auto& jobId = pair.first;
        const auto& job = pair.second;
        if (CellTagFromId(jobId) != cellTag)
            continue;
        if (TypeFromId(jobId) != jobObjectType)
            continue;
        if (job->GetStored() && !includeStoredJobs)
            continue;

        auto* jobStatus = request->add_jobs();
        FillJobStatus(jobStatus, job);
        switch (job->GetState()) {
            case EJobState::Running:
                *jobStatus->mutable_resource_usage() = job->GetResourceUsage();
                if (jobObjectType == EObjectType::SchedulerJob) {
                    runningJobs.emplace_back(job, jobStatus);
                }
                break;

            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed:
                *jobStatus->mutable_result() = job->GetResult();
                if (auto statistics = job->GetStatistics()) {
                    completedJobsStatisticsSize += statistics.GetData().size();
                    job->ResetStatisticsLastSendTime();
                    jobStatus->set_statistics(statistics.GetData());
                }
                break;

            default:
                break;
        }
    }

    if (jobObjectType == EObjectType::SchedulerJob) {
        std::sort(
            runningJobs.begin(),
            runningJobs.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.first->GetStatisticsLastSendTime() < rhs.first->GetStatisticsLastSendTime();
            });

        i64 runningJobsStatisticsSize = 0;

        for (const auto& pair : runningJobs) {
            const auto& job = pair.first;
            auto* jobStatus = pair.second;
            auto statistics = job->GetStatistics();
            if (statistics && StatisticsThrottler_->TryAcquire(statistics.GetData().size())) {
                runningJobsStatisticsSize += statistics.GetData().size();
                job->ResetStatisticsLastSendTime();
                jobStatus->set_statistics(statistics.GetData());
            }
        }

        LOG_DEBUG("Job statistics prepared (RunningJobsStatisticsSize: %v, CompletedJobsStatisticsSize: %v)",
            runningJobsStatisticsSize,
            completedJobsStatisticsSize);

        // TODO(ignat): make it in more general way (non-scheduler specific).
        for (const auto& jobId : SpeclessJobIds_) {
            auto* jobStatus = request->add_jobs();
            ToProto(jobStatus->mutable_job_id(), jobId);
            jobStatus->set_job_type(static_cast<int>(EJobType::Unknown));
            jobStatus->set_state(static_cast<int>(EJobState::Aborted));
            jobStatus->set_phase(static_cast<int>(EJobPhase::Missing));
            jobStatus->set_progress(0.0);

            TJobResult jobResult;
            auto error = TError("Failed to get job spec")
                << TErrorAttribute("abort_reason", NScheduler::EAbortReason::GetSpecFailed);
            ToProto(jobResult.mutable_error(), error);
            *jobStatus->mutable_result() = jobResult;
        }
    }
}

void TJobController::TImpl::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType,
    NRpc::IChannelPtr jobSpecsProxyChannel)
{
    for (const auto& protoJobId : response->jobs_to_remove()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        if (SpeclessJobIds_.find(jobId) != SpeclessJobIds_.end()) {
            SpeclessJobIds_.erase(jobId);
            continue;
        }

        auto job = FindJob(jobId);
        if (job) {
            RemoveJob(job);
        } else {
            LOG_WARNING("Requested to remove a non-existent job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_abort()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            AbortJob(job);
        } else {
            LOG_WARNING("Requested to abort a non-existent job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_interrupt()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            InterruptJob(job);
        } else {
            LOG_WARNING("Requested to interrupt a non-existing job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_fail()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            FailJob(job);
        } else {
            LOG_WARNING("Requested to fail a non-existent job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId: response->jobs_to_store()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            LOG_DEBUG("Storing job (JobId: %v)",
                jobId);
            job->SetStored(true);
        } else {
            LOG_WARNING("Requested to store a non-existent job (JobId: %v)",
                jobId);
        }
    }

    if (jobObjectType == EObjectType::SchedulerJob) {
        IncludeStoredJobsInNextSchedulerHeartbeat_ = response->include_stored_jobs_in_next_heartbeat();
    }

    std::vector<TJobSpec> specs(response->jobs_to_start_size());

    if (specs.empty()) {
        return;
    }

    bool hasSpecsInAttachments = !response->Attachments().empty();

    if (hasSpecsInAttachments) {
        int attachmentIndex = 0;
        for (const auto& info : response->jobs_to_start()) {
            TJobSpec spec;
            const auto& attachment = response->Attachments()[attachmentIndex++];
            DeserializeProtoWithEnvelope(&spec, attachment);

            auto jobId = FromProto<TJobId>(info.job_id());
            auto operationId = FromProto<TJobId>(info.operation_id());
            const auto& resourceLimits = info.resource_limits();

            CreateJob(jobId, operationId, resourceLimits, std::move(spec));
        }
    } else {
        YCHECK(jobSpecsProxyChannel);
        TJobSpecServiceProxy jobSpecServiceProxy(jobSpecsProxyChannel);
        jobSpecServiceProxy.SetDefaultTimeout(Config_->GetJobSpecsTimeout);

        auto jobSpecRequest = jobSpecServiceProxy.GetJobSpecs();
        for (const auto& info : response->jobs_to_start()) {
            auto* subrequest = jobSpecRequest->add_requests();
            *subrequest->mutable_operation_id() = info.operation_id();
            *subrequest->mutable_job_id() = info.job_id();

            auto jobId = FromProto<TJobId>(info.job_id());
            auto operationId = FromProto<TJobId>(info.operation_id());
            YCHECK(SpeclessJobIds_.insert(jobId).second);
            LOG_DEBUG("Getting job spec (OperationId: %v, JobId: %v)",
                operationId,
                jobId);
        }

        auto jobSpecResponseOrError = WaitFor(jobSpecRequest->Invoke());
        if (!jobSpecResponseOrError.IsOK()) {
            LOG_DEBUG(jobSpecResponseOrError, "Failed to get job specs from scheduler");
            return;
        }

        const auto& jobSpecResponse = jobSpecResponseOrError.Value();
        for (int index = 0; index < response->jobs_to_start_size(); ++index) {
            const auto& info = response->jobs_to_start(index);
            auto jobId = FromProto<TJobId>(info.job_id());
            auto operationId = FromProto<TJobId>(info.operation_id());
            const auto& resourceLimits = info.resource_limits();

            const auto& subresponse = jobSpecResponse->mutable_responses(index);
            if (subresponse->has_error()) {
                auto error = FromProto<TError>(jobSpecResponse->responses(index).error());
                if (!error.IsOK()) {
                    LOG_DEBUG(error, "Failed to get job spec from scheduler (OperationId: %v, JobId: %v)",
                        operationId,
                        jobId);
                    continue;
                }
            }

            TJobSpec spec;
            const auto& attachment = jobSpecResponse->Attachments()[index];
            DeserializeProtoWithEnvelope(&spec, attachment);

            YCHECK(SpeclessJobIds_.erase(jobId) > 0);
            CreateJob(jobId, operationId, resourceLimits, std::move(spec));
        }
    }
}

TEnumIndexedVector<std::vector<IJobPtr>, EJobOrigin> TJobController::TImpl::GetJobsByOrigin() const
{
    auto jobs = TEnumIndexedVector<std::vector<IJobPtr>, EJobOrigin>();
    for (const auto& pair : Jobs_) {
        switch (TypeFromId(pair.first)) {
            case EObjectType::MasterJob:
                jobs[EJobOrigin::Master].push_back(pair.second);
                break;
            case EObjectType::SchedulerJob:
                jobs[EJobOrigin::Scheduler].push_back(pair.second);
                break;
            default:
                Y_UNREACHABLE();
        }
    }
    return jobs;
}

void TJobController::TImpl::BuildOrchid(IYsonConsumer* consumer) const
{
    auto jobs = GetJobsByOrigin();
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("resource_limits").Value(GetResourceLimits())
            .Item("resource_usage").Value(GetResourceUsage())
            .Item("active_job_count").DoMapFor(
                TEnumTraits<EJobOrigin>::GetDomainValues(),
                [&] (TFluentMap fluent, EJobOrigin origin) {
                    fluent.Item(FormatEnum(origin)).Value(jobs[origin].size());
                })
            .Item("active_jobs").DoMapFor(
                TEnumTraits<EJobOrigin>::GetDomainValues(),
                [&] (TFluentMap fluent, EJobOrigin origin) {
                    fluent.Item(FormatEnum(origin)).DoMapFor(
                        jobs[origin],
                        [&] (TFluentMap fluent, IJobPtr job) {
                            fluent.Item(ToString(job->GetId()))
                                .BeginMap()
                                    .Item("job_state").Value(job->GetState())
                                    .Item("job_phase").Value(job->GetPhase())
                                    .Item("job_type").Value(job->GetType())
                                    .Item("start_time").Value(job->GetStartTime())
                                    .Item("duration").Value(TInstant::Now() - job->GetStartTime())
                                    .DoIf(static_cast<bool>(job->GetStatistics()), [&] (TFluentMap fluent) {
                                        fluent
                                            .Item("statistics").Value(job->GetStatistics());
                                    })
                                    .DoIf(static_cast<bool>(job->GetOperationId()), [&] (TFluentMap fluent) {
                                        fluent
                                            .Item("operation_id").Value(job->GetOperationId());
                                    })
                                .EndMap();
                        });
                })
        .EndMap();
}

IYPathServicePtr TJobController::TImpl::GetOrchidService()
{
    auto producer = BIND(&TImpl::BuildOrchid, MakeStrong(this));
    return IYPathService::FromProducer(producer);
}

void TJobController::TImpl::OnProfiling()
{
    auto jobs = GetJobsByOrigin();
    for (auto origin : TEnumTraits<EJobOrigin>::GetDomainValues()) {
        Profiler.Enqueue("/active_job_count", jobs[origin].size(), EMetricType::Gauge, {JobOriginToTag_[origin]});
    }
    ProfileResources(ResourceUsageProfiler_, GetResourceUsage());
    ProfileResources(ResourceLimitsProfiler_, GetResourceLimits());
}

TMemoryUsageTracker<EMemoryCategory>* TJobController::TImpl::GetUserMemoryUsageTracker()
{
    if (Bootstrap_->GetExecSlotManager()->ExternalJobMemory()) {
        return ExternalMemoryUsageTracker_.get();
    } else {
        return Bootstrap_->GetMemoryUsageTracker();
    }
}


TMemoryUsageTracker<EMemoryCategory>* TJobController::TImpl::GetSystemMemoryUsageTracker()
{
    return Bootstrap_->GetMemoryUsageTracker();
}

const TMemoryUsageTracker<EMemoryCategory>* TJobController::TImpl::GetUserMemoryUsageTracker() const
{
    if (Bootstrap_->GetExecSlotManager()->ExternalJobMemory()) {
        return ExternalMemoryUsageTracker_.get();
    } else {
        return Bootstrap_->GetMemoryUsageTracker();
    }
}


const TMemoryUsageTracker<EMemoryCategory>* TJobController::TImpl::GetSystemMemoryUsageTracker() const
{
    return Bootstrap_->GetMemoryUsageTracker();
}



////////////////////////////////////////////////////////////////////////////////

TJobController::TJobController(
    TJobControllerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        bootstrap))
{ }

void TJobController::Initialize()
{
    Impl_->Initialize();
}


void TJobController::RegisterFactory(
    EJobType type,
    TJobFactory factory)
{
    Impl_->RegisterFactory(type, factory);
}

IJobPtr TJobController::FindJob(const TJobId& jobId) const
{
    return Impl_->FindJob(jobId);
}

IJobPtr TJobController::GetJobOrThrow(const TJobId& jobId) const
{
    return Impl_->GetJobOrThrow(jobId);
}

std::vector<IJobPtr> TJobController::GetJobs() const
{
    return Impl_->GetJobs();
}

TNodeResources TJobController::GetResourceLimits() const
{
    return Impl_->GetResourceLimits();
}

TNodeResources TJobController::GetResourceUsage(bool includeWaiting) const
{
    return Impl_->GetResourceUsage(includeWaiting);
}

void TJobController::SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits)
{
    Impl_->SetResourceLimitsOverrides(resourceLimits);
}

void TJobController::SetDisableSchedulerJobs(bool value)
{
    Impl_->SetDisableSchedulerJobs(value);
}

void TJobController::PrepareHeartbeatRequest(
    TCellTag cellTag,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    Impl_->PrepareHeartbeatRequest(cellTag, jobObjectType, request);
}

void TJobController::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType,
    IChannelPtr jobSpecsProxyChannel)
{
    Impl_->ProcessHeartbeatResponse(response, jobObjectType, jobSpecsProxyChannel);
}

IYPathServicePtr TJobController::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_SIGNAL(TJobController, void(), ResourcesUpdated, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
