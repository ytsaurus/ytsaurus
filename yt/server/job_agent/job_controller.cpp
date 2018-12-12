#include "job_controller.h"
#include "private.h"
#include "gpu_manager.h"
#include "config.h"

#include <limits>

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/master_connector.h>
#include <yt/server/data_node/chunk_cache.h>

#include <yt/server/exec_agent/slot_manager.h>

#include <yt/server/tablet_node/slot_manager.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>
#include <yt/ytlib/job_tracker_client/job_spec_service_proxy.h>
#include <yt/ytlib/job_tracker_client/helpers.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>
#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/scheduler/public.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/fs.h>

#include <yt/core/net/helpers.h>

namespace NYT::NJobAgent {

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
using namespace NScheduler;
using namespace NNet;

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
        EObjectType jobObjectType);

    NYTree::IYPathServicePtr GetOrchidService();

private:
    const TJobControllerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    THashMap<EJobType, TJobFactory> Factories_;
    THashMap<TJobId, IJobPtr> Jobs_;

    //! Jobs that did not succeed in fetching spec are not getting
    //! their IJob structure, so we have to store job id alongside
    //! with the operation id to fill the TJobStatus proto message
    //! properly.
    THashMap<TJobId, TOperationId> SpecFetchFailedJobIds_;

    bool StartScheduled_ = false;

    bool DisableSchedulerJobs_ = false;

    IThroughputThrottlerPtr StatisticsThrottler_;

    TNodeResourceLimitsOverrides ResourceLimitsOverrides_;

    std::optional<TInstant> UserMemoryOverdraftInstant_;
    std::optional<TInstant> CpuOverdraftInstant_;

    TProfiler ResourceLimitsProfiler_;
    TProfiler ResourceUsageProfiler_;
    TEnumIndexedVector<TTagId, EJobOrigin> JobOriginToTag_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr ResourceAdjustmentExecutor_;

    THashSet<TJobId> JobIdsToConfirm_;
    TInstant LastStoredJobsSendTime_;

    TNodeMemoryTrackerPtr ExternalMemoryUsageTracker_;

    THashSet<int> FreePorts_;

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
    void AbortJob(const IJobPtr& job);

    void FailJob(const IJobPtr& job);

    //! Interrupts a job.
    /*!
     *  If the job is running, interrupts it.
     */
    void InterruptJob(const IJobPtr& job);

    //! Removes the job from the map.
    /*!
     *  It is illegal to call #Remove before the job is stopped.
     */
    void RemoveJob(const IJobPtr& job, bool archiveJobSpec, bool archiveStderr, bool archiveFailContext, bool archiveProfile);

    TJobFactory GetFactory(EJobType type) const;

    void ScheduleStart();

    void OnWaitingJobTimeout(const TWeakPtr<IJob>& weakJob);

    void OnResourcesUpdated(
        const TWeakPtr<IJob>& job,
        const TNodeResources& resourceDelta);

    void OnPortsReleased(const TWeakPtr<IJob>& job);

    void StartWaitingJobs();

    //! Compares new usage with resource limits. Detects resource overdraft.
    bool CheckMemoryOverdraft(const TNodeResources& delta);

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

    TNodeMemoryTracker* GetUserMemoryUsageTracker();
    TNodeMemoryTracker* GetSystemMemoryUsageTracker();

    const TNodeMemoryTracker* GetUserMemoryUsageTracker() const;
    const TNodeMemoryTracker* GetSystemMemoryUsageTracker() const;

    i64 GetUserJobsFreeMemoryWatermark() const;

    TEnumIndexedVector<std::vector<IJobPtr>, EJobOrigin> GetJobsByOrigin() const;
};

////////////////////////////////////////////////////////////////////////////////

TJobController::TImpl::TImpl(
    TJobControllerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(Config_->StatisticsThrottler))
    , ResourceLimitsProfiler_(Profiler.AppendPath("/resource_limits"))
    , ResourceUsageProfiler_(Profiler.AppendPath("/resource_usage"))
{
    YCHECK(Config_);
    YCHECK(Bootstrap_);

    for (int index = 0; index < Config_->PortCount; ++index) {
        FreePorts_.insert(Config_->StartPort + index);
    }
}

void TJobController::TImpl::Initialize()
{
    for (auto origin : TEnumTraits<EJobOrigin>::GetDomainValues()) {
        JobOriginToTag_[origin] = TProfileManager::Get()->RegisterTag("origin", FormatEnum(origin));
    }

    if (Bootstrap_->GetExecSlotManager()->ExternalJobMemory()) {
        YT_LOG_INFO("Using external user job memory");
        ExternalMemoryUsageTracker_ = New<TNodeMemoryTracker>(
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

    if (!Config_->TestGpu) {
        result.set_gpu(Bootstrap_->GetGpuManager()->GetTotalGpuCount());
    }

    const auto* userTracker = GetUserMemoryUsageTracker();
    result.set_user_memory(std::min(
        userTracker->GetLimit(EMemoryCategory::UserJobs),
        // NB: The sum of per-category limits can be greater than the total memory limit.
        // Therefore we need bound memory limit by actually available memory.
        userTracker->GetUsed(EMemoryCategory::UserJobs) + userTracker->GetTotalFree() - GetUserJobsFreeMemoryWatermark()));

    const auto* systemTracker = GetSystemMemoryUsageTracker();
    result.set_system_memory(std::min(
        systemTracker->GetLimit(EMemoryCategory::SystemJobs),
        systemTracker->GetUsed(EMemoryCategory::SystemJobs) + systemTracker->GetTotalFree() - Config_->FreeMemoryWatermark));

    auto optionalCpuLimit = Bootstrap_->GetExecSlotManager()->GetCpuLimit();
    if (optionalCpuLimit && !ResourceLimitsOverrides_.has_cpu()) {
        result.set_cpu(*optionalCpuLimit);
    }

    if (result.has_cpu()) {
        const auto& tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletCpu = tabletSlotManager->GetUsedCpu(Config_->CpuPerTabletSlot);
        result.set_cpu(std::max(0.0, result.cpu() - tabletCpu));
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

    result.set_user_slots(Bootstrap_->GetExecSlotManager()->GetUsedSlotCount());
    return result;
}

void TJobController::TImpl::AdjustResources()
{
    auto optionalMemoryLimit = Bootstrap_->GetExecSlotManager()->GetMemoryLimit();
    if (optionalMemoryLimit) {
        auto* tracker = GetUserMemoryUsageTracker();
        tracker->SetTotalLimit(*optionalMemoryLimit);
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
        UserMemoryOverdraftInstant_ = std::nullopt;
    }

    if (usage.cpu() > limits.cpu()) {
        if (CpuOverdraftInstant_) {
            preemptCpuOverdraft = *CpuOverdraftInstant_+ Config_->CpuOverdraftTimeout <
                TInstant::Now();
        } else {
            CpuOverdraftInstant_ = TInstant::Now();
        }
    } else {
        CpuOverdraftInstant_ = std::nullopt;
    }

    YT_LOG_DEBUG("Resource adjustment parameters (PreemptMemoryOverdraft: %v, PreemptCpuOverdraft: %v, "
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

        UserMemoryOverdraftInstant_ = std::nullopt;
        CpuOverdraftInstant_ = std::nullopt;
    }
}

TDiskResources TJobController::TImpl::GetDiskInfo() const
{
    return Bootstrap_->GetExecSlotManager()->GetDiskInfo();
}

void TJobController::TImpl::SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits)
{
    ResourceLimitsOverrides_ = resourceLimits;
    if (ResourceLimitsOverrides_.has_user_memory()) {
        GetUserMemoryUsageTracker()->SetCategoryLimit(EMemoryCategory::UserJobs, ResourceLimitsOverrides_.user_memory());
    } else {
        GetUserMemoryUsageTracker()->SetCategoryLimit(
            EMemoryCategory::UserJobs,
            Config_->ResourceLimits->UserMemory);
    }

    if (ResourceLimitsOverrides_.has_system_memory()) {
        GetSystemMemoryUsageTracker()->SetCategoryLimit(EMemoryCategory::SystemJobs, ResourceLimitsOverrides_.system_memory());
    }
}

void TJobController::TImpl::SetDisableSchedulerJobs(bool value)
{
    DisableSchedulerJobs_ = value;
}

void TJobController::TImpl::StartWaitingJobs()
{
    bool resourcesUpdated = false;

    {
        auto usedResources = GetResourceUsage();

        auto memoryToRelease = GetUserMemoryUsageTracker()->GetUsed(EMemoryCategory::UserJobs) - usedResources.user_memory();
        if (memoryToRelease > 0) {
            GetUserMemoryUsageTracker()->Release(EMemoryCategory::UserJobs, memoryToRelease);
            resourcesUpdated = true;
        }

        memoryToRelease = GetSystemMemoryUsageTracker()->GetUsed(EMemoryCategory::SystemJobs) - usedResources.system_memory();
        if (memoryToRelease > 0) {
            GetSystemMemoryUsageTracker()->Release(EMemoryCategory::SystemJobs, memoryToRelease);
            resourcesUpdated = true;
        }
    }

    for (const auto& pair : Jobs_) {
        auto job = pair.second;
        if (job->GetState() != EJobState::Waiting)
            continue;

        NLogging::TLogger jobLogger = JobTrackerServerLogger;
        jobLogger.AddTag("JobId: %v", job->GetId());

        const auto& Logger = jobLogger;

        auto portCount = job->GetPortCount();

        auto jobResources = job->GetResourceUsage();
        auto usedResources = GetResourceUsage();
        if (!HasEnoughResources(jobResources, usedResources)) {
            YT_LOG_DEBUG("Not enough resources to start waiting job (JobResources: %v, UsedResources: %v)",
                job->GetId(),
                FormatResources(jobResources),
                FormatResourceUsage(usedResources, GetResourceLimits()));
            continue;
        }

        if (jobResources.user_memory() > 0) {
            bool reachedWatermark = GetUserMemoryUsageTracker()->GetTotalFree() <= GetUserJobsFreeMemoryWatermark();
            if (reachedWatermark) {
                YT_LOG_DEBUG("Not enough memory to start waiting job; reached free memory watermark (JobId: %v)",
                   job->GetId());
                continue;
            }

            auto error = GetUserMemoryUsageTracker()->TryAcquire(EMemoryCategory::UserJobs, jobResources.user_memory());
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Not enough memory to start waiting job");
                continue;
            }
        }

        if (jobResources.system_memory() > 0) {
            bool reachedWatermark = GetSystemMemoryUsageTracker()->GetTotalFree() <= Config_->FreeMemoryWatermark;
            if (reachedWatermark) {
                YT_LOG_DEBUG("Not enough memory to start waiting job; reached free memory watermark (JobId: %v)",
                    job->GetId());
                continue;
            }

            auto error = GetSystemMemoryUsageTracker()->TryAcquire(EMemoryCategory::SystemJobs, jobResources.system_memory());
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Not enough memory to start waiting job");
                continue;
            }
        }

        std::vector<int> ports;

        if (portCount > 0) {
            YT_LOG_INFO("Allocating ports (PortCount: %v)", portCount);

            try {
                ports = AllocateFreePorts(portCount, FreePorts_, jobLogger);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Error while allocating free ports (PortCount: %v)", portCount);
                continue;
            }

            if (ports.size() < portCount) {
                YT_LOG_DEBUG("Not enough bindable free ports to start job (PortCount: %v, FreePortCount: %v)",
                    portCount,
                    ports.size());
                continue;
            }

            for (int port : ports) {
                FreePorts_.erase(port);
            }
            job->SetPorts(ports);
            YT_LOG_DEBUG("Ports allocated (PortCount: %v, Ports: %v)", ports.size(), ports);
        }

        job->SubscribeResourcesUpdated(
            BIND(&TImpl::OnResourcesUpdated, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetControlInvoker()));

        job->SubscribePortsReleased(
            BIND(&TImpl::OnPortsReleased, MakeWeak(this), MakeWeak(job))
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

    YT_LOG_INFO("Job created (JobId: %v, OperationId: %v, JobType: %v)",
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

void TJobController::TImpl::OnWaitingJobTimeout(const TWeakPtr<IJob>& weakJob)
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

void TJobController::TImpl::AbortJob(const IJobPtr& job)
{
    YT_LOG_INFO("Job abort requested (JobId: %v)",
        job->GetId());

    job->Abort(TError(NExecAgent::EErrorCode::AbortByScheduler, "Job aborted by scheduler"));
}

void TJobController::TImpl::FailJob(const IJobPtr& job)
{
    YT_LOG_INFO("Job fail requested (JobId: %v)",
        job->GetId());

    try {
        job->Fail();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to fail job (JobId: %v)", job->GetId());
    }
}

void TJobController::TImpl::InterruptJob(const IJobPtr& job)
{
    YT_LOG_INFO("Job interrupt requested (JobId: %v)",
        job->GetId());

    try {
        job->Interrupt();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to interrupt job (JobId: %v)", job->GetId());
    }
}

void TJobController::TImpl::RemoveJob(const IJobPtr& job, bool archiveJobSpec, bool archiveStderr, bool archiveFailContext, bool archiveProfile)
{
    YCHECK(job->GetPhase() >= EJobPhase::Cleanup);
    YCHECK(job->GetResourceUsage() == ZeroNodeResources());

    if (archiveJobSpec) {
        YT_LOG_INFO("Job spec archived (JobId: %v)", job->GetId());
        job->ReportSpec();
    }

    if (archiveStderr) {
        YT_LOG_INFO("Stderr archived (JobId: %v)", job->GetId());
        job->ReportStderr();
    }

    if (archiveFailContext) {
        YT_LOG_INFO("Fail context archived (JobId: %v)", job->GetId());
        job->ReportFailContext();
    }

    if (archiveProfile) {
        YT_LOG_INFO("Profile archived (JobId: %v)", job->GetId());
        job->ReportProfile();
    }

    YCHECK(Jobs_.erase(job->GetId()) == 1);

    YT_LOG_INFO("Job removed (JobId: %v)", job->GetId());
}

void TJobController::TImpl::OnResourcesUpdated(const TWeakPtr<IJob>& job, const TNodeResources& resourceDelta)
{
    if (!CheckMemoryOverdraft(resourceDelta)) {
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

void TJobController::TImpl::OnPortsReleased(const TWeakPtr<IJob>& job)
{
    auto job_ = job.Lock();
    if (job_) {
        const auto& ports = job_->GetPorts();
        YT_LOG_INFO("Releasing ports (JobId: %v, PortCount: %v, Ports: %v)", job_->GetId(), ports.size(), ports);
        for (auto port : ports) {
            YCHECK(FreePorts_.insert(port).second);
        }
    }
}

bool TJobController::TImpl::CheckMemoryOverdraft(const TNodeResources& delta)
{
    // Only cpu and user_memory can be increased.
    // Network decreases by design. Cpu increasing is handled in AdjustResources.
    // Others are not reported by job proxy (see TSupervisorService::UpdateResourceUsage).

    if (delta.user_memory() > 0) {
        bool reachedWatermark = GetUserMemoryUsageTracker()->GetTotalFree() <= GetUserJobsFreeMemoryWatermark();
        if (reachedWatermark) {
            return false;
        }

        auto error = GetUserMemoryUsageTracker()->TryAcquire(EMemoryCategory::UserJobs, delta.user_memory());
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

    request->set_job_reporter_write_failures_count(Bootstrap_->GetStatisticsReporter()->ExtractWriteFailuresCount());
    request->set_job_reporter_queue_is_too_large(Bootstrap_->GetStatisticsReporter()->GetQueueIsTooLarge());

    // A container for all scheduler jobs that are candidate to send statistics. This set contains
    // only the running jobs since all completed/aborted/failed jobs always send their statistics.
    std::vector<std::pair<IJobPtr, TJobStatus*>> runningJobs;

    i64 completedJobsStatisticsSize = 0;

    bool totalConfirmation = false;
    if (jobObjectType == EObjectType::SchedulerJob) {
        auto now = TInstant::Now();
        if (LastStoredJobsSendTime_ + Config_->TotalConfirmationPeriod < now) {
            LastStoredJobsSendTime_ = now;
            YT_LOG_INFO("Including all stored jobs in heartbeat");
            totalConfirmation = true;
        }
    }

    int confirmedJobCount = 0;

    for (const auto& pair : Jobs_) {
        const auto& jobId = pair.first;
        const auto& job = pair.second;
        if (CellTagFromId(jobId) != cellTag)
            continue;
        if (TypeFromId(jobId) != jobObjectType)
            continue;
        auto it = JobIdsToConfirm_.find(jobId);
        if (job->GetStored() && !totalConfirmation && it == JobIdsToConfirm_.end()) {
            continue;
        }
        if (job->GetStored() || it != JobIdsToConfirm_.end()) {
            YT_LOG_DEBUG("Confirming job (JobId: %v, OperationId: %v, Stored: %v, State: %v)",
                jobId,
                job->GetOperationId(),
                job->GetStored(),
                job->GetState());
            ++confirmedJobCount;
        }
        if (it != JobIdsToConfirm_.end()) {
            JobIdsToConfirm_.erase(it);
        }

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

    request->set_confirmed_job_count(confirmedJobCount);

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

        YT_LOG_DEBUG("Job statistics prepared (RunningJobsStatisticsSize: %v, CompletedJobsStatisticsSize: %v)",
            runningJobsStatisticsSize,
            completedJobsStatisticsSize);

        // TODO(ignat): make it in more general way (non-scheduler specific).
        for (const auto& pair : SpecFetchFailedJobIds_) {
            const auto& jobId = pair.first;
            const auto& operationId = pair.second;
            auto* jobStatus = request->add_jobs();
            ToProto(jobStatus->mutable_job_id(), jobId);
            ToProto(jobStatus->mutable_operation_id(), operationId);
            jobStatus->set_job_type(static_cast<int>(EJobType::SchedulerUnknown));
            jobStatus->set_state(static_cast<int>(EJobState::Aborted));
            jobStatus->set_phase(static_cast<int>(EJobPhase::Missing));
            jobStatus->set_progress(0.0);

            TJobResult jobResult;
            auto error = TError("Failed to get job spec")
                << TErrorAttribute("abort_reason", NScheduler::EAbortReason::GetSpecFailed);
            ToProto(jobResult.mutable_error(), error);
            *jobStatus->mutable_result() = jobResult;
        }

        if (!JobIdsToConfirm_.empty()) {
            YT_LOG_WARNING("Unconfirmed jobs found (UnconfirmedJobCount: %v)", JobIdsToConfirm_.size());
            for (const auto& jobId : JobIdsToConfirm_) {
                YT_LOG_DEBUG("Unconfirmed job (JobId: %v)", jobId);
            }
            ToProto(request->mutable_unconfirmed_jobs(), JobIdsToConfirm_);
        }
    }
}

void TJobController::TImpl::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    for (const auto& protoJobToRemove : response->jobs_to_remove()) {
        auto jobToRemove = FromProto<TJobToRelease>(protoJobToRemove);
        const auto& jobId = jobToRemove.JobId;
        if (SpecFetchFailedJobIds_.erase(jobId) == 1) {
            continue;
        }

        auto job = FindJob(jobId);
        if (job) {
            RemoveJob(job, jobToRemove.ArchiveJobSpec, jobToRemove.ArchiveStderr, jobToRemove.ArchiveFailContext, jobToRemove.ArchiveProfile);
        } else {
            YT_LOG_WARNING("Requested to remove a non-existent job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_abort()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            AbortJob(job);
        } else {
            YT_LOG_WARNING("Requested to abort a non-existent job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_interrupt()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            InterruptJob(job);
        } else {
            YT_LOG_WARNING("Requested to interrupt a non-existing job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_fail()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            FailJob(job);
        } else {
            YT_LOG_WARNING("Requested to fail a non-existent job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId: response->jobs_to_store()) {
        auto jobId = FromProto<TJobId>(protoJobId);
        auto job = FindJob(jobId);
        if (job) {
            YT_LOG_DEBUG("Storing job (JobId: %v)",
                jobId);
            job->SetStored(true);
        } else {
            YT_LOG_WARNING("Requested to store a non-existent job (JobId: %v)",
                jobId);
        }
    }

    JobIdsToConfirm_.clear();
    if (jobObjectType == EObjectType::SchedulerJob) {
        auto jobIdsToConfirm = FromProto<std::vector<TJobId>>(response->jobs_to_confirm());
        JobIdsToConfirm_.insert(jobIdsToConfirm.begin(), jobIdsToConfirm.end());
    }

    std::vector<TJobSpec> specs(response->jobs_to_start_size());

    auto startJob = [&] (const NJobTrackerClient::NProto::TJobStartInfo& startInfo, const TSharedRef& attachment) {
        TJobSpec spec;
        DeserializeProtoWithEnvelope(&spec, attachment);

        auto jobId = FromProto<TJobId>(startInfo.job_id());
        auto operationId = FromProto<TJobId>(startInfo.operation_id());
        const auto& resourceLimits = startInfo.resource_limits();

        CreateJob(jobId, operationId, resourceLimits, std::move(spec));
    };

    THashMap<TString, std::vector<NJobTrackerClient::NProto::TJobStartInfo>> groupedStartInfos;
    size_t attachmentIndex = 0;
    for (const auto& startInfo : response->jobs_to_start()) {
        auto operationId = FromProto<TJobId>(startInfo.operation_id());
        auto jobId = FromProto<TJobId>(startInfo.job_id());
        if (attachmentIndex < response->Attachments().size()) {
            // Start the job right away.
            YT_LOG_DEBUG("Job spec is passed via attachments (OperationId: %v, JobId: %v)",
                operationId,
                jobId);
            const auto& attachment = response->Attachments()[attachmentIndex];
            startJob(startInfo, attachment);
        } else {
            auto addresses = FromProto<NNodeTrackerClient::TAddressMap>(startInfo.spec_service_addresses());
            auto optionalAddress = FindAddress(addresses, Bootstrap_->GetLocalNetworks());
            if (optionalAddress) {
                const auto& address = *optionalAddress;
                YT_LOG_DEBUG("Job spec will be fetched (OperationId: %v, JobId: %v, SpecServiceAddress: %v)",
                    operationId,
                    jobId,
                    address);
                groupedStartInfos[address].push_back(startInfo);
            } else {
                YCHECK(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
                YT_LOG_DEBUG("Job spec cannot be fetched since no suitable network exists (OperationId: %v, JobId: %v, SpecServiceAddresses: %v)",
                    operationId,
                    jobId,
                    GetValues(addresses));
            }
        }
        ++attachmentIndex;
    }

    if (groupedStartInfos.empty()) {
        return;
    }

    auto getSpecServiceChannel = [&] (const auto& address) {
        const auto& client = Bootstrap_->GetMasterClient();
        const auto& channelFactory = client->GetNativeConnection()->GetChannelFactory();
        // COMPAT(babenko)
        return address
            ? channelFactory->CreateChannel(address)
            : client->GetSchedulerChannel();
    };

    std::vector<TFuture<void>> asyncResults;
    for (const auto& pair : groupedStartInfos) {
        const auto& address = pair.first;
        const auto& startInfos = pair.second;

        auto channel = getSpecServiceChannel(address);
        TJobSpecServiceProxy jobSpecServiceProxy(channel);
        jobSpecServiceProxy.SetDefaultTimeout(Config_->GetJobSpecsTimeout);
        auto jobSpecRequest = jobSpecServiceProxy.GetJobSpecs();

        for (const auto& startInfo : startInfos) {
            auto* subrequest = jobSpecRequest->add_requests();
            *subrequest->mutable_operation_id() = startInfo.operation_id();
            *subrequest->mutable_job_id() = startInfo.job_id();
        }

        YT_LOG_DEBUG("Getting job specs (SpecServiceAddress: %v, Count: %v)",
            address,
            startInfos.size());

        auto asyncResult = jobSpecRequest->Invoke().Apply(
            BIND([=, this_ = MakeStrong(this)] (const TJobSpecServiceProxy::TErrorOrRspGetJobSpecsPtr& rspOrError) {
                if (!rspOrError.IsOK()) {
                    YT_LOG_DEBUG(rspOrError, "Error getting job specs (SpecServiceAddress: %v)",
                        address);
                    for (const auto& startInfo : startInfos) {
                        auto jobId = FromProto<TJobId>(startInfo.job_id());
                        auto operationId = FromProto<TOperationId>(startInfo.operation_id());
                        YCHECK(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
                    }
                    return;
                }

                YT_LOG_DEBUG("Job specs received (SpecServiceAddress: %v)",
                    address);

                const auto& rsp = rspOrError.Value();
                YCHECK(rsp->responses_size() == startInfos.size());
                for (size_t  index = 0; index < startInfos.size(); ++index) {
                    const auto& startInfo = startInfos[index];
                    auto operationId = FromProto<TJobId>(startInfo.operation_id());
                    auto jobId = FromProto<TJobId>(startInfo.job_id());

                    const auto& subresponse = rsp->mutable_responses(index);
                    auto error = FromProto<TError>(subresponse->error());
                    if (!error.IsOK()) {
                        YCHECK(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
                        YT_LOG_DEBUG(error, "No spec is available for job (OperationId: %v, JobId: %v)",
                            operationId,
                            jobId);
                        continue;
                    }

                    const auto& attachment = rsp->Attachments()[index];
                    startJob(startInfo, attachment);
                }
            })
            .AsyncVia(Bootstrap_->GetControlInvoker()));
        asyncResults.push_back(asyncResult);
    }

    Y_UNUSED(WaitFor(CombineAll(asyncResults)));
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

TNodeMemoryTracker* TJobController::TImpl::GetUserMemoryUsageTracker()
{
    if (Bootstrap_->GetExecSlotManager()->ExternalJobMemory()) {
        return ExternalMemoryUsageTracker_.Get();
    } else {
        return Bootstrap_->GetMemoryUsageTracker();
    }
}

TNodeMemoryTracker* TJobController::TImpl::GetSystemMemoryUsageTracker()
{
    return Bootstrap_->GetMemoryUsageTracker();
}

const TNodeMemoryTracker* TJobController::TImpl::GetUserMemoryUsageTracker() const
{
    if (Bootstrap_->GetExecSlotManager()->ExternalJobMemory()) {
        return ExternalMemoryUsageTracker_.Get();
    } else {
        return Bootstrap_->GetMemoryUsageTracker();
    }
}

const TNodeMemoryTracker* TJobController::TImpl::GetSystemMemoryUsageTracker() const
{
    return Bootstrap_->GetMemoryUsageTracker();
}

i64 TJobController::TImpl::GetUserJobsFreeMemoryWatermark() const
{
    return Bootstrap_->GetExecSlotManager()->ExternalJobMemory()
        ? 0
        : Config_->FreeMemoryWatermark;
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
    Impl_->RegisterFactory(type, std::move(factory));
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
    EObjectType jobObjectType)
{
    Impl_->ProcessHeartbeatResponse(response, jobObjectType);
}

IYPathServicePtr TJobController::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_SIGNAL(TJobController, void(), ResourcesUpdated, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
