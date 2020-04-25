#include "job_controller.h"
#include "private.h"
#include "gpu_manager.h"

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>
#include <yt/server/node/cell_node/node_resource_manager.h>

#include <yt/server/node/data_node/master_connector.h>
#include <yt/server/node/data_node/chunk_cache.h>

#include <yt/server/node/exec_agent/slot_manager.h>

#include <yt/server/node/tablet_node/slot_manager.h>

#include <yt/server/lib/controller_agent/helpers.h>

#include <yt/server/lib/job_agent/job_reporter.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>
#include <yt/ytlib/job_tracker_client/job_spec_service_proxy.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/scheduler/public.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>
#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/atomic_object.h>

#include <yt/core/net/helpers.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <limits>

namespace NYT::NJobAgent {

using namespace NRpc;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NYson;
using namespace NYTree;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NScheduler;
using namespace NNet;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobSpec;
using NJobTrackerClient::NProto::TJobStatus;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobAgentServerLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TJobController::TImpl
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(), ResourcesUpdated);
    DEFINE_SIGNAL(void(const IJobPtr& job), JobFinished);

public:
    TImpl(
        TJobControllerConfigPtr config,
        TBootstrap* bootstrap);

    void Initialize();

    void RegisterJobFactory(
        EJobType type,
        TJobFactory factory);

    IJobPtr FindJob(TJobId jobId) const;
    IJobPtr GetJobOrThrow(TJobId jobId) const;
    IJobPtr FindRecentlyRemovedJob(TJobId jobId) const;
    std::vector<IJobPtr> GetJobs() const;

    TNodeResources GetResourceLimits() const;
    TNodeResources GetResourceUsage(bool includeWaiting = false) const;
    TDiskResources GetDiskResources() const;
    void SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits);

    void SetDisableSchedulerJobs(bool value);

    TFuture<void> PrepareHeartbeatRequest(
        TCellTag cellTag,
        EObjectType jobObjectType,
        const TReqHeartbeatPtr& request);
    TFuture<void> ProcessHeartbeatResponse(
        const TRspHeartbeatPtr& response,
        EObjectType jobObjectType);

    NYTree::IYPathServicePtr GetOrchidService();

private:
    const TJobControllerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    THashMap<EJobType, TJobFactory> JobFactoryMap_;

    NConcurrency::TReaderWriterSpinLock JobMapLock_;
    THashMap<TJobId, IJobPtr> JobMap_;

    // Map of jobs to hold after remove. It is used to prolong lifetime of stderrs and job specs.
    struct TRecentlyRemovedJobRecord
    {
        IJobPtr Job;
        TInstant RemovalTime;
    };
    THashMap<TJobId, TRecentlyRemovedJobRecord> RecentlyRemovedJobMap_;

    //! Jobs that did not succeed in fetching spec are not getting
    //! their IJob structure, so we have to store job id alongside
    //! with the operation id to fill the TJobStatus proto message
    //! properly.
    THashMap<TJobId, TOperationId> SpecFetchFailedJobIds_;

    bool StartScheduled_ = false;

    std::atomic<bool> DisableSchedulerJobs_ = false;

    IThroughputThrottlerPtr StatisticsThrottler_;

    TAtomicObject<TNodeResourceLimitsOverrides> ResourceLimitsOverrides_;

    std::optional<TInstant> UserMemoryOverdraftInstant_;
    std::optional<TInstant> CpuOverdraftInstant_;

    TProfiler Profiler_;
    TProfiler ResourceLimitsProfiler_;
    TProfiler ResourceUsageProfiler_;
    TProfiler GpuUtilizationProfiler_;
    TEnumIndexedVector<EJobOrigin, TTagId> JobOriginToTag_;
    THashMap<int, TTagId> GpuDeviceNumberToProfilingTag_;
    THashMap<TString, TTagId> GpuNameToProfilingTag_;

    THashMap<std::pair<EJobState, EJobOrigin>, TMonotonicCounter> JobFinalStateCounters_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr ResourceAdjustmentExecutor_;
    TPeriodicExecutorPtr RecentlyRemovedJobCleaner_;
    TPeriodicExecutorPtr ReservedMappedMemoryChecker_;

    THashSet<TJobId> JobIdsToConfirm_;
    TInstant LastStoredJobsSendTime_;

    THashSet<int> FreePorts_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    //! Starts a new job.
    IJobPtr CreateJob(
        TJobId jobId,
        TOperationId operationId,
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
    void RemoveJob(
        const IJobPtr& job,
        const TReleaseJobFlags& releaseFlags);

    std::vector<IJobPtr> GetRunningSchedulerJobsSortedByStartTime() const;

    TJobFactory GetFactory(EJobType type) const;

    void ScheduleStart();

    void OnWaitingJobTimeout(const TWeakPtr<IJob>& weakJob);

    void OnResourcesUpdated(
        const TWeakPtr<IJob>& job,
        const TNodeResources& resourceDelta);

    void OnPortsReleased(const TWeakPtr<IJob>& job);

    void OnJobFinished(const TWeakPtr<IJob>& job);

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

    TEnumIndexedVector<EJobOrigin, std::vector<IJobPtr>> GetJobsByOrigin() const;

    void CleanRecentlyRemovedJobs();

    void CheckReservedMappedMemory();

    TMonotonicCounter* GetJobFinalStateCounter(EJobState state, EJobOrigin origin);
};

////////////////////////////////////////////////////////////////////////////////

TJobController::TImpl::TImpl(
    TJobControllerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(Config_->StatisticsThrottler))
    , Profiler_("/job_controller")
    , ResourceLimitsProfiler_(Profiler_.AppendPath("/resource_limits"))
    , ResourceUsageProfiler_(Profiler_.AppendPath("/resource_usage"))
    , GpuUtilizationProfiler_(Profiler_.AppendPath("/gpu_utilization"))
{
    YT_VERIFY(Config_);
    YT_VERIFY(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (Config_->PortSet) {
        FreePorts_ = *Config_->PortSet;
    } else {
        for (int index = 0; index < Config_->PortCount; ++index) {
            FreePorts_.insert(Config_->StartPort + index);
        }
    }
}

void TJobController::TImpl::Initialize()
{
    Bootstrap_->GetMemoryUsageTracker()->SetCategoryLimit(
        EMemoryCategory::UserJobs,
        Config_->ResourceLimits->UserMemory);

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TImpl::OnProfiling, MakeWeak(this)),
        ProfilingPeriod);
    ProfilingExecutor_->Start();

    ResourceAdjustmentExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TImpl::AdjustResources, MakeWeak(this)),
        Config_->ResourceAdjustmentPeriod);
    ResourceAdjustmentExecutor_->Start();

    RecentlyRemovedJobCleaner_ = New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TImpl::CleanRecentlyRemovedJobs, MakeWeak(this)),
        Config_->RecentlyRemovedJobsCleanPeriod);
    RecentlyRemovedJobCleaner_->Start();

    if (Config_->MappedMemoryController) {
        ReservedMappedMemoryChecker_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND(&TImpl::CheckReservedMappedMemory, MakeWeak(this)),
            Config_->MappedMemoryController->CheckPeriod);
        ReservedMappedMemoryChecker_->Start();
    }
}

void TJobController::TImpl::RegisterJobFactory(EJobType type, TJobFactory factory)
{
    YT_VERIFY(JobFactoryMap_.emplace(type, factory).second);
}

TJobFactory TJobController::TImpl::GetFactory(EJobType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetOrCrash(JobFactoryMap_, type);
}

IJobPtr TJobController::TImpl::FindJob(TJobId jobId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    NConcurrency::TReaderGuard guard(JobMapLock_);
    auto it = JobMap_.find(jobId);
    return it == JobMap_.end() ? nullptr : it->second;
}

IJobPtr TJobController::TImpl::GetJobOrThrow(TJobId jobId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

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

IJobPtr TJobController::TImpl::FindRecentlyRemovedJob(TJobId jobId) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto it = RecentlyRemovedJobMap_.find(jobId);
    return it == RecentlyRemovedJobMap_.end() ? nullptr : it->second.Job;
}

std::vector<IJobPtr> TJobController::TImpl::GetJobs() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    NConcurrency::TReaderGuard guard(JobMapLock_);
    std::vector<IJobPtr> result;
    result.reserve(JobMap_.size());
    for (const auto& [id, job] : JobMap_) {
        result.push_back(job);
    }
    return result;
}

std::vector<IJobPtr> TJobController::TImpl::GetRunningSchedulerJobsSortedByStartTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<IJobPtr> schedulerJobs;
    for (const auto& job : GetJobs()) {
        if (TypeFromId(job->GetId()) == EObjectType::SchedulerJob && job->GetState() == EJobState::Running) {
            schedulerJobs.push_back(job);
        }
    }

    std::sort(schedulerJobs.begin(), schedulerJobs.end(), [] (const IJobPtr& lhs, const IJobPtr& rhs) {
        return lhs->GetStartTime() < rhs->GetStartTime();
    });

    return schedulerJobs;
}

TNodeResources TJobController::TImpl::GetResourceLimits() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TNodeResources result;

    // If chunk cache is disabled, we disable all scheduler jobs.
    result.set_user_slots(Bootstrap_->GetChunkCache()->IsEnabled() && !DisableSchedulerJobs_.load() && !Bootstrap_->IsReadOnly()
        ? Bootstrap_->GetExecSlotManager()->GetSlotCount()
        : 0);

    auto resourceLimitsOverrides = ResourceLimitsOverrides_.Load();
    #define XX(name, Name) \
        result.set_##name(resourceLimitsOverrides.has_##name() \
            ? resourceLimitsOverrides.name() \
            : Config_->ResourceLimits->Name);
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX

    if (!Config_->TestGpuResource) {
        result.set_gpu(Bootstrap_->GetGpuManager()->GetTotalGpuCount());
    }

    const auto& memoryUsageTracker = Bootstrap_->GetMemoryUsageTracker();

    // NB: Some categories can have no explicit limit.
    // Therefore we need bound memory limit by actually available memory.
    result.set_user_memory(std::min(
        memoryUsageTracker->GetLimit(EMemoryCategory::UserJobs),
        memoryUsageTracker->GetUsed(EMemoryCategory::UserJobs) + memoryUsageTracker->GetTotalFree() - Config_->FreeMemoryWatermark));
    result.set_system_memory(std::min(
        memoryUsageTracker->GetLimit(EMemoryCategory::SystemJobs),
        memoryUsageTracker->GetUsed(EMemoryCategory::SystemJobs) + memoryUsageTracker->GetTotalFree() - Config_->FreeMemoryWatermark));

    const auto& nodeResourceManager = Bootstrap_->GetNodeResourceManager();
    result.set_cpu(nodeResourceManager->GetJobsCpuLimit());

    return result;
}

TNodeResources TJobController::TImpl::GetResourceUsage(bool includeWaiting) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto result = ZeroNodeResources();
    for (const auto& job : GetJobs()) {
        if (includeWaiting || job->GetState() != EJobState::Waiting) {
            result += job->GetResourceUsage();
        }
    }

    result.set_user_slots(Bootstrap_->GetExecSlotManager()->GetUsedSlotCount());
    return result;
}

void TJobController::TImpl::AdjustResources()
{
    VERIFY_THREAD_AFFINITY(JobThread);

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
        auto schedulerJobs = GetRunningSchedulerJobsSortedByStartTime();

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

void TJobController::TImpl::CleanRecentlyRemovedJobs()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto now = TInstant::Now();

    std::vector<TJobId> jobIdsToRemove;
    for (const auto& [jobId, jobRecord] : RecentlyRemovedJobMap_) {
        if (jobRecord.RemovalTime + Config_->RecentlyRemovedJobsStoreTimeout < now) {
            jobIdsToRemove.push_back(jobId);
        }
    }

    for (auto jobId : jobIdsToRemove) {
        YT_LOG_INFO("Job is finally removed (JobId: %v)", jobId);
        RecentlyRemovedJobMap_.erase(jobId);
    }
}

void TJobController::TImpl::CheckReservedMappedMemory()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Check mapped memory usage");

    THashMap<TString, i64> vmstat;
    try {
        vmstat = GetVmstat();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to read /proc/vmstat; skipping mapped memory check");
        return;
    }

    auto mappedIt = vmstat.find("nr_mapped");
    if (mappedIt == vmstat.end()) {
        YT_LOG_WARNING("Field \"nr_mapped\" is not found in /proc/vmstat; skipping mapped memory check");
        return;
    }


    i64 mappedMemory = mappedIt->second;

    YT_LOG_INFO("Mapped memory usage (Usage: %v, Reserved: %v)",
        mappedMemory,
        Config_->MappedMemoryController->ReservedMemory);

    if (mappedMemory <= Config_->MappedMemoryController->ReservedMemory) {
        return;
    }

    auto schedulerJobs = GetRunningSchedulerJobsSortedByStartTime();

    auto usage = GetResourceUsage(false);
    auto limits = GetResourceLimits();
    while (usage.user_memory() + mappedMemory > limits.user_memory()) {
        if (schedulerJobs.empty()) {
            break;
        }

        usage -= schedulerJobs.back()->GetResourceUsage();
        schedulerJobs.back()->Abort(TError(
            NExecAgent::EErrorCode::ResourceOverdraft,
            "Mapped memory usage overdraft"));
        schedulerJobs.pop_back();
    }
}

TDiskResources TJobController::TImpl::GetDiskResources() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return Bootstrap_->GetExecSlotManager()->GetDiskResources();
}

void TJobController::TImpl::SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ResourceLimitsOverrides_.Store(resourceLimits);
}

void TJobController::TImpl::SetDisableSchedulerJobs(bool value)
{
    VERIFY_THREAD_AFFINITY_ANY();

    DisableSchedulerJobs_.store(value);

    if (value) {
        Bootstrap_->GetJobInvoker()->Invoke(BIND([=, this_ = MakeStrong(this)] {
            VERIFY_THREAD_AFFINITY(JobThread);
        
            for (const auto& job : GetJobs()) {
                auto jobId = job->GetId();
                if (TypeFromId(jobId) == EObjectType::SchedulerJob && job->GetState() != EJobState::Running) {
                    try {
                        YT_LOG_DEBUG("All scheduler jobs are disabled; trying to interrupt (JobId: %v)",
                            jobId);
                        job->Interrupt();
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Failed to interrupt scheduler job (JobId: %v)",
                            jobId);
                    }
                }
            }
        }));
    }
}

void TJobController::TImpl::StartWaitingJobs()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    bool resourcesUpdated = false;

    {
        auto usedResources = GetResourceUsage();

        auto memoryToRelease = Bootstrap_->GetMemoryUsageTracker()->GetUsed(EMemoryCategory::UserJobs) - usedResources.user_memory();
        if (memoryToRelease > 0) {
            Bootstrap_->GetMemoryUsageTracker()->Release(EMemoryCategory::UserJobs, memoryToRelease);
            resourcesUpdated = true;
        }

        memoryToRelease = Bootstrap_->GetMemoryUsageTracker()->GetUsed(EMemoryCategory::SystemJobs) - usedResources.system_memory();
        if (memoryToRelease > 0) {
            Bootstrap_->GetMemoryUsageTracker()->Release(EMemoryCategory::SystemJobs, memoryToRelease);
            resourcesUpdated = true;
        }
    }

    for (const auto& job : GetJobs()) {
        if (job->GetState() != EJobState::Waiting) {
            continue;
        }

        auto jobLogger = NLogging::TLogger(JobAgentServerLogger)
            .AddTag("JobId: %v", job->GetId());

        const auto& Logger = jobLogger;

        auto portCount = job->GetPortCount();

        auto jobResources = job->GetResourceUsage();
        auto usedResources = GetResourceUsage();
        if (!HasEnoughResources(jobResources, usedResources)) {
            YT_LOG_DEBUG("Not enough resources to start waiting job (JobResources: %v, UsedResources: %v)",
                FormatResources(jobResources),
                FormatResourceUsage(usedResources, GetResourceLimits()));
            continue;
        }

        if (jobResources.user_memory() > 0) {
            bool reachedWatermark = Bootstrap_->GetMemoryUsageTracker()->GetTotalFree() <= Config_->FreeMemoryWatermark;
            if (reachedWatermark) {
                YT_LOG_DEBUG("Not enough memory to start waiting job; reached free memory watermark");
                continue;
            }

            auto error = Bootstrap_->GetMemoryUsageTracker()->TryAcquire(EMemoryCategory::UserJobs, jobResources.user_memory());
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Not enough memory to start waiting job");
                continue;
            }
        }

        if (jobResources.system_memory() > 0) {
            bool reachedWatermark = Bootstrap_->GetMemoryUsageTracker()->GetTotalFree() <= Config_->FreeMemoryWatermark;
            if (reachedWatermark) {
                YT_LOG_DEBUG("Not enough memory to start waiting job; reached free memory watermark");
                continue;
            }

            auto error = Bootstrap_->GetMemoryUsageTracker()->TryAcquire(EMemoryCategory::SystemJobs, jobResources.system_memory());
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
                .Via(Bootstrap_->GetJobInvoker()));

        job->SubscribePortsReleased(
            BIND(&TImpl::OnPortsReleased, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetJobInvoker()));

        job->SubscribeJobFinished(
            BIND(&TImpl::OnJobFinished, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetJobInvoker()));

        job->Start();

        resourcesUpdated = true;
    }

    if (resourcesUpdated) {
        ResourcesUpdated_.Fire();
    }

    StartScheduled_ = false;
}

IJobPtr TJobController::TImpl::CreateJob(
    TJobId jobId,
    TOperationId operationId,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto type = CheckedEnumCast<EJobType>(jobSpec.type());
    auto factory = GetFactory(type);

    auto extensionId = NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext;
    TDuration waitingJobTimeout = Config_->WaitingJobsTimeout;
    if (jobSpec.HasExtension(extensionId)) {
        const auto& extension = jobSpec.GetExtension(extensionId);
        if (extension.has_waiting_job_timeout()) {
            waitingJobTimeout = FromProto<TDuration>(extension.waiting_job_timeout());
        }
    }

    auto job = factory.Run(
        jobId,
        operationId,
        resourceLimits,
        std::move(jobSpec));

    YT_LOG_INFO("Job created (JobId: %v, OperationId: %v, JobType: %v)",
        jobId,
        operationId,
        type);

    {
        NConcurrency::TWriterGuard guard(JobMapLock_);
        YT_VERIFY(JobMap_.emplace(jobId, job).second);
    }

    ScheduleStart();

    // Use #Apply instead of #Subscribe to match #OnWaitingJobTimeout signature.
    TDelayedExecutor::MakeDelayed(waitingJobTimeout)
        .Apply(BIND(&TImpl::OnWaitingJobTimeout, MakeWeak(this), MakeWeak(job))
        .Via(Bootstrap_->GetJobInvoker()));

    return job;
}

void TJobController::TImpl::OnWaitingJobTimeout(const TWeakPtr<IJob>& weakJob)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto job = weakJob.Lock();
    if (!job) {
        return;
    }

    if (job->GetState() == EJobState::Waiting) {
        job->Abort(TError(NExecAgent::EErrorCode::WaitingJobTimeout, "Job waiting has timed out")
            << TErrorAttribute("timeout", Config_->WaitingJobsTimeout));
    }
}

void TJobController::TImpl::ScheduleStart()
{
    if (StartScheduled_) {
        return;
    }

    Bootstrap_->GetJobInvoker()->Invoke(BIND(
        &TImpl::StartWaitingJobs,
        MakeWeak(this)));
    StartScheduled_ = true;
}

void TJobController::TImpl::AbortJob(const IJobPtr& job)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Job abort requested (JobId: %v)",
        job->GetId());

    job->Abort(TError(NExecAgent::EErrorCode::AbortByScheduler, "Job aborted by scheduler"));
}

void TJobController::TImpl::FailJob(const IJobPtr& job)
{
    VERIFY_THREAD_AFFINITY(JobThread);

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
    VERIFY_THREAD_AFFINITY(JobThread);


    YT_LOG_INFO("Job interrupt requested (JobId: %v)",
        job->GetId());

    try {
        job->Interrupt();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to interrupt job (JobId: %v)", job->GetId());
    }
}

void TJobController::TImpl::RemoveJob(
    const IJobPtr& job,
    const TReleaseJobFlags& releaseFlags)
{
    VERIFY_THREAD_AFFINITY(JobThread);
    YT_VERIFY(job->GetPhase() >= EJobPhase::Cleanup);
    YT_VERIFY(job->GetResourceUsage() == ZeroNodeResources());

    if (releaseFlags.ArchiveJobSpec) {
        YT_LOG_INFO("Archiving job spec (JobId: %v)", job->GetId());
        job->ReportSpec();
    }

    if (releaseFlags.ArchiveStderr) {
        YT_LOG_INFO("Archiving stderr (JobId: %v)", job->GetId());
        job->ReportStderr();
    } else {
        // We report zero stderr size to make dynamic tables with jobs and stderrs consistent.
        YT_LOG_INFO("Stderr will not be archived, reporting zero stderr size (JobId: %v)", job->GetId());
        job->SetStderrSize(0);
    }

    if (releaseFlags.ArchiveFailContext) {
        YT_LOG_INFO("Archiving fail context (JobId: %v)", job->GetId());
        job->ReportFailContext();
    }

    if (releaseFlags.ArchiveProfile) {
        YT_LOG_INFO("Archiving profile (JobId: %v)", job->GetId());
        job->ReportProfile();
    }

    bool shouldSave = releaseFlags.ArchiveJobSpec || releaseFlags.ArchiveStderr;
    if (shouldSave) {
        YT_LOG_INFO("Job saved to recently finished jobs (JobId: %v)", job->GetId());
        RecentlyRemovedJobMap_.emplace(job->GetId(), TRecentlyRemovedJobRecord{job, TInstant::Now()});
    }

    {
        NConcurrency::TWriterGuard guard(JobMapLock_);
        YT_VERIFY(JobMap_.erase(job->GetId()) == 1);
    }

    YT_LOG_INFO("Job removed (JobId: %v, Save: %v)", job->GetId(), shouldSave);
}

void TJobController::TImpl::OnResourcesUpdated(const TWeakPtr<IJob>& job, const TNodeResources& resourceDelta)
{
    VERIFY_THREAD_AFFINITY(JobThread);

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

void TJobController::TImpl::OnPortsReleased(const TWeakPtr<IJob>& weakJob)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto job = weakJob.Lock();
    if (!job) {
        return;
    }

    const auto& ports = job->GetPorts();
    YT_LOG_INFO("Releasing ports (JobId: %v, PortCount: %v, Ports: %v)",
        job->GetId(),
        ports.size(),
        ports);
    for (auto port : ports) {
        YT_VERIFY(FreePorts_.insert(port).second);
    }
}

void TJobController::TImpl::OnJobFinished(const TWeakPtr<IJob>& weakJob)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto job = weakJob.Lock();
    if (!job) {
        return;
    }

    EJobOrigin origin;
    switch (TypeFromId(job->GetId())) {
        case EObjectType::SchedulerJob:
            origin = EJobOrigin::Scheduler;
            break;

        case EObjectType::MasterJob:
            origin = EJobOrigin::Master;
            break;

        default:
            YT_ABORT();
    }

    auto* jobFinalStateCounter = GetJobFinalStateCounter(job->GetState(), origin);
    Profiler_.Increment(*jobFinalStateCounter);

    JobFinished_.Fire(job);
}

bool TJobController::TImpl::CheckMemoryOverdraft(const TNodeResources& delta)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    // Only cpu and user_memory can be increased.
    // Network decreases by design. Cpu increasing is handled in AdjustResources.
    // Others are not reported by job proxy (see TSupervisorService::UpdateResourceUsage).

    if (delta.user_memory() > 0) {
        bool reachedWatermark = Bootstrap_->GetMemoryUsageTracker()->GetTotalFree() <= Config_->FreeMemoryWatermark;
        if (reachedWatermark) {
            return false;
        }

        auto error = Bootstrap_->GetMemoryUsageTracker()->TryAcquire(EMemoryCategory::UserJobs, delta.user_memory());
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
    VERIFY_THREAD_AFFINITY(JobThread);

    auto totalResources = GetResourceLimits();
    auto spareResources = MakeNonnegative(totalResources - usedResources);
    // Allow replication/repair data size overcommit.
    spareResources.set_replication_data_size(InfiniteNodeResources().replication_data_size());
    spareResources.set_repair_data_size(InfiniteNodeResources().repair_data_size());
    return Dominates(spareResources, jobResources);
}

TFuture<void> TJobController::TImpl::PrepareHeartbeatRequest(
    TCellTag cellTag,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_THREAD_AFFINITY(JobThread);

            const auto& masterConnector = Bootstrap_->GetMasterConnector();
            request->set_node_id(masterConnector->GetNodeId());
            ToProto(request->mutable_node_descriptor(), masterConnector->GetLocalDescriptor());
            *request->mutable_resource_limits() = GetResourceLimits();
            *request->mutable_resource_usage() = GetResourceUsage(/* includeWaiting */ true);

            *request->mutable_disk_resources() = GetDiskResources();

            request->set_job_reporter_write_failures_count(Bootstrap_->GetJobReporter()->ExtractWriteFailuresCount());
            request->set_job_reporter_queue_is_too_large(Bootstrap_->GetJobReporter()->GetQueueIsTooLarge());

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

            if (jobObjectType == EObjectType::SchedulerJob && !Bootstrap_->GetExecSlotManager()->IsEnabled()) {
                // NB(psushin): if slot manager is disabled we might have experienced an unrecoverable failure (e.g. hanging porto)
                // and to avoid inconsistent state with scheduler we decide not to report to it any jobs at all.
                request->set_confirmed_job_count(0);
                return;
            }

            int confirmedJobCount = 0;

            for (const auto& job : GetJobs()) {
                auto jobId = job->GetId();

                if (CellTagFromId(jobId) != cellTag || TypeFromId(jobId) != jobObjectType) {
                    continue;
                }

                auto confirmIt = JobIdsToConfirm_.find(jobId);
                if (job->GetStored() && !totalConfirmation && confirmIt == JobIdsToConfirm_.end()) {
                    continue;
                }

                if (job->GetStored() || confirmIt != JobIdsToConfirm_.end()) {
                    YT_LOG_DEBUG("Confirming job (JobId: %v, OperationId: %v, Stored: %v, State: %v)",
                        jobId,
                        job->GetOperationId(),
                        job->GetStored(),
                        job->GetState());
                    ++confirmedJobCount;
                }
                if (confirmIt != JobIdsToConfirm_.end()) {
                    JobIdsToConfirm_.erase(confirmIt);
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

                for (const auto& [job, jobStatus] : runningJobs) {
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
                for (auto [jobId, operationId] : SpecFetchFailedJobIds_) {
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
                    for (auto jobId : JobIdsToConfirm_) {
                        YT_LOG_DEBUG("Unconfirmed job (JobId: %v)", jobId);
                    }
                    ToProto(request->mutable_unconfirmed_jobs(), JobIdsToConfirm_);
                }
            }
        })
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run();
}

TFuture<void> TJobController::TImpl::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_THREAD_AFFINITY(JobThread);

            for (const auto& protoJobToRemove : response->jobs_to_remove()) {
                auto jobToRemove = FromProto<TJobToRelease>(protoJobToRemove);
                auto jobId = jobToRemove.JobId;
                if (SpecFetchFailedJobIds_.erase(jobId) == 1) {
                    continue;
                }

                auto job = FindJob(jobId);
                if (job) {
                    RemoveJob(job, jobToRemove.ReleaseFlags);
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

            THashMap<TAddressWithNetwork, std::vector<NJobTrackerClient::NProto::TJobStartInfo>> groupedStartInfos;
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
                    try {
                        auto addressWithNetwork = GetAddressWithNetworkOrThrow(addresses, Bootstrap_->GetLocalNetworks());
                        YT_LOG_DEBUG("Job spec will be fetched (OperationId: %v, JobId: %v, SpecServiceAddress: %v)",
                            operationId,
                            jobId,
                            addressWithNetwork.Address);
                        groupedStartInfos[addressWithNetwork].push_back(startInfo);
                    } catch (const std::exception& ex) {
                        YT_VERIFY(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
                        YT_LOG_DEBUG(ex, "Job spec cannot be fetched since no suitable network exists (OperationId: %v, JobId: %v, SpecServiceAddresses: %v)",
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

            auto getSpecServiceChannel = [&] (const auto& addressWithNetwork) {
                const auto& client = Bootstrap_->GetMasterClient();
                const auto& channelFactory = client->GetNativeConnection()->GetChannelFactory();
                return channelFactory->CreateChannel(addressWithNetwork);
            };

            std::vector<TFuture<void>> asyncResults;
            for (const auto& pair : groupedStartInfos) {
                const auto& addressWithNetwork = pair.first;
                const auto& startInfos = pair.second;

                auto channel = getSpecServiceChannel(addressWithNetwork);
                TJobSpecServiceProxy jobSpecServiceProxy(channel);
                jobSpecServiceProxy.SetDefaultTimeout(Config_->GetJobSpecsTimeout);
                auto jobSpecRequest = jobSpecServiceProxy.GetJobSpecs();

                for (const auto& startInfo : startInfos) {
                    auto* subrequest = jobSpecRequest->add_requests();
                    *subrequest->mutable_operation_id() = startInfo.operation_id();
                    *subrequest->mutable_job_id() = startInfo.job_id();
                }

                YT_LOG_DEBUG("Getting job specs (SpecServiceAddress: %v, Count: %v)",
                    addressWithNetwork,
                    startInfos.size());

                auto asyncResult = jobSpecRequest->Invoke().Apply(
                    BIND([=, this_ = MakeStrong(this)] (const TJobSpecServiceProxy::TErrorOrRspGetJobSpecsPtr& rspOrError) {
                        if (!rspOrError.IsOK()) {
                            YT_LOG_DEBUG(rspOrError, "Error getting job specs (SpecServiceAddress: %v)",
                                addressWithNetwork);
                            for (const auto& startInfo : startInfos) {
                                auto jobId = FromProto<TJobId>(startInfo.job_id());
                                auto operationId = FromProto<TOperationId>(startInfo.operation_id());
                                YT_VERIFY(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
                            }
                            return;
                        }

                        YT_LOG_DEBUG("Job specs received (SpecServiceAddress: %v)",
                            addressWithNetwork);

                        const auto& rsp = rspOrError.Value();
                        YT_VERIFY(rsp->responses_size() == startInfos.size());
                        for (size_t  index = 0; index < startInfos.size(); ++index) {
                            const auto& startInfo = startInfos[index];
                            auto operationId = FromProto<TJobId>(startInfo.operation_id());
                            auto jobId = FromProto<TJobId>(startInfo.job_id());

                            const auto& subresponse = rsp->mutable_responses(index);
                            auto error = FromProto<TError>(subresponse->error());
                            if (!error.IsOK()) {
                                YT_VERIFY(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
                                YT_LOG_DEBUG(error, "No spec is available for job (OperationId: %v, JobId: %v)",
                                    operationId,
                                    jobId);
                                continue;
                            }

                            const auto& attachment = rsp->Attachments()[index];
                            startJob(startInfo, attachment);
                        }
                    })
                    .AsyncVia(Bootstrap_->GetJobInvoker()));
                asyncResults.push_back(asyncResult);
            }

            Y_UNUSED(WaitFor(CombineAll(asyncResults)));
        })
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run();
}

TEnumIndexedVector<EJobOrigin, std::vector<IJobPtr>> TJobController::TImpl::GetJobsByOrigin() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TEnumIndexedVector<EJobOrigin, std::vector<IJobPtr>> result;
    for (const auto& job : GetJobs()) {
        switch (TypeFromId(job->GetId())) {
            case EObjectType::MasterJob:
                result[EJobOrigin::Master].push_back(job);
                break;
            case EObjectType::SchedulerJob:
                result[EJobOrigin::Scheduler].push_back(job);
                break;
            default:
                YT_ABORT();
        }
    }
    return result;
}

void TJobController::TImpl::BuildOrchid(IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

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
                                    .Item("slot_index").Value(job->GetSlotIndex())
                                    .Item("start_time").Value(job->GetStartTime())
                                    .Item("duration").Value(TInstant::Now() - job->GetStartTime())
                                    .OptionalItem("statistics", job->GetStatistics())
                                    .OptionalItem("operation_id", job->GetOperationId())
                                .EndMap();
                        });
                })
            .Item("gpu_utilization").DoMapFor(
                Bootstrap_->GetGpuManager()->GetGpuInfoMap(),
                [&] (TFluentMap fluent, const auto& pair) {
                    const auto& [_, gpuInfo] = pair;
                    fluent.Item(ToString(gpuInfo.Index))
                        .BeginMap()
                            .Item("update_time").Value(gpuInfo.UpdateTime)
                            .Item("utilization_gpu_rate").Value(gpuInfo.UtilizationGpuRate)
                            .Item("utilization_memory_rate").Value(gpuInfo.UtilizationMemoryRate)
                            .Item("memory_used").Value(gpuInfo.MemoryUsed)
                        .EndMap();
                }
            )
            .Item("slot_manager").DoMap(BIND(
                &NExecAgent::TSlotManager::BuildOrchidYson,
                Bootstrap_->GetExecSlotManager()))

        .EndMap();
}

IYPathServicePtr TJobController::TImpl::GetOrchidService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IYPathService::FromProducer(BIND(&TImpl::BuildOrchid, MakeStrong(this)))
        ->Via(Bootstrap_->GetJobInvoker());
}

void TJobController::TImpl::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto jobs = GetJobsByOrigin();
    static const TEnumMemberTagCache<EJobOrigin> JobOriginTagCache("origin");
    for (auto origin : TEnumTraits<EJobOrigin>::GetDomainValues()) {
        Profiler_.Enqueue(
            "/active_job_count",
            jobs[origin].size(),
            EMetricType::Gauge,
            {JobOriginTagCache.GetTag(origin)});
    }
    ProfileResources(ResourceUsageProfiler_, GetResourceUsage());
    ProfileResources(ResourceLimitsProfiler_, GetResourceLimits());

    for (const auto& [index, gpuInfo] : Bootstrap_->GetGpuManager()->GetGpuInfoMap()) {
        TTagId deviceNumberTag;
        {
            auto it = GpuDeviceNumberToProfilingTag_.find(index);
            if (it == GpuDeviceNumberToProfilingTag_.end()) {
                it = GpuDeviceNumberToProfilingTag_.emplace(index, TProfileManager::Get()->RegisterTag("device_number", ToString(index))).first;
            }
            deviceNumberTag = it->second;
        }
        TTagId nameTag;
        {
            auto it = GpuNameToProfilingTag_.find(gpuInfo.Name);
            if (it == GpuNameToProfilingTag_.end()) {
                it = GpuNameToProfilingTag_.emplace(gpuInfo.Name, TProfileManager::Get()->RegisterTag("gpu_name", gpuInfo.Name)).first;
            }
            nameTag = it->second;
        }
        ProfileGpuInfo(GpuUtilizationProfiler_, gpuInfo, {deviceNumberTag, nameTag});
    }
}

TMonotonicCounter* TJobController::TImpl::GetJobFinalStateCounter(EJobState state, EJobOrigin origin)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto key = std::make_pair(state, origin);
    auto it = JobFinalStateCounters_.find(key);
    if (it == JobFinalStateCounters_.end()) {
        TMonotonicCounter counter{
            "/job_final_state",
            {
                TProfileManager::Get()->RegisterTag("state", state),
                TProfileManager::Get()->RegisterTag("origin", origin)
            }
        };
        it = JobFinalStateCounters_.emplace(key, std::move(counter)).first;
    }

    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

TJobController::TJobController(
    TJobControllerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        bootstrap))
{ }

TJobController::~TJobController() = default;

void TJobController::Initialize()
{
    Impl_->Initialize();
}

void TJobController::RegisterJobFactory(
    EJobType type,
    TJobFactory factory)
{
    Impl_->RegisterJobFactory(type, std::move(factory));
}

IJobPtr TJobController::FindJob(TJobId jobId) const
{
    return Impl_->FindJob(jobId);
}

IJobPtr TJobController::GetJobOrThrow(TJobId jobId) const
{
    return Impl_->GetJobOrThrow(jobId);
}

IJobPtr TJobController::FindRecentlyRemovedJob(TJobId jobId) const
{
    return Impl_->FindRecentlyRemovedJob(jobId);
}

std::vector<IJobPtr> TJobController::GetJobs() const
{
    return Impl_->GetJobs();
}

TNodeResources TJobController::GetResourceLimits() const
{
    return Impl_->GetResourceLimits();
}

void TJobController::SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits)
{
    Impl_->SetResourceLimitsOverrides(resourceLimits);
}

void TJobController::SetDisableSchedulerJobs(bool value)
{
    Impl_->SetDisableSchedulerJobs(value);
}

TFuture<void> TJobController::PrepareHeartbeatRequest(
    TCellTag cellTag,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    return Impl_->PrepareHeartbeatRequest(cellTag, jobObjectType, request);
}

TFuture<void> TJobController::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    return Impl_->ProcessHeartbeatResponse(response, jobObjectType);
}

IYPathServicePtr TJobController::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_SIGNAL(TJobController, void(), ResourcesUpdated, *Impl_)
DELEGATE_SIGNAL(TJobController, void(const IJobPtr&), JobFinished, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
