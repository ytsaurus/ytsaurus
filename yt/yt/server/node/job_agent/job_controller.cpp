#include "job_controller.h"

#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>
#include <yt/yt/server/node/exec_node/gpu_manager.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/tablet_node/slot_manager.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/job_agent/gpu_helpers.h>
#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/ytlib/job_tracker_client/helpers.h>
#include <yt/yt/ytlib/job_tracker_client/job_spec_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/process/process.h>
#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/net/helpers.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <util/generic/algorithm.h>

#include <limits>

namespace NYT::NJobAgent {

using namespace NRpc;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;
using namespace NYson;
using namespace NYTree;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NScheduler;
using namespace NNet;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobSpec;
using NJobTrackerClient::NProto::TJobStatus;
using NNodeTrackerClient::NProto::TNodeResources;
using NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides;
using NNodeTrackerClient::NProto::TDiskResources;

using NYT::FromProto;
using NYT::ToProto;

using std::placeholders::_1;

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
    DEFINE_SIGNAL(void(const TError& error), JobProxyBuildInfoUpdated);

public:
    TImpl(
        TJobControllerConfigPtr config,
        IBootstrapBase* bootstrap);

    void Initialize();

    void RegisterJobFactory(
        EJobType type,
        TJobFactory factory);

    void RegisterHeartbeatProcessor(
        EObjectType type,
        TJobHeartbeatProcessorBasePtr heartbeatProcessor);

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

    TFuture<void> RequestJobSpecsAndStartJobs(
        std::vector<NJobTrackerClient::NProto::TJobStartInfo> jobStartInfos);

    bool IsJobProxyProfilingDisabled() const;

private:
    friend class TJobController::TJobHeartbeatProcessorBase;

    const TJobControllerConfigPtr Config_;
    NClusterNode::IBootstrapBase* const Bootstrap_;

    TAtomicObject<TJobControllerDynamicConfigPtr> DynamicConfig_ = New<TJobControllerDynamicConfig>();

    THashMap<EJobType, TJobFactory> JobFactoryMap_;
    THashMap<EObjectType, TJobHeartbeatProcessorBasePtr> JobHeartbeatProcessors_;

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, JobMapLock_);
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
    TBufferedProducerPtr ActiveJobCountBuffer_ = New<TBufferedProducer>();
    TBufferedProducerPtr ResourceLimitsBuffer_ = New<TBufferedProducer>();
    TBufferedProducerPtr ResourceUsageBuffer_ = New<TBufferedProducer>();
    TBufferedProducerPtr GpuUtilizationBuffer_ = New<TBufferedProducer>();

    THashMap<std::pair<EJobState, EJobOrigin>, TCounter> JobFinalStateCounters_;

    // Chunk cache counters.
    TCounter CacheHitArtifactsSizeCounter_;
    TCounter CacheMissArtifactsSizeCounter_;
    TCounter CacheBypassedArtifactsSizeCounter_;
    TGauge TmpfsSizeGauge_;
    TGauge TmpfsUsageGauge_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr ResourceAdjustmentExecutor_;
    TPeriodicExecutorPtr RecentlyRemovedJobCleaner_;
    TPeriodicExecutorPtr ReservedMappedMemoryChecker_;
    TPeriodicExecutorPtr JobProxyBuildInfoUpdater_;

    TInstant LastStoredJobsSendTime_;

    THashSet<int> FreePorts_;

    TErrorOr<TYsonString> CachedJobProxyBuildInfo_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig);
    const THashMap<TJobId, TOperationId>& GetSpecFetchFailedJobIds() const;
    bool StatisticsThrottlerTryAcquire(int size);
    void RemoveSchedulerJobsOnFatalAlert();
    bool NeedTotalConfirmation();

    void DoPrepareHeartbeatRequest(TCellTag cellTag, EObjectType jobObjectType, const TReqHeartbeatPtr& request);
    void PrepareHeartbeatCommonRequestPart(const TReqHeartbeatPtr& request);
    void DoProcessHeartbeatResponse(const TRspHeartbeatPtr& response, EObjectType jobObjectType);
    void ProcessHeartbeatCommonResponsePart(const TRspHeartbeatPtr& response);

    void OnJobSpecsReceived(
        std::vector<NJobTrackerClient::NProto::TJobStartInfo> startInfos,
        const TAddressWithNetwork& addressWithNetwork,
        const TJobSpecServiceProxy::TErrorOrRspGetJobSpecsPtr& rspOrError);

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
    void AbortJob(const IJobPtr& job, const TJobToAbort& abortAttributes);

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

    void OnWaitingJobTimeout(const TWeakPtr<IJob>& weakJob, TDuration waitingJobTimeout);

    void OnResourcesUpdated(
        const TWeakPtr<IJob>& weakJob,
        const TNodeResources& resourceDelta);

    void OnPortsReleased(const TWeakPtr<IJob>& weakJob);

    void OnJobPrepared(const TWeakPtr<IJob>& weakJob);
    void OnJobFinished(const TWeakPtr<IJob>& weakJob);

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

    TCounter* GetJobFinalStateCounter(EJobState state, EJobOrigin origin);

    void BuildJobProxyBuildInfo(NYTree::TFluentAny fluent) const;
    void UpdateJobProxyBuildInfo();

    TDuration GetTotalConfirmationPeriod() const;
    TDuration GetMemoryOverdraftTimeout() const;
    TDuration GetCpuOverdraftTimeout() const;
    TDuration GetRecentlyRemovedJobsStoreTimeout() const;
};

////////////////////////////////////////////////////////////////////////////////

TJobController::TImpl::TImpl(
    TJobControllerConfigPtr config,
    IBootstrapBase* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(Config_->StatisticsThrottler))
    , Profiler_("/job_controller")
    , CacheHitArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_hit_artifacts_size"))
    , CacheMissArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_miss_artifacts_size"))
    , CacheBypassedArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_bypassed_artifacts_size"))
    , TmpfsSizeGauge_(Profiler_.Gauge("/tmpfs/size"))
    , TmpfsUsageGauge_(Profiler_.Gauge("/tmpfs/usage"))
{
    Profiler_.AddProducer("/resource_limits", ResourceLimitsBuffer_);
    Profiler_.AddProducer("/resource_usage", ResourceUsageBuffer_);
    Profiler_.AddProducer("/gpu_utilization", GpuUtilizationBuffer_);
    Profiler_.AddProducer("", ActiveJobCountBuffer_);

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

    JobProxyBuildInfoUpdater_ = New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TImpl::UpdateJobProxyBuildInfo, MakeWeak(this)),
        Config_->JobProxyBuildInfoUpdatePeriod);
    JobProxyBuildInfoUpdater_->Start();
    // Fetch initial job proxy build info immediately.
    JobProxyBuildInfoUpdater_->ScheduleOutOfBand();

    // Wait synchronously for one update in order to get some reasonable value in CachedJobProxyBuildInfo_.
    // Note that if somebody manages to request orchid before this field is set, this will result to nullptr
    // dereference.
    WaitFor(JobProxyBuildInfoUpdater_->GetExecutedEvent())
        .ThrowOnError();

    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
}

void TJobController::TImpl::RegisterJobFactory(EJobType type, TJobFactory factory)
{
    YT_VERIFY(JobFactoryMap_.emplace(type, factory).second);
}

void TJobController::TImpl::RegisterHeartbeatProcessor(EObjectType type, TJobHeartbeatProcessorBasePtr heartbeatProcessor)
{
    YT_VERIFY(JobHeartbeatProcessors_.emplace(type, std::move(heartbeatProcessor)).second);
}

TJobFactory TJobController::TImpl::GetFactory(EJobType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetOrCrash(JobFactoryMap_, type);
}

IJobPtr TJobController::TImpl::FindJob(TJobId jobId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(JobMapLock_);
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

    auto guard = ReaderGuard(JobMapLock_);
    std::vector<IJobPtr> result;
    result.reserve(JobMap_.size());
    for (const auto& [id, job] : JobMap_) {
        result.push_back(job);
    }
    return result;
}

void TJobController::TImpl::RemoveSchedulerJobsOnFatalAlert()
{
    std::vector<TJobId> jobIdsToRemove;
    for (const auto& [jobId, job] : JobMap_) {
        if (TypeFromId(jobId) == EObjectType::SchedulerJob) {
            YT_LOG_INFO("Removing job %v due to fatal alert");
            job->Abort(TError("Job aborted due to fatal alert"));
            jobIdsToRemove.push_back(jobId);
        }
    }

    auto guard = WriterGuard(JobMapLock_);
    for (auto jobId : jobIdsToRemove) {
        YT_VERIFY(JobMap_.erase(jobId) == 1);
    }
}

bool TJobController::TImpl::NeedTotalConfirmation()
{
    if (const auto now = TInstant::Now(); LastStoredJobsSendTime_ + GetTotalConfirmationPeriod() < now) {
        LastStoredJobsSendTime_ = now;
        return true;
    }

    return false;
}

bool TJobController::TImpl::StatisticsThrottlerTryAcquire(const int size)
{
    return StatisticsThrottler_->TryAcquire(size);
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
    bool chunkCacheEnabled = false;
    if (Bootstrap_->IsExecNode()) {
        const auto& chunkCache = Bootstrap_->GetExecNodeBootstrap()->GetChunkCache();
        chunkCacheEnabled = chunkCache->IsEnabled();
    }

    result.set_user_slots(chunkCacheEnabled && !DisableSchedulerJobs_.load() && !Bootstrap_->IsReadOnly()
        ? Bootstrap_->GetExecNodeBootstrap()->GetSlotManager()->GetSlotCount()
        : 0);

    auto resourceLimitsOverrides = ResourceLimitsOverrides_.Load();
    #define XX(name, Name) \
        result.set_##name(resourceLimitsOverrides.has_##name() \
            ? resourceLimitsOverrides.name() \
            : Config_->ResourceLimits->Name);
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX

    if (Bootstrap_->IsExecNode()) {
        const auto& gpuManager = Bootstrap_->GetExecNodeBootstrap()->GetGpuManager();
        result.set_gpu(gpuManager->GetTotalGpuCount());
    } else {
        result.set_gpu(0);
    }

    const auto& memoryUsageTracker = Bootstrap_->GetMemoryUsageTracker();

    // NB: Some categories can have no explicit limit.
    // Therefore we need bound memory limit by actually available memory.
    auto getUsedMemory = [&] (EMemoryCategory category) {
        return std::max<i64>(
            0,
            memoryUsageTracker->GetUsed(category) + memoryUsageTracker->GetTotalFree() - Config_->FreeMemoryWatermark);
    };
    result.set_user_memory(std::min(
        memoryUsageTracker->GetLimit(EMemoryCategory::UserJobs),
        getUsedMemory(EMemoryCategory::UserJobs)));
    result.set_system_memory(std::min(
        memoryUsageTracker->GetLimit(EMemoryCategory::SystemJobs),
        getUsedMemory(EMemoryCategory::SystemJobs)));

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

    if (Bootstrap_->IsExecNode()) {
        const auto& slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();
        result.set_user_slots(slotManager->GetUsedSlotCount());

        const auto& gpuManager = Bootstrap_->GetExecNodeBootstrap()->GetGpuManager();
        result.set_gpu(gpuManager->GetUsedGpuCount());
    } else {
        result.set_user_slots(0);
        result.set_gpu(0);
    }

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
            preemptMemoryOverdraft = *UserMemoryOverdraftInstant_ + GetMemoryOverdraftTimeout() <
                TInstant::Now();
        } else {
            UserMemoryOverdraftInstant_ = TInstant::Now();
        }
    } else {
        UserMemoryOverdraftInstant_ = std::nullopt;
    }

    if (usage.cpu() > limits.cpu()) {
        if (CpuOverdraftInstant_) {
            preemptCpuOverdraft = *CpuOverdraftInstant_ + GetCpuOverdraftTimeout() <
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
                NExecNode::EErrorCode::ResourceOverdraft,
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
        if (jobRecord.RemovalTime + GetRecentlyRemovedJobsStoreTimeout() < now) {
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
            NExecNode::EErrorCode::ResourceOverdraft,
            "Mapped memory usage overdraft"));
        schedulerJobs.pop_back();
    }
}

TDiskResources TJobController::TImpl::GetDiskResources() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (Bootstrap_->IsExecNode()) {
        const auto& slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();
        return slotManager->GetDiskResources();
    } else {
        return TDiskResources{};
    }
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

        auto jobLogger = JobAgentServerLogger.WithTag("JobId: %v", job->GetId());

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

            if (std::ssize(ports) < portCount) {
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

        job->SubscribeJobPrepared(
            BIND(&TImpl::OnJobPrepared, MakeWeak(this), MakeWeak(job))
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
        auto guard = WriterGuard(JobMapLock_);
        YT_VERIFY(JobMap_.emplace(jobId, job).second);
    }

    ScheduleStart();

    // Use #Apply instead of #Subscribe to match #OnWaitingJobTimeout signature.
    TDelayedExecutor::MakeDelayed(waitingJobTimeout)
        .Apply(BIND(&TImpl::OnWaitingJobTimeout, MakeWeak(this), MakeWeak(job), waitingJobTimeout)
        .Via(Bootstrap_->GetJobInvoker()));

    return job;
}

void TJobController::TImpl::OnWaitingJobTimeout(const TWeakPtr<IJob>& weakJob, TDuration waitingJobTimeout)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto job = weakJob.Lock();
    if (!job) {
        return;
    }

    if (job->GetState() == EJobState::Waiting) {
        job->Abort(TError(NExecNode::EErrorCode::WaitingJobTimeout, "Job waiting has timed out")
            << TErrorAttribute("timeout", waitingJobTimeout));
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

void TJobController::TImpl::AbortJob(const IJobPtr& job, const TJobToAbort& abortAttributes)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Job abort requested (JobId: %v, AbortReason: %v, PreemptionReason: %v)",
        job->GetId(),
        abortAttributes.AbortReason,
        abortAttributes.PreemptionReason);

    TError error(NExecNode::EErrorCode::AbortByScheduler, "Job aborted by scheduler");
    if (abortAttributes.AbortReason) {
        error = error << TErrorAttribute("abort_reason", *abortAttributes.AbortReason);
    }
    if (abortAttributes.PreemptionReason) {
        error = error << TErrorAttribute("preemption_reason", *abortAttributes.PreemptionReason);
    }

    job->Abort(error);
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
        auto guard = WriterGuard(JobMapLock_);
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
                NExecNode::EErrorCode::ResourceOverdraft,
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

void TJobController::TImpl::OnJobPrepared(const TWeakPtr<IJob>& weakJob)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto job = weakJob.Lock();
    if (!job) {
        return;
    }

    const auto& chunkCacheStatistics = job->GetChunkCacheStatistics();
    CacheHitArtifactsSizeCounter_.Increment(chunkCacheStatistics.CacheHitArtifactsSize);
    CacheMissArtifactsSizeCounter_.Increment(chunkCacheStatistics.CacheMissArtifactsSize);
    CacheBypassedArtifactsSizeCounter_.Increment(chunkCacheStatistics.CacheBypassedArtifactsSize);
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
    jobFinalStateCounter->Increment();

    JobFinished_.Fire(job);
}

bool TJobController::TImpl::CheckMemoryOverdraft(const TNodeResources& delta)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    // Only "cpu" and "user_memory" can be increased.
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
        BIND(&TJobController::TImpl::DoPrepareHeartbeatRequest, MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run(cellTag, jobObjectType, request);
}

void TJobController::TImpl::DoPrepareHeartbeatRequest(
    TCellTag cellTag,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    GetOrCrash(JobHeartbeatProcessors_, jobObjectType)->PrepareRequest(cellTag, request);
}

void TJobController::TImpl::PrepareHeartbeatCommonRequestPart(const TReqHeartbeatPtr& request)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    request->set_node_id(Bootstrap_->GetNodeId());
    ToProto(request->mutable_node_descriptor(), Bootstrap_->GetLocalDescriptor());
    *request->mutable_resource_limits() = GetResourceLimits();
    *request->mutable_resource_usage() = GetResourceUsage(/* includeWaiting */ true);

    *request->mutable_disk_resources() = GetDiskResources();

    if (Bootstrap_->IsExecNode()) {
        const auto& jobReporter = Bootstrap_->GetExecNodeBootstrap()->GetJobReporter();
        request->set_job_reporter_write_failures_count(jobReporter->ExtractWriteFailuresCount());
        request->set_job_reporter_queue_is_too_large(jobReporter->GetQueueIsTooLarge());
    } else {
        request->set_job_reporter_write_failures_count(0);
        request->set_job_reporter_queue_is_too_large(false);
    }
}

TFuture<void> TJobController::TImpl::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND(&TJobController::TImpl::DoProcessHeartbeatResponse, MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run(response, jobObjectType);
}

void TJobController::TImpl::DoProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    GetOrCrash(JobHeartbeatProcessors_, jobObjectType)->ProcessResponse(response);
}

void TJobController::TImpl::ProcessHeartbeatCommonResponsePart(const TRspHeartbeatPtr& response)
{
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

    {
        auto doAbortJob = [&] (const TJobToAbort& jobToAbort) {
            if (auto job = FindJob(jobToAbort.JobId)) {
                AbortJob(job, jobToAbort);
            } else {
                YT_LOG_WARNING("Requested to abort a non-existent job (JobId: %v, AbortReason: %v, PreemptionReason: %v)",
                    jobToAbort.JobId,
                    jobToAbort.AbortReason,
                    jobToAbort.PreemptionReason);
            }
        };

        if (response->jobs_to_abort_size() > 0) {
            for (const auto& protoJobToAbort : response->jobs_to_abort()) {
                doAbortJob(FromProto<TJobToAbort>(protoJobToAbort));
            }
        } else {
            // COMPAT(eshcherbin)
            for (const auto& protoJobId : response->old_jobs_to_abort()) {
                doAbortJob({FromProto<TJobId>(protoJobId)});
            }
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
}

const THashMap<TJobId, TOperationId>& TJobController::TImpl::GetSpecFetchFailedJobIds() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return SpecFetchFailedJobIds_;
}

bool TJobController::TImpl::IsJobProxyProfilingDisabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = DynamicConfig_.Load();
    return config ? config->DisableJobProxyProfiling.value_or(Config_->DisableJobProxyProfiling) : Config_->DisableJobProxyProfiling;
}

TFuture<void> TJobController::TImpl::RequestJobSpecsAndStartJobs(std::vector<NJobTrackerClient::NProto::TJobStartInfo> jobStartInfos)
{
    THashMap<TAddressWithNetwork, std::vector<NJobTrackerClient::NProto::TJobStartInfo>> groupedStartInfos;

    for (auto& startInfo : jobStartInfos) {
        auto operationId = FromProto<TJobId>(startInfo.operation_id());
        auto jobId = FromProto<TJobId>(startInfo.job_id());
        auto addresses = FromProto<NNodeTrackerClient::TAddressMap>(startInfo.spec_service_addresses());

        std::optional<TAddressWithNetwork> addressWithNetwork;
        try {
            addressWithNetwork = GetAddressWithNetworkOrThrow(addresses, Bootstrap_->GetLocalNetworks());
        } catch (const std::exception& ex) {
            YT_VERIFY(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
            YT_LOG_DEBUG(ex, "Job spec cannot be requested since no suitable network address exists (OperationId: %v, JobId: %v, SpecServiceAddresses: %v)",
                operationId,
                jobId,
                GetValues(addresses));
        }

        if (addressWithNetwork) {
            YT_LOG_DEBUG("Job spec will be requested (OperationId: %v, JobId: %v, SpecServiceAddress: %v)",
                operationId,
                jobId,
                addressWithNetwork->Address);
            groupedStartInfos[*addressWithNetwork].push_back(startInfo);
        }
    }

    auto getSpecServiceChannel = [&] (const auto& addressWithNetwork) {
        const auto& client = Bootstrap_->GetMasterClient();
        const auto& channelFactory = client->GetNativeConnection()->GetChannelFactory();
        return channelFactory->CreateChannel(addressWithNetwork);
    };

    std::vector<TFuture<void>> asyncResults;
    for (auto& [addressWithNetwork, startInfos] : groupedStartInfos) {
        auto channel = getSpecServiceChannel(addressWithNetwork);
        TJobSpecServiceProxy jobSpecServiceProxy(channel);

        auto dynamicConfig = DynamicConfig_.Load();
        auto getJobSpecsTimeout = dynamicConfig
            ? dynamicConfig->GetJobSpecsTimeout.value_or(Config_->GetJobSpecsTimeout)
            : Config_->GetJobSpecsTimeout;

        jobSpecServiceProxy.SetDefaultTimeout(getJobSpecsTimeout);
        auto jobSpecRequest = jobSpecServiceProxy.GetJobSpecs();

        for (const auto& startInfo : startInfos) {
            auto* subrequest = jobSpecRequest->add_requests();
            *subrequest->mutable_operation_id() = startInfo.operation_id();
            *subrequest->mutable_job_id() = startInfo.job_id();
        }

        YT_LOG_DEBUG("Requesting job specs (SpecServiceAddress: %v, Count: %v)",
            addressWithNetwork,
            startInfos.size());

        auto asyncResult = jobSpecRequest->Invoke().Apply(
            BIND(
                &TJobController::TImpl::OnJobSpecsReceived,
                MakeStrong(this),
                Passed(std::move(startInfos)),
                addressWithNetwork)
            .AsyncVia(Bootstrap_->GetJobInvoker()));
        asyncResults.push_back(asyncResult);
    }

    return AllSet(asyncResults).As<void>();
}

void TJobController::TImpl::OnJobSpecsReceived(
    std::vector<NJobTrackerClient::NProto::TJobStartInfo> startInfos,
    const TAddressWithNetwork& addressWithNetwork,
    const TJobSpecServiceProxy::TErrorOrRspGetJobSpecsPtr& rspOrError)
{
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

    YT_LOG_DEBUG("Job specs received (SpecServiceAddress: %v)", addressWithNetwork);

    const auto& rsp = rspOrError.Value();
    YT_VERIFY(rsp->responses_size() == std::ssize(startInfos));
    for (size_t index = 0; index < startInfos.size(); ++index) {
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

        TJobSpec spec;
        DeserializeProtoWithEnvelope(&spec, attachment);

        const auto& resourceLimits = startInfo.resource_limits();

        CreateJob(jobId, operationId, resourceLimits, std::move(spec));
    }
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

void TJobController::TImpl::OnDynamicConfigChanged(
    const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
    const TClusterNodeDynamicConfigPtr& newNodeConfig)
{
    auto jobControllerConfig = newNodeConfig->ExecNode->JobController;

    DynamicConfig_.Store(jobControllerConfig);

    if (jobControllerConfig && jobControllerConfig->ResourceAdjustmentPeriod) {
        ResourceAdjustmentExecutor_->SetPeriod(*jobControllerConfig->ResourceAdjustmentPeriod);
    } else {
        ResourceAdjustmentExecutor_->SetPeriod(Config_->ResourceAdjustmentPeriod);
    }

    if (jobControllerConfig && jobControllerConfig->RecentlyRemovedJobsCleanPeriod) {
        RecentlyRemovedJobCleaner_->SetPeriod(*jobControllerConfig->RecentlyRemovedJobsCleanPeriod);
    } else {
        RecentlyRemovedJobCleaner_->SetPeriod(Config_->RecentlyRemovedJobsCleanPeriod);
    }
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
                                    .Do(std::bind(&IJob::BuildOrchid, job, _1))
                                .EndMap();
                        });
                })
            .DoIf(Bootstrap_->IsExecNode(), [&] (auto fluent) {
                fluent
                    .Item("gpu_utilization").DoMapFor(
                        Bootstrap_->GetExecNodeBootstrap()->GetGpuManager()->GetGpuInfoMap(),
                        [&] (TFluentMap fluent, const auto& pair) {
                            const auto& [_, gpuInfo] = pair;
                            fluent.Item(ToString(gpuInfo.Index))
                                .BeginMap()
                                    .Item("update_time").Value(gpuInfo.UpdateTime)
                                    .Item("utilization_gpu_rate").Value(gpuInfo.UtilizationGpuRate)
                                    .Item("utilization_memory_rate").Value(gpuInfo.UtilizationMemoryRate)
                                    .Item("memory_used").Value(gpuInfo.MemoryUsed)
                                    .Item("memory_limit").Value(gpuInfo.MemoryTotal)
                                    .Item("power_used").Value(gpuInfo.PowerDraw)
                                    .Item("power_limit").Value(gpuInfo.PowerLimit)
                                    .Item("clocks_sm_used").Value(gpuInfo.ClocksSm)
                                    .Item("clocks_sm_limit").Value(gpuInfo.ClocksMaxSm)
                                .EndMap();
                        })
                    .Item("slot_manager").DoMap(BIND(
                        &NExecNode::TSlotManager::BuildOrchidYson,
                        Bootstrap_->GetExecNodeBootstrap()->GetSlotManager()));
            })
            .Item("job_proxy_build").Do(BIND(&TJobController::TImpl::BuildJobProxyBuildInfo, MakeStrong(this)))
        .EndMap();
}

void TJobController::TImpl::BuildJobProxyBuildInfo(NYTree::TFluentAny fluent) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (CachedJobProxyBuildInfo_.IsOK()) {
        fluent.Value(CachedJobProxyBuildInfo_.Value());
    } else {
        fluent
            .BeginMap()
                .Item("error").Value(static_cast<TError>(CachedJobProxyBuildInfo_))
            .EndMap();
    }
}

void TJobController::TImpl::UpdateJobProxyBuildInfo()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    // TODO(max42): not sure if running ytserver-job-proxy --build --yson from JobThread
    // is a good idea; maybe delegate to another thread?

    try {
        auto jobProxyPath = ResolveBinaryPath(JobProxyProgramName)
            .ValueOrThrow();

        TSubprocess jobProxy(jobProxyPath);
        jobProxy.AddArguments({"--build", "--yson"});

        auto result = jobProxy.Execute();
        result.Status.ThrowOnError();

        TMemoryInput input(result.Output.begin(), result.Output.size());
        TYsonInput ysonInput(&input);

        TString ysonBytes;
        TStringOutput outputStream(ysonBytes);
        TYsonWriter writer(&outputStream);
        ParseYson(ysonInput, &writer);

        CachedJobProxyBuildInfo_ = TYsonString(ysonBytes);
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        CachedJobProxyBuildInfo_ = error;
    }

    JobProxyBuildInfoUpdated_.Fire(static_cast<TError>(CachedJobProxyBuildInfo_));
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

    ActiveJobCountBuffer_->Update([this] (ISensorWriter* writer) {
        auto jobs = GetJobsByOrigin();

        for (auto origin : TEnumTraits<EJobOrigin>::GetDomainValues()) {
            writer->PushTag(TTag{"origin", FormatEnum(origin)});
            writer->AddGauge("/active_job_count", jobs[origin].size());
            writer->PopTag();
        }
    });

    ResourceUsageBuffer_->Update([this] (ISensorWriter* writer) {
        ProfileResources(writer, GetResourceUsage());
    });

    ResourceLimitsBuffer_->Update([this] (ISensorWriter* writer) {
        ProfileResources(writer, GetResourceLimits());
    });

    if (Bootstrap_->IsExecNode()) {
        const auto& gpuManager = Bootstrap_->GetExecNodeBootstrap()->GetGpuManager();
        GpuUtilizationBuffer_->Update([this, gpuManager] (ISensorWriter* writer) {
            for (const auto& [index, gpuInfo] : gpuManager->GetGpuInfoMap()) {
                writer->PushTag(TTag{"gpu_name", gpuInfo.Name});
                writer->PushTag(TTag{"device_number", ToString(index)});

                ProfileGpuInfo(writer, gpuInfo);

                writer->PopTag();
                writer->PopTag();
            }
        });
    }

    i64 tmpfsSize = 0;
    i64 tmpfsUsage = 0;
    for (const auto& job : GetJobs()) {
        if (TypeFromId(job->GetId()) != EObjectType::SchedulerJob) {
            continue;
        }

        if (job->GetState() != EJobState::Running || job->GetPhase() != EJobPhase::Running) {
            continue;
        }

        const auto& jobSpec = job->GetSpec();
        auto extensionId = NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext;
        if (!jobSpec.HasExtension(extensionId)) {
            continue;
        }

        auto extension = jobSpec.GetExtension(extensionId);
        if (!extension.has_user_job_spec()) {
            continue;
        }

        for (const auto& tmpfsVolumeProto : extension.user_job_spec().tmpfs_volumes()) {
            tmpfsSize += tmpfsVolumeProto.size();
        }

        auto statisticsYson = job->GetStatistics();
        if (!statisticsYson) {
            continue;
        }

        TString sensorName = "/user_job/tmpfs_size/sum";

        auto statisticsNode = ConvertToNode(statisticsYson);
        auto tmpfsSizeNode = FindNodeByYPath(statisticsNode, sensorName);
        if (!tmpfsSizeNode) {
            continue;
        }

        if (tmpfsSizeNode->GetType() != ENodeType::Int64) {
            YT_LOG_WARNING("Wrong type of sensor (SensorName: %v, ExpectedType: %v, ActualType: %v)",
                sensorName,
                ENodeType::Int64,
                tmpfsSizeNode->GetType());
            continue;
        }

        tmpfsUsage += tmpfsSizeNode->GetValue<i64>();
    }

    TmpfsSizeGauge_.Update(tmpfsSize);
    TmpfsUsageGauge_.Update(tmpfsUsage);
}

TCounter* TJobController::TImpl::GetJobFinalStateCounter(EJobState state, EJobOrigin origin)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto key = std::make_pair(state, origin);
    auto it = JobFinalStateCounters_.find(key);
    if (it == JobFinalStateCounters_.end()) {
        auto counter = Profiler_
            .WithTag("state", FormatEnum(state))
            .WithTag("origin", FormatEnum(origin))
            .Counter("/job_final_state");

        it = JobFinalStateCounters_.emplace(key, counter).first;
    }

    return &it->second;
}

TDuration TJobController::TImpl::GetTotalConfirmationPeriod() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->TotalConfirmationPeriod.value_or(Config_->TotalConfirmationPeriod)
        : Config_->TotalConfirmationPeriod;
}

TDuration TJobController::TImpl::GetMemoryOverdraftTimeout() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->MemoryOverdraftTimeout.value_or(Config_->MemoryOverdraftTimeout)
        : Config_->MemoryOverdraftTimeout;
}

TDuration TJobController::TImpl::GetCpuOverdraftTimeout() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->CpuOverdraftTimeout.value_or(Config_->CpuOverdraftTimeout)
        : Config_->CpuOverdraftTimeout;
}

TDuration TJobController::TImpl::GetRecentlyRemovedJobsStoreTimeout() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->RecentlyRemovedJobsStoreTimeout.value_or(Config_->RecentlyRemovedJobsStoreTimeout)
        : Config_->RecentlyRemovedJobsStoreTimeout;
}

////////////////////////////////////////////////////////////////////////////////

TJobController::TJobController(
    TJobControllerConfigPtr config,
    IBootstrapBase* bootstrap)
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

bool TJobController::IsJobProxyProfilingDisabled() const
{
    return Impl_->IsJobProxyProfilingDisabled();
}

DELEGATE_SIGNAL(TJobController, void(), ResourcesUpdated, *Impl_)
DELEGATE_SIGNAL(TJobController, void(const IJobPtr&), JobFinished, *Impl_)
DELEGATE_SIGNAL(TJobController, void(const TError&), JobProxyBuildInfoUpdated, *Impl_)

void TJobController::RegisterHeartbeatProcessor(
    const EObjectType type,
    TJobHeartbeatProcessorBasePtr heartbeatProcessor)
{
    Impl_->RegisterHeartbeatProcessor(type, std::move(heartbeatProcessor));
}

////////////////////////////////////////////////////////////////////////////////

TJobController::TJobHeartbeatProcessorBase::TJobHeartbeatProcessorBase(
    TJobController* const controller,
    IBootstrapBase* const bootstrap)
    : JobController_(controller)
    , Bootstrap_(bootstrap)
{ }

void TJobController::TJobHeartbeatProcessorBase::RemoveSchedulerJobsOnFatalAlert()
{
    JobController_->Impl_->RemoveSchedulerJobsOnFatalAlert();
}

bool TJobController::TJobHeartbeatProcessorBase::NeedTotalConfirmation()
{
    return JobController_->Impl_->NeedTotalConfirmation();
}

TFuture<void> TJobController::TJobHeartbeatProcessorBase::RequestJobSpecsAndStartJobs(
    std::vector<NJobTrackerClient::NProto::TJobStartInfo> jobStartInfos)
{
    return JobController_->Impl_->RequestJobSpecsAndStartJobs(std::move(jobStartInfos));
}

IJobPtr TJobController::TJobHeartbeatProcessorBase::CreateJob(
    TJobId jobId,
    TOperationId operationId,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec)
{
    return JobController_->Impl_->CreateJob(jobId, operationId, resourceLimits, std::move(jobSpec));
}

const THashMap<TJobId, TOperationId>& TJobController::TJobHeartbeatProcessorBase::GetSpecFetchFailedJobIds()
{
    return JobController_->Impl_->GetSpecFetchFailedJobIds();
}

bool TJobController::TJobHeartbeatProcessorBase::StatisticsThrottlerTryAcquire(const int size)
{
    return JobController_->Impl_->StatisticsThrottlerTryAcquire(size);
}

void TJobController::TJobHeartbeatProcessorBase::PrepareHeartbeatCommonRequestPart(const TReqHeartbeatPtr& request)
{
    JobController_->Impl_->PrepareHeartbeatCommonRequestPart(request);
}

void TJobController::TJobHeartbeatProcessorBase::ProcessHeartbeatCommonResponsePart(const TRspHeartbeatPtr& response)
{
    JobController_->Impl_->ProcessHeartbeatCommonResponsePart(response);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
