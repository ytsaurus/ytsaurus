#include "job_controller.h"

#include "job_resource_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>
#include <yt/yt/server/node/exec_node/gpu_manager.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>
#include <yt/yt/server/node/exec_node/controller_agent_connector.h>
#include <yt/yt/server/node/exec_node/job.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/job_agent/gpu_helpers.h>
#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/job_tracker_client/helpers.h>
#include <yt/yt/ytlib/job_tracker_client/job_spec_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <yt/yt/library/process/process.h>
#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/net/helpers.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

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
using NJobTrackerClient::NProto::TJobStartInfo;
using NNodeTrackerClient::NProto::TNodeResources;
using NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides;
using NNodeTrackerClient::NProto::TDiskResources;
using NExecNode::TControllerAgentDescriptor;

using NYT::FromProto;
using NYT::ToProto;

using std::placeholders::_1;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobAgentServerLogger;

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

    void RegisterSchedulerJobFactory(
        EJobType type,
        TSchedulerJobFactory factory);
    void RegisterMasterJobFactory(
        EJobType type,
        TMasterJobFactory factory);

    void RegisterHeartbeatProcessor(
        EObjectType type,
        TJobHeartbeatProcessorBasePtr heartbeatProcessor);

    IJobPtr FindJob(TJobId jobId) const;
    IJobPtr GetJobOrThrow(TJobId jobId) const;
    IJobPtr FindRecentlyRemovedJob(TJobId jobId) const;
    std::vector<IJobPtr> GetSchedulerJobs() const;
    std::vector<IJobPtr> GetMasterJobs() const;

    void OnReservedMemoryOvercommited(i64 mapped);

    void SetDisableSchedulerJobs(bool value);

    TFuture<void> PrepareHeartbeatRequest(
        TCellTag cellTag,
        const TString& jobTrackerAddress,
        EObjectType jobObjectType,
        const TReqHeartbeatPtr& request);
    TFuture<void> ProcessHeartbeatResponse(
        const TString& jobTrackerAddress,
        const TRspHeartbeatPtr& response,
        EObjectType jobObjectType);

    TFuture<void> PrepareHeartbeatRequest(
        NNodeTrackerClient::NProto::TReqHeartbeat* request);

    NYTree::IYPathServicePtr GetOrchidService();

    TFuture<void> RequestJobSpecsAndStartJobs(
        std::vector<TJobStartInfo> jobStartInfos);

    bool IsJobProxyProfilingDisabled() const;
    NJobProxy::TJobProxyDynamicConfigPtr GetJobProxyDynamicConfig() const;

    TBuildInfoPtr GetBuildInfo() const;

    bool AreSchedulerJobsDisabled() const noexcept;

private:
    friend class TJobController::TJobHeartbeatProcessorBase;

    const TIntrusivePtr<const TJobControllerConfig> Config_;
    NClusterNode::IBootstrapBase* const Bootstrap_;
    IJobResourceManagerPtr JobResourceManager_;

    TAtomicObject<TJobControllerDynamicConfigPtr> DynamicConfig_ = New<TJobControllerDynamicConfig>();

    THashMap<EJobType, TSchedulerJobFactory> SchedulerJobFactoryMap_;
    THashMap<EJobType, TMasterJobFactory> MasterJobFactoryMap_;
    THashMap<EObjectType, TJobHeartbeatProcessorBasePtr> JobHeartbeatProcessors_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, JobMapLock_);
    THashMap<TJobId, IJobPtr> SchedulerJobMap_;
    THashMap<TJobId, IJobPtr> MasterJobMap_;

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

    bool MasterJobsStartScheduled_ = false;
    bool SchedulerJobsStartScheduled_ = false;

    std::atomic<bool> DisableSchedulerJobs_ = false;

    std::optional<TInstant> UserMemoryOverdraftInstant_;
    std::optional<TInstant> CpuOverdraftInstant_;

    TProfiler Profiler_;
    TBufferedProducerPtr ActiveJobCountBuffer_ = New<TBufferedProducer>();
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
    TPeriodicExecutorPtr JobProxyBuildInfoUpdater_;

    TInstant LastStoredJobsSendTime_;

    TAtomicObject<TErrorOr<TBuildInfoPtr>> CachedJobProxyBuildInfo_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    THashMap<TJobId, IJobPtr>& GetJobMap(TJobId jobId);
    const THashMap<TJobId, IJobPtr>& GetJobMap(TJobId jobId) const;

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig);
    const THashMap<TJobId, TOperationId>& GetSpecFetchFailedJobIds() const noexcept;
    void RemoveSchedulerJobsOnFatalAlert();
    bool NeedTotalConfirmation() noexcept;

    void DoPrepareHeartbeatRequest(
        TCellTag cellTag,
        const TString& jobTrackerAddress,
        EObjectType jobObjectType,
        const TReqHeartbeatPtr& request);
    void PrepareHeartbeatCommonRequestPart(const TReqHeartbeatPtr& request);
    void DoProcessHeartbeatResponse(
        const TString& jobTrackerAddress,
        const TRspHeartbeatPtr& response,
        EObjectType jobObjectType);
    void ProcessHeartbeatCommonResponsePart(const TRspHeartbeatPtr& response);

    void OnJobSpecsReceived(
        std::vector<TJobStartInfo> startInfos,
        const TControllerAgentDescriptor& addressWithNetwork,
        const TJobSpecServiceProxy::TErrorOrRspGetJobSpecsPtr& rspOrError);

    //! Starts a new scheduler job.
    IJobPtr CreateSchedulerJob(
        TJobId jobId,
        TOperationId operationId,
        const TNodeResources& resourceLimits,
        TJobSpec&& jobSpec,
        const TControllerAgentDescriptor& controllerAgentDescriptor);

    //! Starts a new master job.
    IJobPtr CreateMasterJob(
        TJobId jobId,
        TOperationId operationId,
        const TString& jobTrackerAddress,
        const TNodeResources& resourceLimits,
        TJobSpec&& jobSpec);

    void RegisterAndStartJob(
        TJobId jobId,
        const IJobPtr& job,
        TDuration waitingJobTimeout);

    //! Stops a job.
    /*!
     *  If the job is running, aborts it.
     */
    void AbortJob(const IJobPtr& job, TJobToAbort&& abortAttributes);

    //! Removes the job from the map.
    /*!
     *  It is illegal to call #Remove before the job is stopped.
     */
    void RemoveJob(
        const IJobPtr& job,
        const TReleaseJobFlags& releaseFlags);

    std::vector<IJobPtr> GetRunningSchedulerJobsSortedByStartTime() const;

    TSchedulerJobFactory GetSchedulerJobFactory(EJobType type) const;
    TMasterJobFactory GetMasterJobFactory(EJobType type) const;

    const TJobHeartbeatProcessorBasePtr& GetJobHeartbeatProcessor(EObjectType jobType) const;

    void ScheduleStartMasterJobs();
    void ScheduleStartSchedulerJobs();

    void OnWaitingJobTimeout(const TWeakPtr<IJob>& weakJob, TDuration waitingJobTimeout);

    void OnJobResourcesUpdated(
        const TWeakPtr<IJob>& weakJob,
        const TNodeResources& resourceDelta);
    
    void OnResourceReleased();

    void OnJobPrepared(const TWeakPtr<IJob>& weakJob);
    void OnJobFinished(const TWeakPtr<IJob>& weakJob);

    void StartWaitingJobs(std::vector<IJobPtr> jobs, bool& startJobsScheduledFlag);
    void StartWaitingMasterJobs();
    void StartWaitingSchedulerJobs();

    void BuildOrchid(IYsonConsumer* consumer) const;

    void OnProfiling();

    void AdjustResources();

    TEnumIndexedVector<EJobOrigin, std::vector<IJobPtr>> GetJobsByOrigin() const;

    void CleanRecentlyRemovedJobs();

    TCounter* GetJobFinalStateCounter(EJobState state, EJobOrigin origin);

    void BuildJobProxyBuildInfo(NYTree::TFluentAny fluent) const;
    void UpdateJobProxyBuildInfo();

    TErrorOr<TControllerAgentDescriptor> TryParseControllerAgentDescriptor(
        const NJobTrackerClient::NProto::TControllerAgentDescriptor& proto) const;
    TErrorOr<TString> TryParseControllerAgentAddress(
        const NNodeTrackerClient::NProto::TAddressMap& proto) const;

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
    , Profiler_("/job_controller")
    , CacheHitArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_hit_artifacts_size"))
    , CacheMissArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_miss_artifacts_size"))
    , CacheBypassedArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_bypassed_artifacts_size"))
    , TmpfsSizeGauge_(Profiler_.Gauge("/tmpfs/size"))
    , TmpfsUsageGauge_(Profiler_.Gauge("/tmpfs/usage"))
{
    Profiler_.AddProducer("/gpu_utilization", GpuUtilizationBuffer_);
    Profiler_.AddProducer("", ActiveJobCountBuffer_);

    YT_VERIFY(Config_);
    YT_VERIFY(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
}

void TJobController::TImpl::Initialize()
{
    JobResourceManager_ = Bootstrap_->GetJobResourceManager();
    JobResourceManager_->SubscribeResourcesReleased(
        BIND_NO_PROPAGATE(&TImpl::OnResourceReleased, MakeWeak(this))
            .Via(Bootstrap_->GetJobInvoker()));
    JobResourceManager_->SubscribeReservedMemoryOvercommited(
        BIND_NO_PROPAGATE(&TImpl::OnReservedMemoryOvercommited, MakeWeak(this))
            .Via(Bootstrap_->GetJobInvoker()));

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TImpl::OnProfiling, MakeWeak(this)),
        Config_->ProfilingPeriod);
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

    // Do not set period initially to defer start.
    JobProxyBuildInfoUpdater_ = New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TImpl::UpdateJobProxyBuildInfo, MakeWeak(this)));
    // Start nominally.
    JobProxyBuildInfoUpdater_->Start();

    // Get ready event before actual start.
    auto buildInfoReadyEvent = JobProxyBuildInfoUpdater_->GetExecutedEvent();

    // Actual start and fetch initial job proxy build info immediately. No need to call ScheduleOutOfBand.
    JobProxyBuildInfoUpdater_->SetPeriod(Config_->JobProxyBuildInfoUpdatePeriod);

    // Wait synchronously for one update in order to get some reasonable value in CachedJobProxyBuildInfo_.
    // Note that if somebody manages to request orchid before this field is set, this will result to nullptr
    // dereference.
    WaitFor(buildInfoReadyEvent)
        .ThrowOnError();

    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
}

void TJobController::TImpl::RegisterSchedulerJobFactory(EJobType type, TSchedulerJobFactory factory)
{
    YT_VERIFY(SchedulerJobFactoryMap_.emplace(type, factory).second);
}

void TJobController::TImpl::RegisterMasterJobFactory(EJobType type, TMasterJobFactory factory)
{
    YT_VERIFY(MasterJobFactoryMap_.emplace(type, factory).second);
}

void TJobController::TImpl::RegisterHeartbeatProcessor(EObjectType type, TJobHeartbeatProcessorBasePtr heartbeatProcessor)
{
    YT_VERIFY(JobHeartbeatProcessors_.emplace(type, std::move(heartbeatProcessor)).second);
}

TSchedulerJobFactory TJobController::TImpl::GetSchedulerJobFactory(EJobType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetOrCrash(SchedulerJobFactoryMap_, type);
}

TMasterJobFactory TJobController::TImpl::GetMasterJobFactory(EJobType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetOrCrash(MasterJobFactoryMap_, type);
}

const TJobController::TJobHeartbeatProcessorBasePtr& TJobController::TImpl::GetJobHeartbeatProcessor(EObjectType jobType) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetOrCrash(JobHeartbeatProcessors_, jobType);
}

THashMap<TJobId, IJobPtr>& TJobController::TImpl::GetJobMap(TJobId jobId)
{
    return const_cast<THashMap<TJobId, IJobPtr>&>(const_cast<const TImpl*>(this)->GetJobMap(jobId));
}

const THashMap<TJobId, IJobPtr>& TJobController::TImpl::GetJobMap(TJobId jobId) const
{
    EObjectType objectType = TypeFromId(jobId);
    YT_LOG_FATAL_IF(
        objectType != EObjectType::SchedulerJob && objectType != EObjectType::MasterJob,
        "Invalid object type in job id (JobId: %v, ObjectType: %v)",
        jobId,
        objectType);
    
    return objectType == EObjectType::SchedulerJob ? SchedulerJobMap_ : MasterJobMap_;
}

IJobPtr TJobController::TImpl::FindJob(TJobId jobId) const
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    auto& jobMap = GetJobMap(jobId);

    auto guard = ReaderGuard(JobMapLock_);
    auto it = jobMap.find(jobId);
    return it == jobMap.end() ? nullptr : it->second;
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

std::vector<IJobPtr> TJobController::TImpl::GetSchedulerJobs() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(JobMapLock_);
    std::vector<IJobPtr> result;
    result.reserve(SchedulerJobMap_.size());
    for (const auto& [id, job] : SchedulerJobMap_) {
        result.push_back(job);
    }
    return result;
}

std::vector<IJobPtr> TJobController::TImpl::GetMasterJobs() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(JobMapLock_);
    std::vector<IJobPtr> result;
    result.reserve(MasterJobMap_.size());
    for (const auto& [id, job] : MasterJobMap_) {
        result.push_back(job);
    }
    return result;
}

void TJobController::TImpl::RemoveSchedulerJobsOnFatalAlert()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    std::vector<TJobId> jobIdsToRemove;

    for (const auto& [jobId, job] : SchedulerJobMap_) {
        YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

        YT_LOG_INFO("Removing job %v due to fatal alert");
        job->Abort(TError("Job aborted due to fatal alert"));
        jobIdsToRemove.push_back(jobId);
    }

    auto guard = WriterGuard(JobMapLock_);
    for (auto jobId : jobIdsToRemove) {
        YT_VERIFY(SchedulerJobMap_.erase(jobId) == 1);
    }
}

bool TJobController::TImpl::NeedTotalConfirmation() noexcept
{
    if (const auto now = TInstant::Now(); LastStoredJobsSendTime_ + GetTotalConfirmationPeriod() < now) {
        LastStoredJobsSendTime_ = now;
        return true;
    }

    return false;
}

std::vector<IJobPtr> TJobController::TImpl::GetRunningSchedulerJobsSortedByStartTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<IJobPtr> schedulerJobs;
    for (const auto& job : GetSchedulerJobs()) {
        YT_VERIFY(TypeFromId(job->GetId()) == EObjectType::SchedulerJob);

        if (job->GetState() == EJobState::Running) {
            schedulerJobs.push_back(job);
        }
    }

    std::sort(schedulerJobs.begin(), schedulerJobs.end(), [] (const IJobPtr& lhs, const IJobPtr& rhs) {
        return lhs->GetStartTime() < rhs->GetStartTime();
    });

    return schedulerJobs;
}

void TJobController::TImpl::AdjustResources()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto usage = JobResourceManager_->GetResourceUsage(/*includeWaiting*/ false);
    auto limits = JobResourceManager_->GetResourceLimits();

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

void TJobController::TImpl::OnReservedMemoryOvercommited(
    i64 mappedMemory)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto usage = JobResourceManager_->GetResourceUsage(false);
    const auto limits = JobResourceManager_->GetResourceLimits();

    auto schedulerJobs = GetRunningSchedulerJobsSortedByStartTime();

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

void TJobController::TImpl::SetDisableSchedulerJobs(bool value)
{
    VERIFY_THREAD_AFFINITY_ANY();

    DisableSchedulerJobs_.store(value);

    if (value) {
        TError error{"All scheduler jobs are disabled"};

        Bootstrap_->GetJobInvoker()->Invoke(BIND([=, this_ = MakeStrong(this), error{std::move(error)}] {
            VERIFY_THREAD_AFFINITY(JobThread);

            NExecNode::InterruptSchedulerJobs(GetSchedulerJobs(), std::move(error));
        }));
    }
}

void TJobController::TImpl::StartWaitingJobs(std::vector<IJobPtr> jobs, bool& startJobsScheduledFlag)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto resourceAcquiringProxy = JobResourceManager_->GetResourceAcquiringProxy();

    for (const auto& job : jobs) {
        if (job->GetState() != EJobState::Waiting) {
            continue;
        }

        auto jobId = job->GetId();
        YT_LOG_DEBUG("Trying to start job (JobId: %v)", jobId);

        if (!resourceAcquiringProxy.TryAcquireResourcesFor(job->AsResourceHolder())) {
            YT_LOG_DEBUG("Job was not started (JobId: %v)", jobId);
        } else {
            YT_LOG_DEBUG("Job started (JobId: %v)", jobId);
        }
    }

    startJobsScheduledFlag = false;
}

void TJobController::TImpl::StartWaitingMasterJobs()
{
    StartWaitingJobs(GetMasterJobs(), MasterJobsStartScheduled_);
}

void TJobController::TImpl::StartWaitingSchedulerJobs()
{
    StartWaitingJobs(GetSchedulerJobs(), SchedulerJobsStartScheduled_);
}

IJobPtr TJobController::TImpl::CreateSchedulerJob(
    TJobId jobId,
    TOperationId operationId,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec,
    const TControllerAgentDescriptor& controllerAgentDescriptor)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto type = CheckedEnumCast<EJobType>(jobSpec.type());
    auto factory = GetSchedulerJobFactory(type);

    auto jobSpecExtId = NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext;
    auto waitingJobTimeout = Config_->WaitingJobsTimeout;

    YT_VERIFY(jobSpec.HasExtension(jobSpecExtId));
    const auto& jobSpecExt = jobSpec.GetExtension(jobSpecExtId);
    if (jobSpecExt.has_waiting_job_timeout()) {
        waitingJobTimeout = FromProto<TDuration>(jobSpecExt.waiting_job_timeout());
    }

    auto job = factory.Run(
        jobId,
        operationId,
        resourceLimits,
        std::move(jobSpec),
        controllerAgentDescriptor);

    YT_LOG_INFO("Scheduler job created (JobId: %v, OperationId: %v, JobType: %v)",
        jobId,
        operationId,
        type);

    RegisterAndStartJob(jobId, job, waitingJobTimeout);

    return job;
}

IJobPtr TJobController::TImpl::CreateMasterJob(
    TJobId jobId,
    TOperationId operationId,
    const TString& jobTrackerAddress,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto type = CheckedEnumCast<EJobType>(jobSpec.type());
    auto factory = GetMasterJobFactory(type);

    auto jobSpecExtId = NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext;
    YT_VERIFY(!jobSpec.HasExtension(jobSpecExtId));

    auto job = factory.Run(
        jobId,
        operationId,
        jobTrackerAddress,
        resourceLimits,
        std::move(jobSpec));

    YT_LOG_INFO("Master job created "
        "(JobId: %v, OperationId: %v, JobType: %v, JobTrackerAddress: %v)",
        jobId,
        operationId,
        type,
        jobTrackerAddress);

    TDuration waitingJobTimeout = Config_->WaitingJobsTimeout;

    RegisterAndStartJob(jobId, job, waitingJobTimeout);

    return job;
}

void TJobController::TImpl::RegisterAndStartJob(const TJobId jobId, const IJobPtr& job, const TDuration waitingJobTimeout)
{
    auto& jobMap = GetJobMap(jobId);
    {
        auto guard = WriterGuard(JobMapLock_);
        EmplaceOrCrash(jobMap, jobId, job);
    }

    job->SubscribeResourcesUpdated(
        BIND_NO_PROPAGATE(&TImpl::OnJobResourcesUpdated, MakeWeak(this), MakeWeak(job)));
    
    job->SubscribeJobPrepared(
        BIND_NO_PROPAGATE(&TImpl::OnJobPrepared, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetJobInvoker()));

    job->SubscribeJobFinished(
        BIND_NO_PROPAGATE(&TImpl::OnJobFinished, MakeWeak(this), MakeWeak(job))
            .Via(Bootstrap_->GetJobInvoker()));

    ScheduleStartMasterJobs();
    ScheduleStartSchedulerJobs();

    // Use #Apply instead of #Subscribe to match #OnWaitingJobTimeout signature.
    TDelayedExecutor::MakeDelayed(waitingJobTimeout)
        .Apply(BIND(&TImpl::OnWaitingJobTimeout, MakeWeak(this), MakeWeak(job), waitingJobTimeout)
        .Via(Bootstrap_->GetJobInvoker()));
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

void TJobController::TImpl::ScheduleStartMasterJobs()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (MasterJobsStartScheduled_) {
        return;
    }

    Bootstrap_->GetJobInvoker()->Invoke(BIND(
        &TImpl::StartWaitingMasterJobs,
        MakeWeak(this)));
    MasterJobsStartScheduled_ = true;
}

void TJobController::TImpl::ScheduleStartSchedulerJobs()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (SchedulerJobsStartScheduled_) {
        return;
    }

    Bootstrap_->GetJobInvoker()->Invoke(BIND(
        &TImpl::StartWaitingSchedulerJobs,
        MakeWeak(this)));
    SchedulerJobsStartScheduled_ = true;
}

void TJobController::TImpl::AbortJob(const IJobPtr& job, TJobToAbort&& abortAttributes)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_INFO("Aborting job (JobId: %v, AbortReason: %v, PreemptionReason: %v)",
        job->GetId(),
        abortAttributes.AbortReason,
        abortAttributes.PreemptionReason);

    TError error(NExecNode::EErrorCode::AbortByScheduler, "Job aborted by scheduler");
    if (abortAttributes.AbortReason) {
        error = error << TErrorAttribute("abort_reason", *abortAttributes.AbortReason);
    }
    if (abortAttributes.PreemptionReason) {
        error = error << TErrorAttribute("preemption_reason", std::move(*abortAttributes.PreemptionReason));
    }

    job->Abort(error);
}

void TJobController::TImpl::RemoveJob(
    const IJobPtr& job,
    const TReleaseJobFlags& releaseFlags)
{
    VERIFY_THREAD_AFFINITY(JobThread);
    YT_VERIFY(job->GetPhase() >= EJobPhase::Cleanup);
    
    {
        auto oneUserSlotResources = ZeroNodeResources();
        oneUserSlotResources.set_user_slots(1);
        YT_VERIFY(Dominates(oneUserSlotResources, job->GetResourceUsage()));
    }

    auto jobId = job->GetId();

    if (releaseFlags.ArchiveJobSpec) {
        YT_LOG_INFO("Archiving job spec (JobId: %v)", jobId);
        job->ReportSpec();
    }

    if (releaseFlags.ArchiveStderr) {
        YT_LOG_INFO("Archiving stderr (JobId: %v)", jobId);
        job->ReportStderr();
    } else {
        // We report zero stderr size to make dynamic tables with jobs and stderrs consistent.
        YT_LOG_INFO("Stderr will not be archived, reporting zero stderr size (JobId: %v)", jobId);
        job->SetStderrSize(0);
    }

    if (releaseFlags.ArchiveFailContext) {
        YT_LOG_INFO("Archiving fail context (JobId: %v)", jobId);
        job->ReportFailContext();
    }

    if (releaseFlags.ArchiveProfile) {
        YT_LOG_INFO("Archiving profile (JobId: %v)", jobId);
        job->ReportProfile();
    }

    bool shouldSave = releaseFlags.ArchiveJobSpec || releaseFlags.ArchiveStderr;
    if (shouldSave) {
        YT_LOG_INFO("Job saved to recently finished jobs (JobId: %v)", jobId);
        RecentlyRemovedJobMap_.emplace(jobId, TRecentlyRemovedJobRecord{job, TInstant::Now()});
    }

    auto& jobMap = GetJobMap(jobId);
    {
        auto guard = WriterGuard(JobMapLock_);
        YT_VERIFY(jobMap.erase(jobId) == 1);
    }

    YT_LOG_INFO("Job removed (JobId: %v, Save: %v)", job->GetId(), shouldSave);
}

void TJobController::TImpl::OnJobResourcesUpdated(const TWeakPtr<IJob>& weakCurrentJob, const TNodeResources& resourceDelta)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto currentJob = weakCurrentJob.Lock();
    YT_VERIFY(currentJob);

    auto jobId = currentJob->GetId();

    YT_LOG_DEBUG("Job resource usage updated (JobId: %v, Delta: %v)", jobId, FormatResources(resourceDelta));

    if (JobResourceManager_->CheckMemoryOverdraft(resourceDelta)) {
        if (currentJob->ResourceUsageOverdrafted()) {
            // TODO(pogorelov): Maybe do not abort job at RunningExtraGpuCheckCommand phase?
            currentJob->Abort(TError(
                NExecNode::EErrorCode::ResourceOverdraft,
                "Failed to increase resource usage")
                << TErrorAttribute("resource_delta", FormatResources(resourceDelta)));
        } else {
            bool foundJobToAbort = false;
            for (const auto& job : GetSchedulerJobs()) {
                if (job->GetState() == EJobState::Running && job->ResourceUsageOverdrafted()) {
                    job->Abort(TError(
                        NExecNode::EErrorCode::ResourceOverdraft,
                        "Failed to increase resource usage on node by some other job with guarantee")
                        << TErrorAttribute("resource_delta", FormatResources(resourceDelta))
                        << TErrorAttribute("other_job_id", currentJob->GetId()));
                    foundJobToAbort = true;
                    break;
                }
            }
            if (!foundJobToAbort) {
                currentJob->Abort(TError(
                    NExecNode::EErrorCode::NodeResourceOvercommit,
                    "Fail to increase resource usage since resource usage on node overcommitted")
                    << TErrorAttribute("resource_delta", FormatResources(resourceDelta)));
            }
        }
        return;
    }
}

void TJobController::TImpl::OnResourceReleased()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    ScheduleStartMasterJobs();
    ScheduleStartSchedulerJobs();
}

void TJobController::TImpl::OnJobPrepared(const TWeakPtr<IJob>& weakJob)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto job = weakJob.Lock();
    if (!job) {
        return;
    }

    YT_VERIFY(job->IsStarted());

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
    auto jobObjectType = TypeFromId(job->GetId());
    switch (jobObjectType) {
        case EObjectType::SchedulerJob:
            origin = EJobOrigin::Scheduler;
            break;

        case EObjectType::MasterJob:
            origin = EJobOrigin::Master;
            break;

        default:
            YT_ABORT();
    }

    if (job->IsUrgent()) {
        YT_LOG_DEBUG("Urgent job has finished, scheduling out-of-order job heartbeat (JobId: %v, JobType: %v, Origin: %v)",
            job->GetId(),
            job->GetType(),
            origin);
        GetJobHeartbeatProcessor(jobObjectType)->ScheduleHeartbeat(job);
    }

    if (!job->IsStarted()) {
        return;
    }

    auto* jobFinalStateCounter = GetJobFinalStateCounter(job->GetState(), origin);
    jobFinalStateCounter->Increment();

    JobFinished_.Fire(job);
}

TFuture<void> TJobController::TImpl::PrepareHeartbeatRequest(
    TCellTag cellTag,
    const TString& jobTrackerAddress,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        BIND(&TJobController::TImpl::DoPrepareHeartbeatRequest, MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run(cellTag, jobTrackerAddress, jobObjectType, request);
}

void TJobController::TImpl::DoPrepareHeartbeatRequest(
    TCellTag cellTag,
    const TString& jobTrackerAddress,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    VERIFY_THREAD_AFFINITY(JobThread);
    
    GetJobHeartbeatProcessor(jobObjectType)->PrepareRequest(cellTag, jobTrackerAddress, request);
}

void TJobController::TImpl::PrepareHeartbeatCommonRequestPart(const TReqHeartbeatPtr& request)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    request->set_node_id(Bootstrap_->GetNodeId());
    ToProto(request->mutable_node_descriptor(), Bootstrap_->GetLocalDescriptor());
    *request->mutable_resource_limits() = JobResourceManager_->GetResourceLimits();
    *request->mutable_resource_usage() = JobResourceManager_->GetResourceUsage(/*includeWaiting*/ true);

    *request->mutable_disk_resources() = JobResourceManager_->GetDiskResources();

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
    const TString& jobTrackerAddress,
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TJobController::TImpl::DoProcessHeartbeatResponse, MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run(jobTrackerAddress, response, jobObjectType);
}

void TJobController::TImpl::DoProcessHeartbeatResponse(
    const TString& jobTrackerAddress,
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    const auto& heartbeatProcessor = GetJobHeartbeatProcessor(jobObjectType);
    heartbeatProcessor->ProcessResponse(jobTrackerAddress, response);
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

    for (const auto& protoJobToAbort : response->jobs_to_abort()) {
        auto jobToAbort = FromProto<TJobToAbort>(protoJobToAbort);

        if (auto job = FindJob(jobToAbort.JobId)) {
            AbortJob(job, std::move(jobToAbort));
        } else {
            YT_LOG_WARNING("Requested to abort a non-existent job (JobId: %v, AbortReason: %v, PreemptionReason: %v)",
                jobToAbort.JobId,
                jobToAbort.AbortReason,
                jobToAbort.PreemptionReason);
        }
    }
}

TFuture<void> TJobController::TImpl::PrepareHeartbeatRequest(
    NNodeTrackerClient::NProto::TReqHeartbeat* request)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND([=, this_ = MakeStrong(this)] {
        VERIFY_THREAD_AFFINITY(JobThread);

        *request->mutable_resource_limits() = JobResourceManager_->GetResourceLimits();
        *request->mutable_resource_usage() = JobResourceManager_->GetResourceUsage(/*includeWaiting*/ true);
    })
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run();
}

const THashMap<TJobId, TOperationId>& TJobController::TImpl::GetSpecFetchFailedJobIds() const noexcept
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

NJobProxy::TJobProxyDynamicConfigPtr TJobController::TImpl::GetJobProxyDynamicConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = DynamicConfig_.Load();
    return config ? config->JobProxy : New<NJobProxy::TJobProxyDynamicConfig>();
}

TBuildInfoPtr TJobController::TImpl::GetBuildInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto buildInfo = CachedJobProxyBuildInfo_.Load();
    if (buildInfo.IsOK()) {
        return buildInfo.Value();
    } else {
        return nullptr;
    }
}

bool TJobController::TImpl::AreSchedulerJobsDisabled() const noexcept
{
    return DisableSchedulerJobs_.load();
}

TFuture<void> TJobController::TImpl::RequestJobSpecsAndStartJobs(std::vector<TJobStartInfo> jobStartInfos)
{
    THashMap<TControllerAgentDescriptor, std::vector<TJobStartInfo>> groupedStartInfos;

    for (auto& startInfo : jobStartInfos) {
        auto operationId = FromProto<TOperationId>(startInfo.operation_id());
        auto jobId = FromProto<TJobId>(startInfo.job_id());

        auto agentDescriptorOrError = TryParseControllerAgentDescriptor(startInfo.controller_agent_descriptor());

        if (agentDescriptorOrError.IsOK()) {
            auto& agentDescriptor = agentDescriptorOrError.Value();
            YT_LOG_DEBUG("Job spec will be requested (OperationId: %v, JobId: %v, SpecServiceAddress: %v)",
                operationId,
                jobId,
                agentDescriptor.Address);
            groupedStartInfos[std::move(agentDescriptor)].push_back(startInfo);
        } else {
            YT_LOG_DEBUG(agentDescriptorOrError, "Job spec cannot be requested (OperationId: %v, JobId: %v)",
                operationId,
                jobId);
            YT_VERIFY(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
        }
    }

    std::vector<TFuture<void>> asyncResults;
    for (auto& [agentDescriptor, startInfos] : groupedStartInfos) {
        const auto& channel = Bootstrap_
            ->GetExecNodeBootstrap()
            ->GetControllerAgentConnectorPool()
            ->GetOrCreateChannel(agentDescriptor);
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
            agentDescriptor.Address,
            startInfos.size());

        auto asyncResult = jobSpecRequest->Invoke().Apply(
            BIND(
                &TJobController::TImpl::OnJobSpecsReceived,
                MakeStrong(this),
                Passed(std::move(startInfos)),
                agentDescriptor)
            .AsyncVia(Bootstrap_->GetJobInvoker()));
        asyncResults.push_back(asyncResult);
    }

    return AllSet(asyncResults).As<void>();
}

void TJobController::TImpl::OnJobSpecsReceived(
    std::vector<TJobStartInfo> startInfos,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    const TJobSpecServiceProxy::TErrorOrRspGetJobSpecsPtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (!rspOrError.IsOK()) {
        YT_LOG_DEBUG(rspOrError, "Error getting job specs (SpecServiceAddress: %v)",
            controllerAgentDescriptor.Address);
        for (const auto& startInfo : startInfos) {
            auto jobId = FromProto<TJobId>(startInfo.job_id());
            auto operationId = FromProto<TOperationId>(startInfo.operation_id());
            YT_VERIFY(SpecFetchFailedJobIds_.insert({jobId, operationId}).second);
        }
        return;
    }

    YT_LOG_DEBUG("Job specs received (SpecServiceAddress: %v)", controllerAgentDescriptor.Address);

    const auto& rsp = rspOrError.Value();

    YT_VERIFY(rsp->responses_size() == std::ssize(startInfos));
    for (size_t index = 0; index < startInfos.size(); ++index) {
        auto& startInfo = startInfos[index];
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

        startInfo.mutable_resource_limits()->set_vcpu(
            static_cast<double>(NVectorHdrf::TCpuResource(
                startInfo.resource_limits().cpu() * JobResourceManager_->GetCpuToVCpuFactor())));

        CreateSchedulerJob(
            jobId,
            operationId,
            startInfo.resource_limits(),
            std::move(spec),
            controllerAgentDescriptor);
    }
}

TEnumIndexedVector<EJobOrigin, std::vector<IJobPtr>> TJobController::TImpl::GetJobsByOrigin() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TEnumIndexedVector<EJobOrigin, std::vector<IJobPtr>> result;
    result[EJobOrigin::Master] = GetMasterJobs();
    result[EJobOrigin::Scheduler] = GetSchedulerJobs();

    return result;
}

void TJobController::TImpl::OnDynamicConfigChanged(
    const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
    const TClusterNodeDynamicConfigPtr& newNodeConfig)
{
    auto jobControllerConfig = newNodeConfig->ExecNode->JobController;
    YT_ASSERT(jobControllerConfig);
    DynamicConfig_.Store(jobControllerConfig);

    ProfilingExecutor_->SetPeriod(
        jobControllerConfig->ProfilingPeriod.value_or(
            Config_->ProfilingPeriod));
    ResourceAdjustmentExecutor_->SetPeriod(
        jobControllerConfig->ResourceAdjustmentPeriod.value_or(
            Config_->ResourceAdjustmentPeriod));
    RecentlyRemovedJobCleaner_->SetPeriod(
        jobControllerConfig->RecentlyRemovedJobsCleanPeriod.value_or(
            Config_->RecentlyRemovedJobsCleanPeriod));
    JobProxyBuildInfoUpdater_->SetPeriod(
        jobControllerConfig->JobProxyBuildInfoUpdatePeriod.value_or(
            Config_->JobProxyBuildInfoUpdatePeriod));
}

void TJobController::TImpl::BuildOrchid(IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto jobs = GetJobsByOrigin();
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("resource_limits").Value(JobResourceManager_->GetResourceLimits())
            .Item("resource_usage").Value(JobResourceManager_->GetResourceUsage())
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
                                    .Item("job_tracker_address").Value(job->GetJobTrackerAddress())
                                    .Item("start_time").Value(job->GetStartTime())
                                    .Item("duration").Value(TInstant::Now() - job->GetStartTime())
                                    .OptionalItem("statistics", job->GetStatistics())
                                    .OptionalItem("operation_id", job->GetOperationId())
                                    .Item("resource_usage").Value(job->GetResourceUsage())
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

    auto buildInfo = CachedJobProxyBuildInfo_.Load();

    if (buildInfo.IsOK()) {
        fluent.Value(buildInfo.Value());
    } else {
        fluent
            .BeginMap()
                .Item("error").Value(static_cast<TError>(buildInfo))
            .EndMap();
    }
}

void TJobController::TImpl::UpdateJobProxyBuildInfo()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    // TODO(max42): not sure if running ytserver-job-proxy --build --yson from JobThread
    // is a good idea; maybe delegate to another thread?

    TErrorOr<TBuildInfoPtr> buildInfo;

    try {
        auto jobProxyPath = ResolveBinaryPath(JobProxyProgramName)
            .ValueOrThrow();

        TSubprocess jobProxy(jobProxyPath);
        jobProxy.AddArguments({"--build", "--yson"});

        auto result = jobProxy.Execute();
        result.Status.ThrowOnError();

        buildInfo = ConvertTo<TBuildInfoPtr>(TYsonString(result.Output));
    } catch (const std::exception& ex) {
        buildInfo = TError(NExecNode::EErrorCode::JobProxyUnavailable, "Failed to receive job proxy build info")
            << ex;
    }

    CachedJobProxyBuildInfo_.Store(buildInfo);

    JobProxyBuildInfoUpdated_.Fire(static_cast<TError>(buildInfo));
}

IYPathServicePtr TJobController::TImpl::GetOrchidService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IYPathService::FromProducer(BIND_NO_PROPAGATE(&TImpl::BuildOrchid, MakeStrong(this)))
        ->Via(Bootstrap_->GetJobInvoker());
}

void TJobController::TImpl::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    ActiveJobCountBuffer_->Update([this] (ISensorWriter* writer) {
        auto jobs = GetJobsByOrigin();

        for (auto origin : TEnumTraits<EJobOrigin>::GetDomainValues()) {
            TWithTagGuard tagGuard(writer, "origin", FormatEnum(origin));
            writer->AddGauge("/active_job_count", jobs[origin].size());
        }
    });

    if (Bootstrap_->IsExecNode()) {
        const auto& gpuManager = Bootstrap_->GetExecNodeBootstrap()->GetGpuManager();
        GpuUtilizationBuffer_->Update([gpuManager] (ISensorWriter* writer) {
            for (const auto& [index, gpuInfo] : gpuManager->GetGpuInfoMap()) {
                TWithTagGuard tagGuard(writer);
                tagGuard.AddTag("gpu_name", gpuInfo.Name);
                tagGuard.AddTag("device_number", ToString(index));
                ProfileGpuInfo(writer, gpuInfo);
            }
        });
    }

    i64 tmpfsSize = 0;
    i64 tmpfsUsage = 0;
    for (const auto& job : GetSchedulerJobs()) {
        if (job->GetState() != EJobState::Running || job->GetPhase() != EJobPhase::Running) {
            continue;
        }

        const auto& jobSpec = job->GetSpec();
        auto jobSpecExtId = NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext;
        if (!jobSpec.HasExtension(jobSpecExtId)) {
            continue;
        }

        const auto& jobSpecExt = jobSpec.GetExtension(jobSpecExtId);
        if (!jobSpecExt.has_user_job_spec()) {
            continue;
        }

        for (const auto& tmpfsVolumeProto : jobSpecExt.user_job_spec().tmpfs_volumes()) {
            tmpfsSize += tmpfsVolumeProto.size();
        }

        auto statisticsYson = job->GetStatistics();
        if (!statisticsYson) {
            continue;
        }

        static const TString sensorName = "/user_job/tmpfs_size/sum";

        auto tmpfsSizeSum = TryGetInt64(statisticsYson.AsStringBuf(), sensorName);
        if (!tmpfsSizeSum) {
            continue;
        }

        tmpfsUsage += *tmpfsSizeSum;
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

TErrorOr<TControllerAgentDescriptor> TJobController::TImpl::TryParseControllerAgentDescriptor(
    const NJobTrackerClient::NProto::TControllerAgentDescriptor& proto) const
{
    const auto incarnationId = FromProto<NScheduler::TIncarnationId>(proto.incarnation_id());

    auto addressOrError = TryParseControllerAgentAddress(proto.addresses());
    if (!addressOrError.IsOK()) {
        return TError{std::move(addressOrError)};
    }

    return TControllerAgentDescriptor{std::move(addressOrError.Value()), incarnationId};
}

TErrorOr<TString> TJobController::TImpl::TryParseControllerAgentAddress(
    const NNodeTrackerClient::NProto::TAddressMap& proto) const
{
    const auto addresses = FromProto<NNodeTrackerClient::TAddressMap>(proto);
    try {
        return GetAddressOrThrow(addresses, Bootstrap_->GetLocalNetworks());
    } catch (const std::exception& ex) {
        return TError{
            "No suitable controller agent address exists (SpecServiceAddresses: %v)",
            GetValues(addresses)}
            << TError{ex};
    }
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
        std::move(config),
        bootstrap))
{ }

TJobController::~TJobController() = default;

void TJobController::Initialize()
{
    Impl_->Initialize();
}

void TJobController::RegisterSchedulerJobFactory(
    EJobType type,
    TSchedulerJobFactory factory)
{
    Impl_->RegisterSchedulerJobFactory(type, std::move(factory));
}

void TJobController::RegisterMasterJobFactory(
    EJobType type,
    TMasterJobFactory factory)
{
    Impl_->RegisterMasterJobFactory(type, std::move(factory));
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

std::vector<IJobPtr> TJobController::GetSchedulerJobs() const
{
    return Impl_->GetSchedulerJobs();
}

std::vector<IJobPtr> TJobController::GetMasterJobs() const
{
    return Impl_->GetMasterJobs();
}

void TJobController::SetDisableSchedulerJobs(bool value)
{
    Impl_->SetDisableSchedulerJobs(value);
}

TFuture<void> TJobController::PrepareHeartbeatRequest(
    TCellTag cellTag,
    const TString& jobTrackerAddress,
    EObjectType jobObjectType,
    const TReqHeartbeatPtr& request)
{
    return Impl_->PrepareHeartbeatRequest(
        cellTag,
        jobTrackerAddress,
        jobObjectType,
        request);
}

TFuture<void> TJobController::ProcessHeartbeatResponse(
    const TString& jobTrackerAddress,
    const TRspHeartbeatPtr& response,
    EObjectType jobObjectType)
{
    return Impl_->ProcessHeartbeatResponse(
        jobTrackerAddress,
        response,
        jobObjectType);
}

TFuture<void> TJobController::PrepareHeartbeatRequest(
    NNodeTrackerClient::NProto::TReqHeartbeat* heartbeat)
{
    return Impl_->PrepareHeartbeatRequest(heartbeat);
}

IYPathServicePtr TJobController::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

bool TJobController::IsJobProxyProfilingDisabled() const
{
    return Impl_->IsJobProxyProfilingDisabled();
}

NJobProxy::TJobProxyDynamicConfigPtr TJobController::GetJobProxyDynamicConfig() const
{
    return Impl_->GetJobProxyDynamicConfig();
}

bool TJobController::AreSchedulerJobsDisabled() const noexcept
{
    return Impl_->AreSchedulerJobsDisabled();
}

TBuildInfoPtr TJobController::GetBuildInfo() const
{
    return Impl_->GetBuildInfo();
}

void TJobController::RegisterHeartbeatProcessor(
    const EObjectType type,
    TJobHeartbeatProcessorBasePtr heartbeatProcessor)
{
    Impl_->RegisterHeartbeatProcessor(type, std::move(heartbeatProcessor));
}

DELEGATE_SIGNAL(TJobController, void(const IJobPtr&), JobFinished, *Impl_)
DELEGATE_SIGNAL(TJobController, void(const TError&), JobProxyBuildInfoUpdated, *Impl_)

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

bool TJobController::TJobHeartbeatProcessorBase::NeedTotalConfirmation() const noexcept
{
    return JobController_->Impl_->NeedTotalConfirmation();
}

TFuture<void> TJobController::TJobHeartbeatProcessorBase::RequestJobSpecsAndStartJobs(
    std::vector<TJobStartInfo> jobStartInfos)
{
    return JobController_->Impl_->RequestJobSpecsAndStartJobs(std::move(jobStartInfos));
}

IJobPtr TJobController::TJobHeartbeatProcessorBase::CreateMasterJob(
    TJobId jobId,
    TOperationId operationId,
    const TString& jobTrackerAddress,
    const TNodeResources& resourceLimits,
    TJobSpec&& jobSpec)
{
    return JobController_->Impl_->CreateMasterJob(
        jobId,
        operationId,
        jobTrackerAddress,
        resourceLimits,
        std::move(jobSpec));
}

const THashMap<TJobId, TOperationId>& TJobController::TJobHeartbeatProcessorBase::GetSpecFetchFailedJobIds() const noexcept
{
    return JobController_->Impl_->GetSpecFetchFailedJobIds();
}

void TJobController::TJobHeartbeatProcessorBase::PrepareHeartbeatCommonRequestPart(const TReqHeartbeatPtr& request)
{
    JobController_->Impl_->PrepareHeartbeatCommonRequestPart(request);
}

void TJobController::TJobHeartbeatProcessorBase::ProcessHeartbeatCommonResponsePart(const TRspHeartbeatPtr& response)
{
    JobController_->Impl_->ProcessHeartbeatCommonResponsePart(response);
}

TErrorOr<TControllerAgentDescriptor> TJobController::TJobHeartbeatProcessorBase::TryParseControllerAgentDescriptor(
    const NJobTrackerClient::NProto::TControllerAgentDescriptor& proto) const
{
    return JobController_->Impl_->TryParseControllerAgentDescriptor(proto);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
