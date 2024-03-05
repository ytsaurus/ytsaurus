#include "job_controller.h"

#include "bootstrap.h"
#include "helpers.h"
#include "job.h"
#include "job_info.h"
#include "private.h"
#include "scheduler_connector.h"
#include "slot_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/job_controller.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/job_agent/config.h>
#include <yt/yt/server/lib/job_agent/structs.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/library/process/process.h>
#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NJobAgent;
using namespace NClusterNode;
using namespace NObjectClient;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NScheduler::NProto::NNode;
using namespace NScheduler::NProto;
using namespace NControllerAgent::NProto;
using namespace NProfiling;
using namespace NScheduler;
using namespace NControllerAgent;

using NNodeTrackerClient::NProto::TNodeResources;

using TControllerAgentConnectorPtr = TControllerAgentConnectorPool::TControllerAgentConnectorPtr;

using TJobStartInfo = TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

NScheduler::TAllocationToAbort ParseAllocationToAbort(const NScheduler::NProto::NNode::TAllocationToAbort& allocationToAbortProto)
{
    NScheduler::TAllocationToAbort result;

    FromProto(&result, allocationToAbortProto);

    return result;
}

// COMPAT(pogorelov): AllocationId is currently equal to JobId.

TJobId FromAllocationId(TAllocationId allocationId)
{
    return TJobId(allocationId.Underlying());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJobController
    : public IJobController
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(const TJobPtr& job), JobRegistered);
    DEFINE_SIGNAL_OVERRIDE(
        void(TAllocationId, TOperationId, const TControllerAgentDescriptor&, const TError&),
        AllocationFailed);
    DEFINE_SIGNAL_OVERRIDE(void(const TJobPtr& job), JobFinished);
    DEFINE_SIGNAL_OVERRIDE(void(const TError& error), JobProxyBuildInfoUpdated);

public:
    TJobController(IBootstrapBase* bootstrap)
        : Bootstrap_(bootstrap)
        , DynamicConfig_(New<TJobControllerDynamicConfig>())
        , OperationInfoRequestBackoffStrategy_(DynamicConfig_.Acquire()->OperationInfoRequestBackoffStrategy)
        , Profiler_("/job_controller")
        , CacheHitArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_hit_artifacts_size"))
        , CacheMissArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_miss_artifacts_size"))
        , CacheBypassedArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_bypassed_artifacts_size"))
        , TmpfsUsageGauge_(Profiler_.Gauge("/tmpfs/usage"))
        , TmpfsLimitGauge_(Profiler_.Gauge("/tmpfs/limit"))
        , JobProxyMaxMemoryGauge_(Profiler_.Gauge("/job_proxy_max_memory"))
        , UserJobMaxMemoryGauge_(Profiler_.Gauge("/user_job_max_memory"))
    {
        YT_VERIFY(Bootstrap_);

        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

        Profiler_.AddProducer("/gpu_utilization", GpuUtilizationBuffer_);
        Profiler_.AddProducer("", ActiveJobCountBuffer_);
    }

    void Initialize() override
    {
        auto dynamicConfig = GetDynamicConfig();

        JobResourceManager_ = Bootstrap_->GetJobResourceManager();
        JobResourceManager_->RegisterResourcesConsumer(
            BIND_NO_PROPAGATE(&TJobController::OnResourceReleased, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()),
            EResourcesConsumerType::SchedulerJob);
        JobResourceManager_->SubscribeReservedMemoryOvercommited(
            BIND_NO_PROPAGATE(&TJobController::OnReservedMemoryOvercommited, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()));
        JobResourceManager_->SubscribeResourceUsageOverdrafted(
            BIND_NO_PROPAGATE(&TJobController::OnResourceUsageOverdrafted, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()));
        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::OnProfiling, MakeWeak(this)),
            dynamicConfig->ProfilingPeriod);
        ResourceAdjustmentExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::AdjustResources, MakeWeak(this)),
            dynamicConfig->ResourceAdjustmentPeriod);
        RecentlyRemovedJobCleaner_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::CleanRecentlyRemovedJobs, MakeWeak(this)),
            dynamicConfig->RecentlyRemovedJobsCleanPeriod);
        JobProxyBuildInfoUpdater_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::UpdateJobProxyBuildInfo, MakeWeak(this)));
    }

    void Start() override
    {
        auto dynamicConfig = GetDynamicConfig();

        ProfilingExecutor_->Start();
        ResourceAdjustmentExecutor_->Start();
        RecentlyRemovedJobCleaner_->Start();
        JobProxyBuildInfoUpdater_->Start();

        // Get ready event before actual start.
        auto buildInfoReadyEvent = JobProxyBuildInfoUpdater_->GetExecutedEvent();

        // Actual start and fetch initial job proxy build info immediately. No need to call ScheduleOutOfBand.
        JobProxyBuildInfoUpdater_->SetPeriod(dynamicConfig->JobProxyBuildInfoUpdatePeriod);

        // Wait synchronously for one update in order to get some reasonable value in CachedJobProxyBuildInfo_.
        // Note that if somebody manages to request orchid before this field is set, this will result to nullptr
        // dereference.
        WaitFor(buildInfoReadyEvent)
            .ThrowOnError();
    }

    TJobPtr FindJob(TJobId jobId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(JobMapLock_);
        auto it = JobMap_.find(jobId);
        return it == JobMap_.end() ? nullptr : it->second;
    }

    TJobPtr GetJobOrThrow(TJobId jobId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto job = FindJob(jobId);
        if (!job) {
            THROW_ERROR_EXCEPTION(
                NExecNode::EErrorCode::NoSuchJob,
                "Job %v is unknown",
                jobId);
        }
        return job;
    }

    TJobPtr FindRecentlyRemovedJob(TJobId jobId) const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto it = RecentlyRemovedJobMap_.find(jobId);
        return it == RecentlyRemovedJobMap_.end() ? nullptr : it->second.Job;
    }

    void SetJobsDisabledByMaster(bool value) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        JobsDisabledByMaster_.store(value);

        if (value) {
            TError error{"All scheduler jobs are disabled"};

            Bootstrap_->GetJobInvoker()->Invoke(BIND([=, this, this_ = MakeStrong(this), error{std::move(error)}] {
                VERIFY_THREAD_AFFINITY(JobThread);

                InterruptAllJobs(std::move(error));
            }));
        }
    }

    void PrepareAgentHeartbeatRequest(
        const TControllerAgentConnectorPool::TControllerAgentConnector::TReqHeartbeatPtr& request,
        const TAgentHeartbeatContextPtr& context) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        DoPrepareAgentHeartbeatRequest(request, context);
    }

    void ProcessAgentHeartbeatResponse(
        const TControllerAgentConnectorPool::TControllerAgentConnector::TRspHeartbeatPtr& response,
        const TAgentHeartbeatContextPtr& context) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        DoProcessAgentHeartbeatResponse(response, context);
    }

    void PrepareSchedulerHeartbeatRequest(
        const TSchedulerConnector::TReqHeartbeatPtr& request,
        const TSchedulerHeartbeatContextPtr& context) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        DoPrepareSchedulerHeartbeatRequest(request, context);
    }

    void ProcessSchedulerHeartbeatResponse(
        const TSchedulerConnector::TRspHeartbeatPtr& response,
        const TSchedulerHeartbeatContextPtr& context) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        DoProcessSchedulerHeartbeatResponse(response, context);
    }

    bool IsJobProxyProfilingDisabled() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetDynamicConfig()->DisableJobProxyProfiling;
    }

    NJobProxy::TJobProxyDynamicConfigPtr GetJobProxyDynamicConfig() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetDynamicConfig()->JobProxy;
    }

    TJobControllerDynamicConfigPtr GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DynamicConfig_.Acquire();
    }

    TBuildInfoPtr GetBuildInfo() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto buildInfo = CachedJobProxyBuildInfo_.Load();
        if (buildInfo.IsOK()) {
            return buildInfo.Value();
        } else {
            return nullptr;
        }
    }

    bool AreJobsDisabled() const noexcept override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();

        return JobsDisabledByMaster_.load() || slotManager->HasFatalAlert();
    }

    void ScheduleStartJobs()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (StartJobsScheduled_) {
            return;
        }

        Bootstrap_->GetJobInvoker()->Invoke(BIND(
            &TJobController::StartWaitingJobs,
            MakeWeak(this)));
        StartJobsScheduled_ = true;
    }

    IYPathServicePtr GetOrchidService() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND_NO_PROPAGATE(
            &TJobController::BuildOrchid,
            MakeStrong(this)));
    }

    void OnAgentIncarnationOutdated(const TControllerAgentDescriptor& controllerAgentDescriptor) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        for (TForbidContextSwitchGuard guard; const auto& [id, job] : JobMap_) {
            if (job->GetControllerAgentDescriptor() == controllerAgentDescriptor) {
                job->UpdateControllerAgentDescriptor({});
            }
        }
    }

    void OnJobMemoryThrashing(TJobId jobId) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto job = GetJobOrThrow(jobId);
        job->Abort(TError("Aborting job due to extensive memory thrashing in job container")
            << TErrorAttribute("abort_reason", NScheduler::EAbortReason::JobMemoryThrashing));
    }

    TFuture<void> AbortAllJobs(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO(error, "Abort all jobs");

        std::vector<TFuture<void>> jobResourceReleaseFutures;
        jobResourceReleaseFutures.reserve(std::size(JobsWaitingForCleanup_) + std::size(JobMap_));

        for (const auto& job : JobsWaitingForCleanup_) {
            jobResourceReleaseFutures.push_back(job->GetCleanupFinishedEvent());
        }

        for (TForbidContextSwitchGuard guard; const auto& [jobId, job] : JobMap_) {
            YT_LOG_INFO("Aborting job (JobId: %v)", jobId);
            job->Abort(error);

            jobResourceReleaseFutures.push_back(job->GetCleanupFinishedEvent());
        }

        return AllSet(std::move(jobResourceReleaseFutures))
            .AsVoid();
    }

    TFuture<void> GetAllJobsCleanedupFuture() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        std::vector<TFuture<void>> jobResourceReleaseFutures;
        jobResourceReleaseFutures.reserve(std::size(JobsWaitingForCleanup_) + std::size(JobMap_));

        for (const auto& job : JobsWaitingForCleanup_) {
            jobResourceReleaseFutures.push_back(job->GetCleanupFinishedEvent());
        }

        for (TForbidContextSwitchGuard guard; const auto& [jobId, job] : JobMap_) {
            jobResourceReleaseFutures.push_back(job->GetCleanupFinishedEvent());
        }

        return AllSet(std::move(jobResourceReleaseFutures))
            .AsVoid();
    }

    void OnDynamicConfigChanged(
        const TJobControllerDynamicConfigPtr& /*oldConfig*/,
        const TJobControllerDynamicConfigPtr& newConfig) override
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        DynamicConfig_.Store(newConfig);

        OperationInfoRequestBackoffStrategy_.UpdateOptions(newConfig->OperationInfoRequestBackoffStrategy);
        ProfilingExecutor_->SetPeriod(
            newConfig->ProfilingPeriod);
        ResourceAdjustmentExecutor_->SetPeriod(
            newConfig->ResourceAdjustmentPeriod);
        RecentlyRemovedJobCleaner_->SetPeriod(
            newConfig->RecentlyRemovedJobsCleanPeriod);
        JobProxyBuildInfoUpdater_->SetPeriod(
            newConfig->JobProxyBuildInfoUpdatePeriod);
    }

    TGuid RegisterThrottlingRequest(TFuture<void> future) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        auto id = TGuid::Create();
        YT_VERIFY(OutstandingThrottlingRequests_.emplace(id, future).second);
        // Remove future from outstanding requests after it was set + timeout.
        future.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& /*error*/) {
            TDelayedExecutor::Submit(
                BIND(&TJobController::EvictThrottlingRequest, this_, id).Via(Bootstrap_->GetJobInvoker()),
                GetDynamicConfig()->JobCommon->JobThrottler->MaxBackoffTime * 2);
        }));
        return id;
    }

    TFuture<void> GetThrottlingRequestOrThrow(TGuid id) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        auto future = FindThrottlingRequest(id);
        if (!future) {
            THROW_ERROR_EXCEPTION("Unknown throttling request %v", id);
        }
        return future;
    }

    void EvictThrottlingRequest(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        YT_LOG_DEBUG("Outstanding throttling request evicted (ThrottlingRequestId: %v)",
            id);
        YT_VERIFY(OutstandingThrottlingRequests_.erase(id) == 1);
    }

    TFuture<void> FindThrottlingRequest(TGuid id)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        auto it = OutstandingThrottlingRequests_.find(id);
        return it == OutstandingThrottlingRequests_.end() ? TFuture<void>() : it->second;
    }

private:
    NClusterNode::IBootstrapBase* const Bootstrap_;
    TAtomicIntrusivePtr<TJobControllerDynamicConfig> DynamicConfig_;

    TJobResourceManagerPtr JobResourceManager_;

    // For converting vcpu to cpu back after getting response from scheduler.
    // It is needed because cpu_to_vcpu_factor can change between preparing request and processing response.
    double LastHeartbeatCpuToVCpuFactor_ = 1.0;

    TRelativeConstantBackoffStrategy OperationInfoRequestBackoffStrategy_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, JobMapLock_);
    THashMap<TJobId, TJobPtr> JobMap_;
    THashMap<TOperationId, THashSet<TJobPtr>> OperationIdToJobs_;

    THashSet<TJobPtr> JobsWaitingForCleanup_;

    // Map of jobs to hold after remove. It is used to prolong lifetime of stderrs and job specs.
    struct TRecentlyRemovedJobRecord
    {
        TJobPtr Job;
        TInstant RemovalTime;
    };
    THashMap<TJobId, TRecentlyRemovedJobRecord> RecentlyRemovedJobMap_;

    bool StartJobsScheduled_ = false;

    std::atomic<bool> JobsDisabledByMaster_ = false;

    std::optional<TInstant> UserMemoryOverdraftInstant_;
    std::optional<TInstant> CpuOverdraftInstant_;

    TProfiler Profiler_;
    TBufferedProducerPtr GpuUtilizationBuffer_ = New<TBufferedProducer>();
    TBufferedProducerPtr ActiveJobCountBuffer_ = New<TBufferedProducer>();
    THashMap<EJobState, TCounter> JobFinalStateCounters_;

    // Chunk cache counters.
    TCounter CacheHitArtifactsSizeCounter_;
    TCounter CacheMissArtifactsSizeCounter_;
    TCounter CacheBypassedArtifactsSizeCounter_;

    TGauge TmpfsUsageGauge_;
    TGauge TmpfsLimitGauge_;
    TGauge JobProxyMaxMemoryGauge_;
    TGauge UserJobMaxMemoryGauge_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr ResourceAdjustmentExecutor_;
    TPeriodicExecutorPtr RecentlyRemovedJobCleaner_;
    TPeriodicExecutorPtr JobProxyBuildInfoUpdater_;

    TAtomicObject<TErrorOr<TBuildInfoPtr>> CachedJobProxyBuildInfo_;

    THashMap<TGuid, TFuture<void>> OutstandingThrottlingRequests_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    TFuture<TJobStartInfo>
    SettleJob(
        const TControllerAgentDescriptor& controllerAgentDescriptor,
        TOperationId operationId,
        TAllocationId allocationId)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        const auto& controllerAgentConnectorPool = Bootstrap_
            ->GetExecNodeBootstrap()
            ->GetControllerAgentConnectorPool();

        return controllerAgentConnectorPool->SettleJob(
            controllerAgentDescriptor,
            operationId,
            allocationId);
    }

    TError MakeJobsDisabledError() const
    {
        auto error = TError("Jobs disabled on node")
            << TErrorAttribute("abort_reason", EAbortReason::NodeWithDisabledJobs);
        return error;
    }

    std::vector<TJobPtr> GetJobs()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TForbidContextSwitchGuard guard;

        std::vector<TJobPtr> currentJobs;
        currentJobs.reserve(JobMap_.size());

        for (const auto& [id, job] : JobMap_) {
            currentJobs.push_back(job);
        }

        return currentJobs;
    }

    void SettleAndStartJobs(std::vector<TAllocationStartInfo> allocationStartInfoProtos)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TForbidContextSwitchGuard guard;

        bool areJobsDisabled = AreJobsDisabled();

        for (auto& startInfoProto : allocationStartInfoProtos) {
            auto operationId = FromProto<TOperationId>(startInfoProto.operation_id());
            auto allocationId = FromProto<TAllocationId>(startInfoProto.allocation_id());

            auto incarnationId = FromProto<NScheduler::TIncarnationId>(
                startInfoProto.controller_agent_descriptor().incarnation_id());

            const auto& controllerAgentConnectorPool = Bootstrap_->GetExecNodeBootstrap()->GetControllerAgentConnectorPool();
            auto maybeAgentDescriptor = controllerAgentConnectorPool->GetDescriptorByIncarnationId(incarnationId);
            YT_VERIFY(maybeAgentDescriptor);

            auto agentDescriptor = std::move(*maybeAgentDescriptor);

            YT_LOG_INFO(
                "Requested to create allocation (OperationId: %v, AllocationId: %v, ControllerAgentDescriptor: %v)",
                operationId,
                allocationId,
                agentDescriptor);

            if (areJobsDisabled) {
                const auto& allocationAbortingError = MakeJobsDisabledError();

                YT_LOG_INFO(
                    "Allocation not created since jobs disabled on node (OperationId: %v, AllocationId: %v, ControllerAgentDescriptor: %v)",
                    operationId,
                    allocationId,
                    agentDescriptor);

                AllocationFailed_.Fire(
                    allocationId,
                    operationId,
                    agentDescriptor,
                    allocationAbortingError);

                continue;
            }

            SettleJob(agentDescriptor, operationId, allocationId)
                .SubscribeUnique(BIND([
                    operationId,
                    allocationId,
                    resourceLimits = startInfoProto.resource_limits(),
                    agentDescriptor,
                    this,
                    this_ = MakeStrong(this)
                ] (TErrorOr<TJobStartInfo>&& jobInfoOrError) mutable
                {
                    resourceLimits.set_vcpu(
                        static_cast<double>(NVectorHdrf::TCpuResource(
                            resourceLimits.cpu() * JobResourceManager_->GetCpuToVCpuFactor())));
                    OnJobStartInfoReceived(
                        allocationId,
                        operationId,
                        resourceLimits,
                        agentDescriptor,
                        std::move(jobInfoOrError));
                })
                    .Via(Bootstrap_->GetJobInvoker()));
        }
    }

    NClusterNode::TJobResources BuildJobResources(
        const TNodeResources& nodeResources,
        const TJobSpecExt* jobSpecExt)
    {
        auto resources = FromNodeResources(nodeResources);
        const auto* userJobSpec = jobSpecExt && jobSpecExt->has_user_job_spec()
            ? &jobSpecExt->user_job_spec()
            : nullptr;

        resources.DiskSpaceRequest = GetDynamicConfig()->MinRequiredDiskSpace;
        if (userJobSpec) {
            // COMPAT(ignat)
            if (userJobSpec->has_disk_space_limit()) {
                resources.DiskSpaceRequest = userJobSpec->disk_space_limit();
            }

            if (userJobSpec->has_disk_request()) {
                resources.DiskSpaceRequest = userJobSpec->disk_request().disk_space();
                resources.InodeRequest = userJobSpec->disk_request().inode_count();
            }
        }

        return resources;
    }

    NClusterNode::TJobResourceAttributes BuildJobResourceAttributes(const TJobSpecExt* jobSpecExt)
    {
        const auto* userJobSpec = jobSpecExt && jobSpecExt->has_user_job_spec()
            ? &jobSpecExt->user_job_spec()
            : nullptr;

        TJobResourceAttributes resourceAttributes;
        resourceAttributes.AllowIdleCpuPolicy = jobSpecExt->allow_idle_cpu_policy();

        if (userJobSpec) {
            if (userJobSpec->has_disk_request() && userJobSpec->disk_request().has_medium_index()) {
                resourceAttributes.MediumIndex = userJobSpec->disk_request().medium_index();
            }

            if (userJobSpec->has_cuda_toolkit_version()) {
                resourceAttributes.CudaToolkitVersion = userJobSpec->cuda_toolkit_version();
            }
        }

        return resourceAttributes;
    }

    void OnJobStartInfoReceived(
        const TAllocationId& allocationId,
        const TOperationId& operationId,
        const TNodeResources& resourceLimits,
        const TControllerAgentDescriptor& controllerAgentDescriptor,
        TErrorOr<TJobStartInfo>&& jobInfoOrError)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Starting settled job (AllocationId: %v, OperationId: %v)", allocationId, operationId);

        if (!jobInfoOrError.IsOK()) {
            YT_LOG_DEBUG(
                jobInfoOrError,
                "No job is available for allocation (OperationId: %v, AllocationId: %v)",
                operationId,
                allocationId);

            AllocationFailed_.Fire(
                allocationId,
                operationId,
                controllerAgentDescriptor,
                jobInfoOrError);

            return;
        }

        auto& jobInfo = jobInfoOrError.Value();
        auto* jobSpecExt = &jobInfo.JobSpec.GetExtension(TJobSpecExt::job_spec_ext);

        auto resources = BuildJobResources(resourceLimits, jobSpecExt);
        auto resourceAttributes = BuildJobResourceAttributes(jobSpecExt);

        CreateJob(
            jobInfo.JobId,
            operationId,
            resources,
            resourceAttributes,
            std::move(jobInfo.JobSpec),
            controllerAgentDescriptor);
    }

    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        static const TString tmpfsSizeSensorName = "/user_job/tmpfs_size/sum";
        static const TString jobProxyMaxMemorySensorName = "/job_proxy/max_memory/sum";
        static const TString userJobMaxMemorySensorName = "/user_job/max_memory/sum";

        ActiveJobCountBuffer_->Update([this] (ISensorWriter* writer) {
            TWithTagGuard tagGuard(writer, "origin", FormatEnum(EJobOrigin::Scheduler));

            writer->AddGauge("/active_job_count", JobMap_.size());
        });

        const auto& gpuManager = Bootstrap_->GetExecNodeBootstrap()->GetGpuManager();
        GpuUtilizationBuffer_->Update([gpuManager] (ISensorWriter* writer) {
            for (const auto& [index, gpuInfo] : gpuManager->GetGpuInfoMap()) {
                TWithTagGuard tagGuard(writer);
                tagGuard.AddTag("gpu_name", gpuInfo.Name);
                tagGuard.AddTag("device_number", ToString(index));
                ProfileGpuInfo(writer, gpuInfo);
            }
        });

        i64 totalJobProxyMaxMemory = 0;
        i64 totalUserJobMaxMemory = 0;
        i64 tmpfsLimit = 0;
        i64 tmpfsUsage = 0;

        for (TForbidContextSwitchGuard guard; const auto& [id, job] : JobMap_) {
            if (job->GetState() != EJobState::Running || job->GetPhase() != EJobPhase::Running) {
                continue;
            }

            const auto& jobSpec = job->GetSpec();
            auto jobSpecExtId = TJobSpecExt::job_spec_ext;
            if (!jobSpec.HasExtension(jobSpecExtId)) {
                continue;
            }

            const auto& jobSpecExt = jobSpec.GetExtension(jobSpecExtId);
            if (!jobSpecExt.has_user_job_spec()) {
                continue;
            }

            for (const auto& tmpfsVolumeProto : jobSpecExt.user_job_spec().tmpfs_volumes()) {
                tmpfsLimit += tmpfsVolumeProto.size();
            }

            auto statisticsYson = job->GetStatistics();
            if (!statisticsYson) {
                continue;
            }

            if (auto jobProxyMaxMemory = TryGetInt64(statisticsYson.AsStringBuf(), jobProxyMaxMemorySensorName)) {
                totalJobProxyMaxMemory += *jobProxyMaxMemory;
            }

            if (auto tmpfsSizeSum = TryGetInt64(statisticsYson.AsStringBuf(), tmpfsSizeSensorName)) {
                tmpfsUsage += *tmpfsSizeSum;
            }

            if (auto userJobMaxMemory = TryGetInt64(statisticsYson.AsStringBuf(), userJobMaxMemorySensorName)) {
                totalUserJobMaxMemory += *userJobMaxMemory;
            }
        }

        TmpfsUsageGauge_.Update(tmpfsUsage);
        TmpfsLimitGauge_.Update(tmpfsLimit);

        JobProxyMaxMemoryGauge_.Update(totalJobProxyMaxMemory);
        UserJobMaxMemoryGauge_.Update(totalUserJobMaxMemory);
    }

    TCounter* GetJobFinalStateCounter(EJobState state)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto it = JobFinalStateCounters_.find(state);
        if (it == JobFinalStateCounters_.end()) {
            auto counter = Profiler_
                .WithTag("state", FormatEnum(state))
                .WithTag("origin", FormatEnum(EJobOrigin::Scheduler))
                .Counter("/job_final_state");

            it = JobFinalStateCounters_.emplace(state, counter).first;
        }

        return &it->second;
    }

    void ReplaceCpuWithVCpu(TNodeResources& resources) const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        resources.set_cpu(static_cast<double>(NVectorHdrf::TCpuResource(resources.cpu() * LastHeartbeatCpuToVCpuFactor_)));
        resources.clear_vcpu();
    }

    void OnResourceUsageOverdrafted(TResourceHolderPtr resourceHolder)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TForbidContextSwitchGuard guard;

        auto currentJob = DynamicPointerCast<TJob>(std::move(resourceHolder));
        if (!currentJob) {
            return;
        }

        auto jobId = currentJob->GetId();

        YT_LOG_DEBUG("Resource usage overdrafted on job resources updating (JobId: %v)", jobId);

        if (currentJob->ResourceUsageOverdrafted()) {
            // TODO(pogorelov): Maybe do not abort job at RunningExtraGpuCheckCommand phase?
            currentJob->Abort(TError(
                NExecNode::EErrorCode::ResourceOverdraft,
                "Resource usage overdrafted")
                // GetResourceUsage can be updated again, but it is pretty rare situation.
                << TErrorAttribute("resource_usage", FormatResources(currentJob->GetResourceUsage())));
        } else {
            bool foundJobToAbort = false;
            for (const auto& [id, job] : JobMap_) {
                if (job->GetState() == EJobState::Running && job->ResourceUsageOverdrafted()) {
                    job->Abort(TError(
                        NExecNode::EErrorCode::ResourceOverdraft,
                        "Some other job with guarantee overdrafted node resource usage")
                        << TErrorAttribute("resource_usage", FormatResources(job->GetResourceUsage()))
                        << TErrorAttribute("other_job_id", currentJob->GetId()));
                    foundJobToAbort = true;
                    break;
                }
            }

            if (!foundJobToAbort) {
                currentJob->Abort(TError(
                    NExecNode::EErrorCode::NodeResourceOvercommit,
                    "Resource usage on node overcommitted"));
            }
        }
    }

    void OnResourceReleased()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ScheduleStartJobs();
    }

    void DoPrepareAgentHeartbeatRequest(
        const TControllerAgentConnectorPool::TControllerAgentConnector::TReqHeartbeatPtr& request,
        const TAgentHeartbeatContextPtr& context)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        const auto& agentDescriptor = context->ControllerAgentConnector->GetDescriptor();

        ToProto(request->mutable_controller_agent_incarnation_id(), agentDescriptor.IncarnationId);

        auto getJobStatistics = [] (const TJobPtr& job) {
            auto statistics = job->GetStatistics();
            if (!statistics) {
                if (const auto& timeStatistics = job->GetTimeStatistics(); !timeStatistics.IsEmpty()) {
                    TStatistics timeStatisticsToSend;
                    timeStatisticsToSend.SetTimestamp(TInstant::Now());

                    timeStatistics.AddSamplesTo(&timeStatisticsToSend);

                    statistics = NYson::ConvertToYsonString(timeStatisticsToSend);
                }
            }

            return statistics;
        };

        auto addAllocationInfoToHeartbeatRequest = [&request] (TAllocationId allocationId) {
            ToProto(request->add_allocations()->mutable_allocation_id(), allocationId);
        };

        std::vector<TJobPtr> runningJobs;
        runningJobs.reserve(std::size(JobMap_));

        i64 finishedJobsStatisticsSize = 0;

        auto sendFinishedJob = [&request, &finishedJobsStatisticsSize, &getJobStatistics] (const TJobPtr& job) {
            YT_LOG_DEBUG(
                "Adding finished job info to heartbeat to agent (JobId: %v, JobState: %v, AgentDescriptor: %v, OperationId: %v)",
                job->GetId(),
                job->GetState(),
                job->GetControllerAgentDescriptor(),
                job->GetOperationId());

            auto* jobStatus = request->add_jobs();
            FillJobStatus(jobStatus, job);

            *jobStatus->mutable_result() = job->GetResult();

            job->ResetStatisticsLastSendTime();

            if (auto statistics = getJobStatistics(job)) {
                auto statisticsString = statistics.ToString();
                finishedJobsStatisticsSize += std::ssize(statisticsString);
                jobStatus->set_statistics(std::move(statisticsString));
            }
        };

        std::vector<TJobPtr> agentMismatchJobs;

        THashSet<TJobPtr> removedJobsToForcefullySend = context->JobsToForcefullySend;

        int confirmedJobCount = 0;

        for (TForbidContextSwitchGuard guard; const auto& [jobId, job] : JobMap_) {
            removedJobsToForcefullySend.erase(job);

            bool jobConfirmationRequested = context->JobsToForcefullySend.contains(job);

            const auto& controllerAgentDescriptor = job->GetControllerAgentDescriptor();

            if (!controllerAgentDescriptor) {
                YT_LOG_DEBUG(
                    "Skipping heartbeat for job since old agent incarnation is outdated and new incarnation is not received yet "
                    "(JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                if (jobConfirmationRequested) {
                    agentMismatchJobs.push_back(job);
                }
                continue;
            }

            if (controllerAgentDescriptor != agentDescriptor) {
                if (jobConfirmationRequested) {
                    agentMismatchJobs.push_back(job);
                }
                continue;
            }

            if (job->GetStored()) {
                if (!jobConfirmationRequested && !job->IsGrowingStale(context->JobStalenessDelay)) {
                    continue;
                }

                YT_LOG_DEBUG(
                    "Confirming job (JobId: %v, OperationId: %v, Stored: %v, State: %v, ControllerAgentDescriptor: %v)",
                    jobId,
                    job->GetOperationId(),
                    job->GetStored(),
                    job->GetState(),
                    agentDescriptor);
                ++confirmedJobCount;
            }

            switch (job->GetState()) {
                case EJobState::Waiting:
                case EJobState::Running:
                    runningJobs.push_back(job);
                    break;
                case EJobState::Aborted:
                case EJobState::Failed:
                case EJobState::Completed:
                    sendFinishedJob(job);

                    break;
                default:
                    break;
            }
        }

        if (!std::empty(agentMismatchJobs)) {
            constexpr int maxJobCountToLog = 5;

            std::vector<TJobId> nonSentJobs;
            nonSentJobs.reserve(maxJobCountToLog);
            for (const auto& job : agentMismatchJobs) {
                if (std::ssize(nonSentJobs) >= maxJobCountToLog) {
                    break;
                }
                nonSentJobs.push_back(job->GetId());
            }

            YT_LOG_DEBUG(
                "Can not report some jobs because of agent mismatch (TotalUnreportedJobCount: %v, JobSample: %v, ControllerAgentDescriptor: %v)",
                std::size(agentMismatchJobs),
                nonSentJobs,
                agentDescriptor);
        }

        if (!std::empty(removedJobsToForcefullySend)) {
            for (const auto& job : removedJobsToForcefullySend) {
                YT_LOG_DEBUG(
                    "Forcefully adding removed job info to heartbeat to agent (JobId: %v, JobState: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetState(),
                    job->GetOperationId());

                sendFinishedJob(job);
            }
        }

        // In case of statistics size throttling we want to report older jobs first to ensure
        // that all jobs will sent statistics eventually.
        std::sort(
            runningJobs.begin(),
            runningJobs.end(),
            [] (const auto& lhs, const auto& rhs) noexcept {
                return lhs->GetStatisticsLastSendTime() < rhs->GetStatisticsLastSendTime();
            });

        const auto now = TInstant::Now();
        int consideredRunningJobCount = 0;
        int reportedRunningJobCount = 0;
        i64 runningJobsStatisticsSize = 0;
        for (const auto& job : runningJobs) {
            YT_LOG_DEBUG(
                "Adding running job info to heartbeat to agent (JobId: %v, AgentDescriptor: %v, OperationId: %v)",
                job->GetId(),
                job->GetControllerAgentDescriptor(),
                job->GetOperationId());

            addAllocationInfoToHeartbeatRequest(job->GetAllocationId());

            auto* jobStatus = request->add_jobs();

            FillJobStatus(jobStatus, job);

            if (now - job->GetStatisticsLastSendTime() < context->RunningJobStatisticsSendingBackoff) {
                continue;
            }

            ++consideredRunningJobCount;

            if (auto statistics = getJobStatistics(job)) {
                auto statisticsString = statistics.ToString();
                if (context->StatisticsThrottler->TryAcquire(statisticsString.size())) {
                    ++reportedRunningJobCount;

                    runningJobsStatisticsSize += statisticsString.size();
                    job->ResetStatisticsLastSendTime();
                    jobStatus->set_statistics(std::move(statisticsString));
                }
            }
        }

        for (auto [allocationId, operationId] : context->AllocationIdsWaitingForSpec) {
            addAllocationInfoToHeartbeatRequest(allocationId);
        }

        request->set_confirmed_job_count(confirmedJobCount);
        if (!std::empty(context->UnconfirmedJobIds)) {
            ToProto(request->mutable_unconfirmed_job_ids(), context->UnconfirmedJobIds);
        }

        YT_LOG_DEBUG(
            "Job statistics for agent prepared (RunningJobsStatisticsSize: %v, FinishedJobsStatisticsSize: %v, "
            "RunningJobCount: %v, SkippedJobCountDueToBackoff: %v, SkippedJobCountDueToStatisticsSizeThrottling: %v, "
            "AgentDescriptor: %v)",
            runningJobsStatisticsSize,
            finishedJobsStatisticsSize,
            std::size(runningJobs),
            std::ssize(runningJobs) - consideredRunningJobCount,
            consideredRunningJobCount - reportedRunningJobCount,
            agentDescriptor);
    }

    void DoProcessAgentHeartbeatResponse(
        const TControllerAgentConnectorPool::TControllerAgentConnector::TRspHeartbeatPtr& response,
        const TAgentHeartbeatContextPtr& context)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        const auto& agentDescriptor = context->ControllerAgentConnector->GetDescriptor();

        for (const auto& protoJobToStore : response->jobs_to_store()) {
            auto jobToStore = FromProto<NControllerAgent::TJobToStore>(protoJobToStore);

            if (auto job = FindJob(jobToStore.JobId)) {
                YT_LOG_DEBUG(
                    "Agent requested to store job (JobId: %v, AgentDescriptor: %v)",
                    jobToStore.JobId,
                    agentDescriptor);
                YT_VERIFY(job->IsFinished());
                job->SetStored();
            } else {
                YT_LOG_WARNING(
                    "Agent requested to store a non-existent job (JobId: %v, AgentDescriptor: %v)",
                    jobToStore.JobId,
                    agentDescriptor);
            }
        }

        {
            std::vector<TJobId> jobIdsToConfirm;
            jobIdsToConfirm.reserve(response->jobs_to_confirm_size());
            for (const auto& protoJobToConfirm : response->jobs_to_confirm()) {
                auto jobToConfirm = FromProto<NControllerAgent::TJobToConfirm>(protoJobToConfirm);

                YT_LOG_DEBUG("Agent requested to confirm job (JobId: %v, AgentDescriptor: %v)", jobToConfirm.JobId, agentDescriptor);

                if (auto job = FindJob(jobToConfirm.JobId)) {
                    job->UpdateControllerAgentDescriptor(agentDescriptor);
                }

                jobIdsToConfirm.push_back(jobToConfirm.JobId);
            }

            ConfirmJobs(jobIdsToConfirm, context->ControllerAgentConnector);
        }

        for (const auto& protoJobToInterrupt : response->jobs_to_interrupt()) {
            auto jobId = FromProto<TJobId>(protoJobToInterrupt.job_id());
            auto interruptionReason = CheckedEnumCast<EInterruptReason>(protoJobToInterrupt.reason());
            auto timeout = FromProto<TDuration>(protoJobToInterrupt.timeout());

            if (auto job = FindJob(jobId)) {
                YT_LOG_DEBUG(
                    "Agent requested to interrupt job (JobId: %v, InterruptionReason: %v, AgentDescriptor: %v)",
                    jobId,
                    interruptionReason,
                    agentDescriptor);

                job->Interrupt(
                    timeout,
                    interruptionReason,
                    /*preemptionReason*/ std::nullopt,
                    /*preemptedFor*/ std::nullopt);
            } else {
                YT_LOG_WARNING(
                    "Agent requested to interrupt a non-existent job (JobId: %v, AgentDescriptor: %v)",
                    jobId,
                    agentDescriptor);
            }
        }

        for (const auto& protoJobToFail : response->jobs_to_fail()) {
            auto jobId = FromProto<TJobId>(protoJobToFail.job_id());

            if (auto job = FindJob(jobId)) {
                YT_LOG_DEBUG(
                    "Agent requested to fail job (JobId: %v)",
                    jobId);

                job->Fail(std::nullopt);
            } else {
                YT_LOG_WARNING(
                    "Agent requested to fail a non-existent job (JobId: %v)",
                    jobId);
            }
        }

        for (const auto& protoJobToAbort : response->jobs_to_abort()) {
            auto jobToAbort = FromProto<NControllerAgent::TJobToAbort>(protoJobToAbort);

            if (auto job = FindJob(jobToAbort.JobId)) {
                YT_LOG_DEBUG(
                    "Agent requested to abort job (JobId: %v, AgentDescriptor: %v)",
                    jobToAbort.JobId,
                    agentDescriptor);

                AbortJob(job, jobToAbort.AbortReason, jobToAbort.Graceful);
            } else {
                YT_LOG_WARNING(
                    "Agent requested to abort a non-existent job (JobId: %v, AbortReason: %v, AgentDescriptor: %v)",
                    jobToAbort.JobId,
                    jobToAbort.AbortReason,
                    agentDescriptor);
            }
        }

        for (const auto& protoJobToRemove : response->jobs_to_remove()) {
            auto jobToRemove = FromProto<TJobToRelease>(protoJobToRemove);
            auto jobId = jobToRemove.JobId;

            if (auto job = FindJob(jobId)) {
                YT_LOG_DEBUG(
                    "Agent requested to remove job (JobId: %v, AgentDescriptor: %v, ReleaseFlags: %v)",
                    jobId,
                    agentDescriptor,
                    jobToRemove.ReleaseFlags);

                if (job->IsFinished()) {
                    RemoveJob(job, jobToRemove.ReleaseFlags);
                } else {
                    YT_LOG_DEBUG("Requested to remove running job; aborting job (JobId: %v, JobState: %v)", jobId, job->GetState());
                    AbortJob(job, EAbortReason::Other);
                    RemoveJob(job, jobToRemove.ReleaseFlags);
                }
            } else {
                YT_LOG_WARNING(
                    "Agent requested to remove a non-existent job (JobId: %v, AgentDescriptor: %v)",
                    jobId,
                    agentDescriptor);
            }
        }

        for (auto protoOperationId : response->unknown_operation_ids()) {
            auto operationId = NYT::FromProto<TOperationId>(protoOperationId);

            YT_LOG_DEBUG(
                "Operation is not handled by agent, reset it for jobs (OperationId: %v, AgentDescriptor: %v)",
                operationId,
                agentDescriptor);

            // TODO(pogorelov): Request operationIds for such operations immediately.

            UpdateOperationControllerAgent(operationId, {});
        }
    }

    void DoPrepareSchedulerHeartbeatRequest(
        const TSchedulerConnector::TReqHeartbeatPtr& request,
        const TSchedulerHeartbeatContextPtr& context)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Preparing scheduler heartbeat request");

        const auto& controllerAgentConnectorPool = Bootstrap_->GetExecNodeBootstrap()->GetControllerAgentConnectorPool();

        *request->mutable_resource_limits() = ToNodeResources(JobResourceManager_->GetResourceLimits());
        *request->mutable_resource_usage() = ToNodeResources(JobResourceManager_->GetResourceUsage(/*includeWaiting*/ true));

        *request->mutable_disk_resources() = JobResourceManager_->GetDiskResources();

        for (auto incarnationId : controllerAgentConnectorPool->GetRegisteredAgentIncarnationIds()) {
            auto* agentDescriptorProto = request->add_registered_controller_agents();
            ToProto(agentDescriptorProto->mutable_incarnation_id(), incarnationId);
        }

        const auto& jobReporter = Bootstrap_->GetExecNodeBootstrap()->GetJobReporter();
        request->set_job_reporter_write_failures_count(jobReporter->ExtractWriteFailuresCount());
        request->set_job_reporter_queue_is_too_large(jobReporter->GetQueueIsTooLarge());

        // Only for scheduler `cpu` stores `vcpu` actually.
        // In all resource limits and usages we send and get back vcpu instead of cpu.
        LastHeartbeatCpuToVCpuFactor_ = JobResourceManager_->GetCpuToVCpuFactor();
        ReplaceCpuWithVCpu(*request->mutable_resource_limits());
        ReplaceCpuWithVCpu(*request->mutable_resource_usage());

        auto* execNodeBootstrap = Bootstrap_->GetExecNodeBootstrap();
        auto slotManager = execNodeBootstrap->GetSlotManager();

        const bool requestOperationInfo = OperationInfoRequestBackoffStrategy_
            .RecordInvocationIfOverBackoff();

        THashSet<TOperationId> operationIdsToRequestInfo;

        for (TForbidContextSwitchGuard guard; const auto& [id, job] : JobMap_) {
            if (requestOperationInfo && !job->GetControllerAgentDescriptor()) {
                operationIdsToRequestInfo.insert(job->GetOperationId());
            }

            if (job->IsFinished()) {
                continue;
            }

            YT_LOG_DEBUG(
                "Adding allocation info to heartbeat to scheduler (AllocationId: %v, AllocationState: %v, OperationId: %v)",
                job->GetAllocationId(),
                job->GetState(),
                job->GetOperationId());

            auto* allocationStatus = request->add_allocations();
            FillJobStatus(allocationStatus, job);
            {
                auto& resourceUsage = *allocationStatus->mutable_resource_usage();
                resourceUsage = ToNodeResources(job->GetResourceUsage());
                ReplaceCpuWithVCpu(resourceUsage);
            }
        }

        for (auto [allocationId, operationId] : controllerAgentConnectorPool->GetAllocationIdsWaitingForSpec()) {
            auto* allocationStatus = request->add_allocations();

            ToProto(allocationStatus->mutable_allocation_id(), allocationId);
            ToProto(allocationStatus->mutable_operation_id(), operationId);

            allocationStatus->set_state(ToProto<int>(EAllocationState::Waiting));
        }

        for (const auto& job : context->JobsToForcefullySend) {
            YT_LOG_DEBUG(
                "Forcefully adding allocation to heartbeat to scheduler (JobId: %v, JobState: %v, OperationId: %v)",
                job->GetId(),
                job->GetState(),
                job->GetOperationId());

            YT_VERIFY(job->IsFinished());

            auto* allocationStatus = request->add_allocations();
            FillJobStatus(allocationStatus, job);
            ToProto(allocationStatus->mutable_result()->mutable_error(), job->GetJobError());
        }

        for (const auto& [allocationId, info] : context->FailedAllocations) {
            auto* jobStatus = request->add_allocations();
            ToProto(jobStatus->mutable_allocation_id(), allocationId);

            ToProto(jobStatus->mutable_operation_id(), info.OperationId);
            jobStatus->set_state(static_cast<int>(JobStateToAllocationState(EJobState::Aborted)));

            TAllocationResult jobResult;
            auto error = TError("Failed to get job spec")
                << TErrorAttribute("abort_reason", EAbortReason::GetSpecFailed)
                << info.Error;
            ToProto(jobResult.mutable_error(), error);
            *jobStatus->mutable_result() = jobResult;
        }

        if (requestOperationInfo) {
            YT_LOG_DEBUG(
                "Adding operation info requests for stored jobs (Count: %v)",
                std::size(operationIdsToRequestInfo));

            ToProto(request->mutable_operations_ids_to_request_info(), operationIdsToRequestInfo);
        }

        YT_LOG_DEBUG("Scheduler heartbeat request prepared");
    }

    void DoProcessSchedulerHeartbeatResponse(
        const TSchedulerConnector::TRspHeartbeatPtr& response,
        const TSchedulerHeartbeatContextPtr& /*context*/)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (response->registered_controller_agents_sent()) {
            THashSet<TControllerAgentDescriptor> receivedRegisteredAgents;
            receivedRegisteredAgents.reserve(response->registered_controller_agents_size());
            for (const auto& protoAgentDescriptor : response->registered_controller_agents()) {
                auto descriptorOrError = TryParseControllerAgentDescriptor(
                    protoAgentDescriptor,
                    Bootstrap_->GetLocalNetworks());
                YT_LOG_FATAL_IF(
                    !descriptorOrError.IsOK(),
                    descriptorOrError,
                    "Failed to parse registered controller agent descriptor");

                EmplaceOrCrash(receivedRegisteredAgents, std::move(descriptorOrError.Value()));
            }

            const auto& controllerAgentConnectorPool = Bootstrap_->GetExecNodeBootstrap()->GetControllerAgentConnectorPool();
            controllerAgentConnectorPool->OnRegisteredAgentSetReceived(std::move(receivedRegisteredAgents));
        }

        for (const auto& protoAllocationToAbort : response->allocations_to_abort()) {
            auto allocationToAbort = ParseAllocationToAbort(protoAllocationToAbort);

            if (auto job = FindJob(FromAllocationId(allocationToAbort.AllocationId))) {
                YT_LOG_WARNING(
                    "Scheduler requested to abort allocation (AllocationId: %v)",
                    allocationToAbort.AllocationId);

                AbortAllocation(job, allocationToAbort);
            } else {
                YT_LOG_WARNING(
                    "Scheduler requested to abort a non-existent allocation (AllocationId: %v, AbortReason: %v)",
                    allocationToAbort.AllocationId,
                    allocationToAbort.AbortReason);
            }
        }

        for (const auto& allocationToInterrupt : response->allocations_to_preempt()) {
            auto timeout = FromProto<TDuration>(allocationToInterrupt.timeout());
            auto jobId = FromAllocationId(FromProto<TAllocationId>(allocationToInterrupt.allocation_id()));

            if (auto job = FindJob(jobId)) {
                YT_LOG_WARNING(
                    "Scheduler requested to interrupt job (JobId: %v)",
                    jobId);

                std::optional<TString> preemptionReason;
                if (allocationToInterrupt.has_preemption_reason()) {
                    preemptionReason = allocationToInterrupt.preemption_reason();
                }

                std::optional<NScheduler::TPreemptedFor> preemptedFor;
                if (allocationToInterrupt.has_preempted_for()) {
                    preemptedFor = FromProto<NScheduler::TPreemptedFor>(allocationToInterrupt.preempted_for());
                }

                job->Interrupt(
                    timeout,
                    /*interruptionReason*/ EInterruptReason::Preemption,
                    preemptionReason,
                    preemptedFor);
            } else {
                YT_LOG_WARNING(
                    "Scheduler requested to interrupt a non-existing job (JobId: %v)",
                    jobId);
            }
        }

        for (const auto& protoOperationInfo : response->operation_infos()) {
            auto operationId = FromProto<TOperationId>(protoOperationInfo.operation_id());
            if (!protoOperationInfo.running()) {
                HandleJobsOfNonRunningOperation(operationId);
                continue;
            }

            if (!protoOperationInfo.has_controller_agent_descriptor()) {
                continue;
            }

            auto incarnationId = FromProto<NScheduler::TIncarnationId>(
                protoOperationInfo.controller_agent_descriptor().incarnation_id());

            const auto& controllerAgentConnectorPool = Bootstrap_->GetExecNodeBootstrap()->GetControllerAgentConnectorPool();
            auto descriptor = controllerAgentConnectorPool->GetDescriptorByIncarnationId(incarnationId);
            YT_VERIFY(descriptor);
            UpdateOperationControllerAgent(operationId, std::move(*descriptor));
        }

        {
            auto minSpareResources = FromProto<NScheduler::TJobResources>(response->min_spare_resources());

            const auto& schedulerConnector = Bootstrap_->GetExecNodeBootstrap()->GetSchedulerConnector();
            schedulerConnector->SetMinSpareResources(minSpareResources);
        }

        YT_VERIFY(response->Attachments().empty());

        std::vector<TAllocationStartInfo> allocationStartInfos;
        allocationStartInfos.reserve(response->allocations_to_start_size());
        for (const auto& startInfo : response->allocations_to_start()) {
            allocationStartInfos.push_back(startInfo);

            // We get vcpu here. Need to replace it with real cpu back.
            auto& resourceLimits = *allocationStartInfos.back().mutable_resource_limits();
            resourceLimits.set_cpu(static_cast<double>(NVectorHdrf::TCpuResource(resourceLimits.cpu() / LastHeartbeatCpuToVCpuFactor_)));
        }

        SettleAndStartJobs(std::move(allocationStartInfos));
    }

    void StartWaitingJobs()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto resourceAcquiringContext = JobResourceManager_->GetResourceAcquiringContext();

        for (TForbidContextSwitchGuard guard; const auto& [id, job] : JobMap_) {
            if (job->GetState() != EJobState::Waiting) {
                continue;
            }

            auto jobId = job->GetId();
            YT_LOG_DEBUG("Trying to start job (JobId: %v)", jobId);

            try {
                if (!resourceAcquiringContext.TryAcquireResourcesFor(StaticPointerCast<TResourceHolder>(job))) {
                    YT_LOG_DEBUG("Job was not started (JobId: %v)", jobId);
                } else {
                    YT_LOG_DEBUG("Job started (JobId: %v)", jobId);
                }
            } catch (const std::exception& ex) {
                job->Abort(TError("Failed to acquire resources for job")
                    << ex);
            }
        }

        StartJobsScheduled_ = false;
    }

    void CreateJob(
        TJobId jobId,
        TOperationId operationId,
        const NClusterNode::TJobResources& resourceLimits,
        const NClusterNode::TJobResourceAttributes& resourceAttributes,
        TJobSpec&& jobSpec,
        const TControllerAgentDescriptor& controllerAgentDescriptor)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobType = CheckedEnumCast<EJobType>(jobSpec.type());
        YT_LOG_FATAL_IF(
            jobType >= EJobType::SchedulerUnknown,
            "Trying to create job with unexpected type (JobId: %v, JobType: %v)",
            jobId,
            jobType);

        auto jobSpecExtId = TJobSpecExt::job_spec_ext;
        auto waitingJobTimeout = GetDynamicConfig()->WaitingJobsTimeout;

        YT_VERIFY(jobSpec.HasExtension(jobSpecExtId));
        const auto& jobSpecExt = jobSpec.GetExtension(jobSpecExtId);
        if (jobSpecExt.has_waiting_job_timeout()) {
            waitingJobTimeout = FromProto<TDuration>(jobSpecExt.waiting_job_timeout());
        }

        TJobPtr job;
        try {
            job = NExecNode::CreateJob(
                jobId,
                operationId,
                resourceLimits,
                resourceAttributes,
                std::move(jobSpec),
                controllerAgentDescriptor,
                Bootstrap_->GetExecNodeBootstrap(),
                GetDynamicConfig()->JobCommon);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(
                error,
                "Scheduler job was not created (JobId: %v, OperationId: %v)",
                jobId,
                operationId);

            AllocationFailed_.Fire(
                AllocationIdFromJobId(jobId),
                operationId,
                controllerAgentDescriptor,
                error);

            return;
        } catch (...) {
            YT_LOG_FATAL(
                "Unexpected failure during job creation (JobId: %v, OperationId: %v)",
                jobId,
                operationId);
        }

        YT_LOG_INFO("Scheduler job created (JobId: %v, OperationId: %v, JobType: %v)",
            jobId,
            operationId,
            jobType);

        RegisterJob(jobId, job, waitingJobTimeout);
    }

    void RegisterJob(TJobId jobId, const TJobPtr& job, TDuration waitingJobTimeout)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TForbidContextSwitchGuard guard;

        {
            auto guard = WriterGuard(JobMapLock_);
            EmplaceOrCrash(JobMap_, jobId, job);
            EmplaceOrCrash(OperationIdToJobs_[job->GetOperationId()], job);
        }

        JobRegistered_.Fire(job);

        job->SubscribeJobPrepared(
            BIND_NO_PROPAGATE(&TJobController::OnJobPrepared, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()));

        job->SubscribeJobFinished(
            BIND_NO_PROPAGATE(&TJobController::OnJobFinished, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()));

        job->GetCleanupFinishedEvent()
            .Subscribe(BIND_NO_PROPAGATE([=, this_ = MakeWeak(this), job_ = MakeWeak(job)] (const TError& result) {
                YT_LOG_FATAL_IF(!result.IsOK(), result, "Cleanup finish failed");

                auto strongThis = this_.Lock();
                if (!strongThis) {
                    return;
                }

                strongThis->OnJobCleanupFinished(job_);
            })
                .Via(Bootstrap_->GetJobInvoker()));

        if (AreJobsDisabled()) {
            YT_LOG_INFO(
                "Aborting job instead of starting since jobs disabled on node (JobId: %v, OperationId: %v)",
                jobId,
                job->GetOperationId());
            job->Abort(MakeJobsDisabledError());
            return;
        }

        ScheduleStartJobs();

        TDelayedExecutor::Submit(
            BIND(&TJobController::OnWaitingJobTimeout, MakeWeak(this), MakeWeak(job), waitingJobTimeout),
            waitingJobTimeout,
            Bootstrap_->GetJobInvoker());
    }

    void OnJobCleanupFinished(const TWeakPtr<TJob>& weakJob)
    {
        auto job = weakJob.Lock();

        if (!job) {
            return;
        }

        YT_VERIFY(job->GetPhase() == EJobPhase::Finished);
        if (JobsWaitingForCleanup_.erase(job)) {
            YT_LOG_DEBUG(
                "Job cleanup finished (JobId: %v)",
                job->GetId());
        }
    }

    void UnregisterJob(const TJobPtr& job)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto operationId = job->GetOperationId();

        auto guard = WriterGuard(JobMapLock_);

        EraseOrCrash(JobMap_, job->GetId());

        auto& jobIds = GetOrCrash(OperationIdToJobs_, operationId);
        EraseOrCrash(jobIds, job);
        if (std::empty(jobIds)) {
            EraseOrCrash(OperationIdToJobs_, operationId);
        }
    }

    void OnWaitingJobTimeout(const TWeakPtr<TJob>& weakJob, TDuration waitingJobTimeout)
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

    void AbortAllocation(const TJobPtr& job, const NScheduler::TAllocationToAbort& abortAttributes)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Aborting allocation (AllocationId: %v, AbortReason: %v)",
            job->GetId(),
            abortAttributes.AbortReason);

        auto error = TError(NExecNode::EErrorCode::AbortByScheduler, "Job aborted by scheduler")
            << TErrorAttribute("abort_reason", abortAttributes.AbortReason.value_or(EAbortReason::Unknown));

        DoAbortJob(job, std::move(error));
    }

    void AbortJob(const TJobPtr& job, EAbortReason abortReason, bool graceful = false)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Aborting job (JobId: %v, AbortReason: %v)",
            job->GetId(),
            abortReason);

        auto error = TError(NExecNode::EErrorCode::AbortByControllerAgent, "Job aborted by controller agent")
            << TErrorAttribute("abort_reason", abortReason)
            << TErrorAttribute("graceful_abort", graceful);

        DoAbortJob(job, std::move(error), graceful);
    }

    void DoAbortJob(const TJobPtr& job, TError abortionError, bool graceful = false)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        job->Abort(std::move(abortionError), graceful);
    }

    void RemoveJob(
        const TJobPtr& job,
        const NControllerAgent::TReleaseJobFlags& releaseFlags)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        YT_VERIFY(job->GetPhase() >= EJobPhase::FinalizingJobProxy);

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

        if (job->GetPhase() != EJobPhase::Finished) {
            YT_LOG_DEBUG(
                "Job waiting for cleanup (JobId: %v, JobPhase: %v)",
                jobId,
                job->GetPhase());

            EmplaceOrCrash(JobsWaitingForCleanup_, job);
        }

        UnregisterJob(job);

        YT_LOG_INFO("Job removed (JobId: %v, Save: %v)", job->GetId(), shouldSave);
    }

    TDuration GetMemoryOverdraftTimeout() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return GetDynamicConfig()->MemoryOverdraftTimeout;
    }

    TDuration GetCpuOverdraftTimeout() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return GetDynamicConfig()->CpuOverdraftTimeout;
    }

    TDuration GetRecentlyRemovedJobsStoreTimeout() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return GetDynamicConfig()->RecentlyRemovedJobsStoreTimeout;
    }

    void CleanRecentlyRemovedJobs()
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

    void OnReservedMemoryOvercommited(i64 mappedMemory)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto usage = JobResourceManager_->GetResourceUsage(false);
        const auto limits = JobResourceManager_->GetResourceLimits();
        auto schedulerJobs = GetRunningJobsSortedByStartTime();

        while (usage.UserMemory + mappedMemory > limits.UserMemory &&
            !schedulerJobs.empty())
        {
            usage -= schedulerJobs.back()->GetResourceUsage();
            schedulerJobs.back()->Abort(TError(
                NExecNode::EErrorCode::ResourceOverdraft,
                "Mapped memory usage overdraft"));
            schedulerJobs.pop_back();
        }
    }

    void AdjustResources()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto usage = JobResourceManager_->GetResourceUsage(/*includeWaiting*/ false);
        auto limits = JobResourceManager_->GetResourceLimits();

        bool preemptMemoryOverdraft = false;
        bool preemptCpuOverdraft = false;
        if (usage.UserMemory > limits.UserMemory) {
            if (UserMemoryOverdraftInstant_) {
                preemptMemoryOverdraft = *UserMemoryOverdraftInstant_ + GetMemoryOverdraftTimeout() <
                    TInstant::Now();
            } else {
                UserMemoryOverdraftInstant_ = TInstant::Now();
            }
        } else {
            UserMemoryOverdraftInstant_ = std::nullopt;
        }

        if (usage.Cpu > limits.Cpu) {
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
            auto jobs = GetRunningJobsSortedByStartTime();

            while ((preemptCpuOverdraft && usage.Cpu > limits.Cpu) ||
                (preemptMemoryOverdraft && usage.UserMemory > limits.UserMemory))
            {
                if (jobs.empty()) {
                    break;
                }

                usage -= jobs.back()->GetResourceUsage();
                jobs.back()->Abort(TError(
                    NExecNode::EErrorCode::ResourceOverdraft,
                    "Resource usage overdraft adjustment"));
                jobs.pop_back();
            }

            UserMemoryOverdraftInstant_ = std::nullopt;
            CpuOverdraftInstant_ = std::nullopt;
        }
    }

    std::vector<TJobPtr> GetRunningJobsSortedByStartTime() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        std::vector<TJobPtr> schedulerJobs;
        for (TForbidContextSwitchGuard guard; const auto& [id, job] : JobMap_) {
            if (job->GetState() == EJobState::Running) {
                schedulerJobs.push_back(job);
            }
        }

        std::sort(schedulerJobs.begin(), schedulerJobs.end(), [] (const TJobPtr& lhs, const TJobPtr& rhs) {
            return lhs->GetStartTime() < rhs->GetStartTime();
        });

        return schedulerJobs;
    }

    void InterruptAllJobs(TError error)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        for (const auto& job : GetJobs()) {
            const auto& Logger = job->GetLogger();
            try {
                YT_LOG_DEBUG(error, "Trying to interrupt job");
                job->Interrupt(
                    GetDynamicConfig()->DisabledJobsInterruptionTimeout,
                    EInterruptReason::JobsDisabledOnNode,
                    /*preemptionReason*/ {},
                    /*preemptedFor*/ {});
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to interrupt job");
            }
        }
    }

    void OnJobPrepared(const TJobPtr& job)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(job->IsStarted());

        const auto& chunkCacheStatistics = job->GetChunkCacheStatistics();
        CacheHitArtifactsSizeCounter_.Increment(chunkCacheStatistics.CacheHitArtifactsSize);
        CacheMissArtifactsSizeCounter_.Increment(chunkCacheStatistics.CacheMissArtifactsSize);
        CacheBypassedArtifactsSizeCounter_.Increment(chunkCacheStatistics.CacheBypassedArtifactsSize);
    }

    void OnJobFinished(const TJobPtr& job)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (!job->IsStarted()) {
            return;
        }

        auto* jobFinalStateCounter = GetJobFinalStateCounter(job->GetState());
        jobFinalStateCounter->Increment();

        JobFinished_.Fire(job);
    }

    void UpdateJobProxyBuildInfo()
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

    void HandleJobsOfNonRunningOperation(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Removing jobs of operation (OperationId: %v)", operationId);

        auto operationJobsIt = OperationIdToJobs_.find(operationId);
        if (operationJobsIt == std::end(OperationIdToJobs_)) {
            YT_LOG_DEBUG("There are no operation jobs on node (OperationId: %v)", operationId);
            return;
        }

        std::vector operationJobs(std::begin(operationJobsIt->second), std::end(operationJobsIt->second));
        for (auto job : operationJobs) {
            if (job->IsFinished()) {
                auto removeJob = [
                    jobId = job->GetId(),
                    weakJob = MakeWeak(job),
                    this_ = MakeStrong(this),
                    this
                ]
                {
                    VERIFY_THREAD_AFFINITY(JobThread);

                    if (auto job = weakJob.Lock(); job && !IsJobRemoved(job)) {
                        RemoveJob(job, NControllerAgent::TReleaseJobFlags{});
                    } else {
                        YT_LOG_DEBUG(
                            "Delayed remove skipped since job is already removed (JobId: %v)",
                            jobId);
                    }
                };

                auto removalDelay = GetDynamicConfig()->UnknownOperationJobsRemovalDelay;

                YT_LOG_DEBUG(
                    "Schedule delayed removal of job (JobId: %v, Delay: %v)",
                    job->GetId(),
                    removalDelay);

                TDelayedExecutor::Submit(
                    BIND(removeJob),
                    removalDelay,
                    Bootstrap_->GetJobInvoker());
            } else {
                auto error = TError("Operation %v is not running", operationId)
                    << TErrorAttribute("abort_reason", EAbortReason::OperationFinished);
                job->Abort(std::move(error));
            }
        }
    }

    bool IsJobRemoved(const TJobPtr& job) const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return !JobMap_.contains(job->GetId());
    }

    void UpdateOperationControllerAgent(
        TOperationId operationId,
        TControllerAgentDescriptor controllerAgentDescriptor)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto operationJobsIt = OperationIdToJobs_.find(operationId);
        if (operationJobsIt == std::end(OperationIdToJobs_)) {
            return;
        }

        YT_LOG_DEBUG(
            "Updating controller agent for jobs (OperationId: %v, ControllerAgentAddress: %v, ControllerAgentIncarnationId: %v)",
            operationId,
            controllerAgentDescriptor.Address,
            controllerAgentDescriptor.IncarnationId);

        auto& operationJobs = operationJobsIt->second;
        for (const auto& job : operationJobs) {
            job->UpdateControllerAgentDescriptor(controllerAgentDescriptor);
        }
    }

    void ConfirmJobs(const std::vector<TJobId>& jobIds, TControllerAgentConnectorPtr initiator)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        std::vector<TJobId> unconfirmedJobIds;
        for (auto jobId : jobIds) {
            YT_LOG_DEBUG("Requested to confirm job (JobId: %v)", jobId);

            if (auto job = FindJob(jobId)) {
                if (auto controllerAgentConnector = job->GetControllerAgentConnector()) {
                    controllerAgentConnector->EnqueueFinishedJob(job);
                } else {
                    YT_LOG_DEBUG(
                        "Controller agent for job is not received yet; "
                        "finished job info will be reported later (JobId: %v, JobControllerAgentDescriptor: %v)",
                        jobId,
                        job->GetControllerAgentDescriptor());
                }
            } else {
                YT_LOG_DEBUG("Job unconfirmed (JobId: %v)", jobId);

                unconfirmedJobIds.push_back(jobId);
            }
        }

        initiator->AddUnconfirmedJobIds(std::move(unconfirmedJobIds));

        Bootstrap_->GetExecNodeBootstrap()->GetControllerAgentConnectorPool()->SendOutOfBandHeartbeatsIfNeeded();
    }

    static void BuildJobsInfo(const std::vector<TBriefJobInfo>& jobsInfo, TFluentAny fluent)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        fluent.DoMapFor(
            jobsInfo,
            [&] (TFluentMap fluent, const TBriefJobInfo& jobInfo) {
                jobInfo.BuildOrchid(fluent);
            });
    }

    static void BuildJobsWaitingForCleanupInfo(
        const std::vector<std::pair<TJobId, EJobPhase>>& jobsWaitingForCleanupInfo,
        TFluentAny fluent)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        fluent.DoMapFor(
            jobsWaitingForCleanupInfo,
            [] (TFluentMap fluent, const auto& jobInfo) {
                auto [id, phase] = jobInfo;

                fluent
                    .Item(ToString(id)).BeginMap()
                        .Item("phase").Value(phase)
                    .EndMap();
            });
    }

    static void BuildJobProxyBuildInfo(const TErrorOr<TBuildInfoPtr>& buildInfo, TFluentAny fluent)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (buildInfo.IsOK()) {
            fluent.Value(buildInfo.Value());
        } else {
            fluent
                .BeginMap()
                    .Item("error").Value(static_cast<TError>(buildInfo))
                .EndMap();
        }
    }

    auto DoGetStateSnapshot() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);
        TForbidContextSwitchGuard guard;

        std::vector<TBriefJobInfo> jobInfo;
        jobInfo.reserve(JobMap_.size());

        for (const auto& [id, job] : JobMap_) {
            jobInfo.emplace_back(job->GetBriefInfo());
        }

        std::vector<std::pair<TJobId, EJobPhase>> jobsWaitingForCleanupInfo;

        jobsWaitingForCleanupInfo.reserve(JobsWaitingForCleanup_.size());

        for (TForbidContextSwitchGuard guard; const auto& job : JobsWaitingForCleanup_) {
            jobsWaitingForCleanupInfo.emplace_back(job->GetId(), job->GetPhase());
        }

        return std::tuple(
            std::move(jobInfo),
            std::move(jobsWaitingForCleanupInfo),
            CachedJobProxyBuildInfo_.Load());
    }

    auto GetStateSnapshot() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshotOrError = WaitFor(BIND(
            &TJobController::DoGetStateSnapshot,
            MakeStrong(this))
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run());

        YT_LOG_FATAL_UNLESS(
            snapshotOrError.IsOK(),
            snapshotOrError,
            "Unexpected failure while making exec node job controller info snapshot"
        );

        return std::move(snapshotOrError.Value());
    }

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto [
            jobsInfo,
            jobsWaitingForCleanupInfo,
            buildInfo
        ] = GetStateSnapshot();

        BuildYsonFluently(consumer).BeginMap()
            .Item("active_job_count").Value(std::ssize(jobsInfo))
            .Item("active_jobs").Do(std::bind(
                &TJobController::BuildJobsInfo,
                jobsInfo,
                std::placeholders::_1))
            .Item("jobs_waiting_for_cleanup").Do(std::bind(
                &TJobController::BuildJobsWaitingForCleanupInfo,
                jobsWaitingForCleanupInfo,
                std::placeholders::_1))
            .Item("job_proxy_build").Do(std::bind(
                &TJobController::BuildJobProxyBuildInfo,
                buildInfo,
                std::placeholders::_1))
        .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobControllerPtr CreateJobController(NClusterNode::IBootstrapBase* bootstrap)
{
    return New<TJobController>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
