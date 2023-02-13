#include "job_controller.h"

#include "bootstrap.h"
#include "job.h"
#include "private.h"
#include "scheduler_connector.h"
#include "slot_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/job_controller.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/job_spec_service_proxy.h>
#include <yt/yt/server/lib/job_agent/config.h>
#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/job_tracker_client/helpers.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/library/process/process.h>
#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

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
using namespace NProfiling;
using namespace NScheduler;
using namespace NControllerAgent;

using NControllerAgent::NProto::TJobSpec;
using NControllerAgent::NProto::TJobResult;
using NNodeTrackerClient::NProto::TNodeResources;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

// COMPAT(pogorelov)

const auto& GetAllocationsToAbort(const IJobController::TRspSchedulerHeartbeatPtr& response)
{
    return response->allocations_to_abort();
}

const auto& GetAllocationsToAbort(const IJobController::TRspOldSchedulerHeartbeatPtr& response)
{
    return response->jobs_to_abort();
}

NScheduler::TAllocationToAbort ParseAllocationToAbort(const NScheduler::NProto::NNode::TAllocationToAbort& protoAllocationToAbort)
{
    NScheduler::TAllocationToAbort allocationToAbort;
    FromProto(&allocationToAbort, protoAllocationToAbort);

    return allocationToAbort;
}

NScheduler::TAllocationToAbort ParseAllocationToAbort(const NJobTrackerClient::NProto::TJobToAbort& protoJobToAbort)
{
    NJobTrackerClient::TJobToAbort jobToAbort;
    FromProto(&jobToAbort, protoJobToAbort);

    return {jobToAbort.JobId, jobToAbort.AbortReason};
}

const auto& GetAllocationsToInterrupt(const IJobController::TRspSchedulerHeartbeatPtr& response)
{
    return response->allocations_to_interrupt();
}

const auto& GetAllocationsToInterrupt(const IJobController::TRspOldSchedulerHeartbeatPtr& response)
{
    return response->jobs_to_interrupt();
}

const auto& GetAllocationId(const NScheduler::NProto::NNode::TAllocationToInterrupt& allocationToInterrupt)
{
    return allocationToInterrupt.allocation_id();
}

const auto& GetAllocationId(const NJobTrackerClient::NProto::TJobToInterrupt& jobToInterrupt)
{
    return jobToInterrupt.job_id();
}

auto* AddAllocations(const IJobController::TReqSchedulerHeartbeatPtr& request)
{
    return request->add_allocations();
}

auto* AddAllocations(const IJobController::TReqOldSchedulerHeartbeatPtr& request)
{
    return request->add_jobs();
}

auto* MutableUnconfirmedAllocations(const IJobController::TReqSchedulerHeartbeatPtr& request)
{
    return request->mutable_unconfirmed_allocations();
}

auto* MutableUnconfirmedAllocations(const IJobController::TReqOldSchedulerHeartbeatPtr& request)
{
    return request->mutable_unconfirmed_jobs();
}

const auto& GetAllocationsToStart(const IJobController::TRspSchedulerHeartbeatPtr& response)
{
    return response->allocations_to_start();
}

const auto& GetAllocationsToStart(const IJobController::TRspOldSchedulerHeartbeatPtr& response)
{
    return response->jobs_to_start();
}

[[maybe_unused]] TAllocationStartInfo GetAllocationStartInfoType(const IJobController::TRspSchedulerHeartbeatPtr& response);
[[maybe_unused]] NJobTrackerClient::NProto::TJobStartInfo GetAllocationStartInfoType(const IJobController::TRspOldSchedulerHeartbeatPtr& response);

[[maybe_unused]] TAllocationResult GetAllocationResultType(const IJobController::TReqSchedulerHeartbeatPtr& request);
[[maybe_unused]] NJobTrackerClient::NProto::TJobResult GetAllocationResultType(const IJobController::TReqOldSchedulerHeartbeatPtr& request);

const auto& GetAllocationId(const TAllocationStartInfo& allocationToInterrupt)
{
    return allocationToInterrupt.allocation_id();
}

const auto& GetAllocationId(const NJobTrackerClient::NProto::TJobStartInfo& jobToInterrupt)
{
    return jobToInterrupt.job_id();
}

auto* MutableAllocationId(TAllocationStatus* allocationStatus)
{
    return allocationStatus->mutable_allocation_id();
}

auto* MutableAllocationId(NJobTrackerClient::NProto::TJobStatus* jobStatus)
{
    return jobStatus->mutable_job_id();
}

// AllcationId is currently equal to JobId.
TJobId ParseAllocationIdAsJobId(auto& protoAllocationId)
{
    return FromProto<TJobId>(protoAllocationId);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJobController
    : public IJobController
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(const TJobPtr& job), JobFinished);
    DEFINE_SIGNAL_OVERRIDE(void(const TError& error), JobProxyBuildInfoUpdated);

public:
    TJobController(IBootstrapBase* bootstrap)
        : Config_(bootstrap->GetConfig()->ExecNode->JobController)
        , Bootstrap_(bootstrap)
        , Profiler_("/job_controller")
        , CacheHitArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_hit_artifacts_size"))
        , CacheMissArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_miss_artifacts_size"))
        , CacheBypassedArtifactsSizeCounter_(Profiler_.Counter("/chunk_cache/cache_bypassed_artifacts_size"))
        , TmpfsSizeGauge_(Profiler_.Gauge("/tmpfs/size"))
        , TmpfsUsageGauge_(Profiler_.Gauge("/tmpfs/usage"))
        , JobProxyMaxMemoryGauge_(Profiler_.Gauge("/job_proxy_max_memory"))
        , UserJobMaxMemoryGauge_(Profiler_.Gauge("/user_job_max_memory"))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);

        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

        Profiler_.AddProducer("/gpu_utilization", GpuUtilizationBuffer_);
        Profiler_.AddProducer("", ActiveJobCountBuffer_);
    }

    void Initialize() override
    {
        JobResourceManager_ = Bootstrap_->GetJobResourceManager();
        JobResourceManager_->RegisterResourcesConsumer(
            BIND_NO_PROPAGATE(&TJobController::OnResourceReleased, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()),
            EResourcesConsumerType::SchedulerJob);
        JobResourceManager_->SubscribeReservedMemoryOvercommited(
            BIND_NO_PROPAGATE(&TJobController::OnReservedMemoryOvercommited, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()));

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::OnProfiling, MakeWeak(this)),
            Config_->ProfilingPeriod);
        ProfilingExecutor_->Start();

        ResourceAdjustmentExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::AdjustResources, MakeWeak(this)),
            Config_->ResourceAdjustmentPeriod);
        ResourceAdjustmentExecutor_->Start();

        RecentlyRemovedJobCleaner_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::CleanRecentlyRemovedJobs, MakeWeak(this)),
            Config_->RecentlyRemovedJobsCleanPeriod);
        RecentlyRemovedJobCleaner_->Start();

        // Do not set period initially to defer start.
        JobProxyBuildInfoUpdater_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::UpdateJobProxyBuildInfo, MakeWeak(this)));
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
        dynamicConfigManager->SubscribeConfigChanged(
            BIND_NO_PROPAGATE(&TJobController::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void RegisterJobFactory(EJobType type, TJobFactory factory) override
    {
        YT_VERIFY(type < EJobType::SchedulerUnknown);
        EmplaceOrCrash(JobFactoryMap_, type, factory);
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
                NScheduler::EErrorCode::NoSuchJob,
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

    std::vector<TJobPtr> GetJobs() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(JobMapLock_);

        std::vector<TJobPtr> result;
        result.reserve(JobMap_.size());
        for (const auto& [id, job] : JobMap_) {
            result.push_back(job);
        }

        return result;
    }

    void SetDisableSchedulerJobs(bool value) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DisableJobs_.store(value);

        if (value) {
            TError error{"All scheduler jobs are disabled"};

            Bootstrap_->GetJobInvoker()->Invoke(BIND([=, this, this_ = MakeStrong(this), error{std::move(error)}] {
                VERIFY_THREAD_AFFINITY(JobThread);

                InterruptAllJobs(std::move(error));
            }));
        }
    }

    void PrepareAgentHeartbeatRequest(
        const TReqAgentHeartbeatPtr& request,
        const TAgentHeartbeatContextPtr& context) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        DoPrepareAgentHeartbeatRequest(request, context);
    }

    void ProcessAgentHeartbeatResponse(
        const TRspAgentHeartbeatPtr& response,
        const TAgentHeartbeatContextPtr& context) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        DoProcessAgentHeartbeatResponse(response, context);
    }

    TFuture<void> PrepareSchedulerHeartbeatRequest(
        const TReqSchedulerHeartbeatPtr& request) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return
            BIND(&TJobController::DoPrepareSchedulerHeartbeatRequest<TReqSchedulerHeartbeatPtr>, MakeStrong(this))
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(request);
    }

    TFuture<void> ProcessSchedulerHeartbeatResponse(
        const TRspSchedulerHeartbeatPtr& response) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return
            BIND(&TJobController::DoProcessSchedulerHeartbeatResponse<TRspSchedulerHeartbeatPtr>, MakeStrong(this))
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(response);
    }

    TFuture<void> PrepareSchedulerHeartbeatRequest(
        const TReqOldSchedulerHeartbeatPtr& request) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return
            BIND(&TJobController::DoPrepareSchedulerHeartbeatRequest<TReqOldSchedulerHeartbeatPtr>, MakeStrong(this))
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(request);
    }

    TFuture<void> ProcessSchedulerHeartbeatResponse(
        const TRspOldSchedulerHeartbeatPtr& response) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return
            BIND(&TJobController::DoProcessSchedulerHeartbeatResponse<TRspOldSchedulerHeartbeatPtr>, MakeStrong(this))
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(response);
    }

    bool IsJobProxyProfilingDisabled() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetDynamicConfig()->DisableJobProxyProfiling.value_or(Config_->DisableJobProxyProfiling);
    }

    NJobProxy::TJobProxyDynamicConfigPtr GetJobProxyDynamicConfig() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetDynamicConfig()->JobProxy;
    }

    TJobControllerDynamicConfigPtr GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto config = DynamicConfig_.Load();
        YT_VERIFY(config);

        return config;
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

    bool AreSchedulerJobsDisabled() const noexcept override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DisableJobs_.load();
    }

    void BuildJobProxyBuildInfo(TFluentAny fluent) const override
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

    void BuildJobsInfo(TFluentAny fluent) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto jobs = GetJobs();

        fluent.DoMapFor(
            jobs,
            [&] (TFluentMap fluent, const TJobPtr& job) {
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
                        .Item("resource_usage").Value(job->GetResourceUsage())
                        .Do(std::bind(&TJob::BuildOrchid, job, std::placeholders::_1))
                    .EndMap();
            });
    }

    void ScheduleStartJobs() override
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

    int GetActiveJobCount() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return std::ssize(JobMap_);
    }

    void OnAgentIncarnationOutdated(const TControllerAgentDescriptor& controllerAgentDescriptor) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        for (const auto& [id, job] : JobMap_) {
            if (job->GetControllerAgentDescriptor() == controllerAgentDescriptor) {
                job->UpdateControllerAgentDescriptor({});
            }
        }
    }

private:
    const TIntrusivePtr<const TJobControllerConfig> Config_;
    NClusterNode::IBootstrapBase* const Bootstrap_;
    IJobResourceManagerPtr JobResourceManager_;

    // For converting vcpu to cpu back after getting response from scheduler.
    // It is needed because cpu_to_vcpu_factor can change between preparing request and processing response.
    double LastHeartbeatCpuToVCpuFactor_ = 1.0;

    THashSet<NObjectClient::TJobId> JobIdsToConfirm_;

    TAtomicObject<TJobControllerDynamicConfigPtr> DynamicConfig_ = New<TJobControllerDynamicConfig>();

    THashMap<EJobType, TJobFactory> JobFactoryMap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, JobMapLock_);
    THashMap<TJobId, TJobPtr> JobMap_;
    THashMap<TOperationId, THashSet<TJobPtr>> OperationIdToJobs_;

    // Map of jobs to hold after remove. It is used to prolong lifetime of stderrs and job specs.
    struct TRecentlyRemovedJobRecord
    {
        TJobPtr Job;
        TInstant RemovalTime;
    };
    THashMap<TJobId, TRecentlyRemovedJobRecord> RecentlyRemovedJobMap_;

    //! Jobs that did not succeed in fetching spec are not getting
    //! their IJob structure, so we have to store job id alongside
    //! with the operation id to fill the TJobStatus proto message
    //! properly.
    THashMap<TJobId, TOperationId> SpecFetchFailedJobIds_;

    bool StartJobsScheduled_ = false;

    std::atomic<bool> DisableJobs_ = false;

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

    TGauge TmpfsSizeGauge_;
    TGauge TmpfsUsageGauge_;
    TGauge JobProxyMaxMemoryGauge_;
    TGauge UserJobMaxMemoryGauge_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr ResourceAdjustmentExecutor_;
    TPeriodicExecutorPtr RecentlyRemovedJobCleaner_;
    TPeriodicExecutorPtr JobProxyBuildInfoUpdater_;

    TInstant LastStoredJobsSendTime_;

    TAtomicObject<TErrorOr<TBuildInfoPtr>> CachedJobProxyBuildInfo_;

    TInstant LastOperationInfosRequestTime_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    const TJobFactory& GetJobFactory(EJobType type) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetOrCrash(JobFactoryMap_, type);
    }

    template <class TAllocationStartInfo>
    TFuture<void> RequestJobSpecsAndStartJobs(std::vector<TAllocationStartInfo> jobStartInfos)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        THashMap<TControllerAgentDescriptor, std::vector<TAllocationStartInfo>> groupedStartInfos;

        for (auto& startInfo : jobStartInfos) {
            auto operationId = FromProto<TOperationId>(startInfo.operation_id());
            auto jobId = ParseAllocationIdAsJobId(GetAllocationId(startInfo));

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

            auto getJobSpecsTimeout = GetDynamicConfig()->GetJobSpecsTimeout.value_or(
                Config_->GetJobSpecsTimeout);

            jobSpecServiceProxy.SetDefaultTimeout(getJobSpecsTimeout);
            auto jobSpecRequest = jobSpecServiceProxy.GetJobSpecs();

            for (const auto& startInfo : startInfos) {
                auto* subrequest = jobSpecRequest->add_requests();
                *subrequest->mutable_operation_id() = startInfo.operation_id();
                *subrequest->mutable_job_id() = GetAllocationId(startInfo);
            }

            YT_LOG_DEBUG("Requesting job specs (SpecServiceAddress: %v, Count: %v)",
                agentDescriptor.Address,
                startInfos.size());

            auto asyncResult = jobSpecRequest->Invoke().Apply(
                BIND(
                    &TJobController::OnJobSpecsReceived<TAllocationStartInfo>,
                    MakeStrong(this),
                    Passed(std::move(startInfos)),
                    agentDescriptor)
                .AsyncVia(Bootstrap_->GetJobInvoker()));
            asyncResults.push_back(asyncResult);
        }

        return AllSet(asyncResults).As<void>();
    }

    template <class TAllocationStartInfo>
    void OnJobSpecsReceived(
        std::vector<TAllocationStartInfo> startInfos,
        const TControllerAgentDescriptor& controllerAgentDescriptor,
        const TJobSpecServiceProxy::TErrorOrRspGetJobSpecsPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Error getting job specs (SpecServiceAddress: %v)",
                controllerAgentDescriptor.Address);
            for (const auto& startInfo : startInfos) {
                auto jobId = ParseAllocationIdAsJobId(GetAllocationId(startInfo));
                auto operationId = FromProto<TOperationId>(startInfo.operation_id());
                EmplaceOrCrash(SpecFetchFailedJobIds_, jobId, operationId);
            }
            return;
        }

        YT_LOG_DEBUG("Job specs received (SpecServiceAddress: %v)", controllerAgentDescriptor.Address);

        const auto& rsp = rspOrError.Value();

        YT_VERIFY(rsp->responses_size() == std::ssize(startInfos));
        for (size_t index = 0; index < startInfos.size(); ++index) {
            auto& startInfo = startInfos[index];
            auto operationId = FromProto<TOperationId>(startInfo.operation_id());
            auto jobId = ParseAllocationIdAsJobId(GetAllocationId(startInfo));

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

            CreateJob(
                jobId,
                operationId,
                startInfo.resource_limits(),
                std::move(spec),
                controllerAgentDescriptor);
        }
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

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

    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        static const TString tmpfsSizeSensorName = "/user_job/tmpfs_size/sum";
        static const TString jobProxyMaxMemorySensorName = "/job_proxy/max_memory/sum";
        static const TString userJobMaxMemorySensorName = "/user_job/max_memory/sum";

        ActiveJobCountBuffer_->Update([this] (ISensorWriter* writer) {
            TWithTagGuard tagGuard(writer, "origin", FormatEnum(EJobOrigin::Scheduler));
            writer->AddGauge("/active_job_count", GetJobs().size());
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
        i64 tmpfsSize = 0;
        i64 tmpfsUsage = 0;
        for (const auto& job : GetJobs()) {
            YT_VERIFY(TypeFromId(job->GetId()) == EObjectType::SchedulerJob);

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

        TmpfsSizeGauge_.Update(tmpfsSize);
        TmpfsUsageGauge_.Update(tmpfsUsage);

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

    TErrorOr<TControllerAgentDescriptor> TryParseControllerAgentDescriptor(
        const NJobTrackerClient::NProto::TControllerAgentDescriptor& proto) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto incarnationId = FromProto<NScheduler::TIncarnationId>(proto.incarnation_id());

        auto addressOrError = TryParseControllerAgentAddress(proto.addresses());
        if (!addressOrError.IsOK()) {
            return TError{std::move(addressOrError)};
        }

        return TControllerAgentDescriptor{std::move(addressOrError.Value()), incarnationId};
    }

    TErrorOr<TString> TryParseControllerAgentAddress(
        const NNodeTrackerClient::NProto::TAddressMap& proto) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

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

    void OnJobResourcesUpdated(const TWeakPtr<TJob>& weakCurrentJob, const TNodeResources& resourceDelta)
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
                for (const auto& job : GetJobs()) {
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

    void OnResourceReleased()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ScheduleStartJobs();
    }

    void DoPrepareAgentHeartbeatRequest(
        TReqAgentHeartbeatPtr request,
        TAgentHeartbeatContextPtr context)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        request->set_node_id(Bootstrap_->GetNodeId());
        ToProto(request->mutable_node_descriptor(), Bootstrap_->GetLocalDescriptor());
        ToProto(request->mutable_controller_agent_incarnation_id(), context->AgentDescriptor.IncarnationId);

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

        std::vector<TJobPtr> runningJobs;
        runningJobs.reserve(std::size(JobMap_));

        i64 finishedJobsStatisticsSize = 0;

        bool totalConfirmation = NeedTotalConfirmation(context->LastTotalConfirmationTime);
        YT_LOG_DEBUG_IF(
            totalConfirmation,
            "Send all finished jobs due to total confirmation (ControllerAgentDescriptor: %v)",
            context->AgentDescriptor);

        for (const auto& [id, job] : JobMap_) {
            const auto& controllerAgentDescriptor = job->GetControllerAgentDescriptor();

            if (!controllerAgentDescriptor) {
                YT_LOG_DEBUG(
                    "Skipping heartbeat for job since old agent incarnation is outdated and new incarnation is not received yet (JobId: %v)",
                    job->GetId());
                continue;
            }

            if (controllerAgentDescriptor != context->AgentDescriptor) {
                continue;
            }

            switch (job->GetState()) {
                case EJobState::Running:
                    runningJobs.push_back(job);
                    break;
                case EJobState::Aborted:
                case EJobState::Failed:
                case EJobState::Completed:
                    if (context->SentEnqueuedJobs.contains(job) || totalConfirmation) {
                        auto* const jobStatus = request->add_jobs();
                        FillJobStatus(jobStatus, job);

                        *jobStatus->mutable_result() = job->GetResult();

                        job->ResetStatisticsLastSendTime();

                        if (auto statistics = getJobStatistics(job)) {
                            auto statisticsString = statistics.ToString();
                            finishedJobsStatisticsSize += std::ssize(statisticsString);
                            jobStatus->set_statistics(std::move(statisticsString));
                        }

                        context->SentEnqueuedJobs.erase(job);
                    }

                    break;
                default:
                    break;
            }
        }

        if (!std::empty(context->SentEnqueuedJobs)) {
            constexpr int maxJobCountToLog = 5;

            std::vector<TJobId> nonSentJobs;
            nonSentJobs.reserve(maxJobCountToLog);
            for (const auto& job : context->SentEnqueuedJobs) {
                if (std::ssize(nonSentJobs) >= maxJobCountToLog) {
                    break;
                }
                nonSentJobs.push_back(job->GetId());
            }

            YT_LOG_DEBUG(
                "Can not report some jobs because of agent missmatch (TotalUnreportedJobCount: %v, JobSample: %v, ControllerAgentDescriptor: %v)",
                std::size(context->SentEnqueuedJobs),
                nonSentJobs,
                context->AgentDescriptor);
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
        int consideredRunnigJobCount = 0;
        int reportedRunningJobCount = 0;
        i64 runningJobsStatisticsSize = 0;
        for (const auto& job : runningJobs) {
            auto* jobStatus = request->add_jobs();

            FillJobStatus(jobStatus, job);

            if (now - job->GetStatisticsLastSendTime() < context->RunningJobInfoSendingBackoff) {
                break;
            }

            ++consideredRunnigJobCount;

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

        YT_LOG_DEBUG(
            "Job statistics for agent prepared (RunningJobsStatisticsSize: %v, FinishedJobsStatisticsSize: %v, "
            "RunningJobCount: %v, SkippedJobCountDueToBackoff: %v, SkippedJobCountDueToStatisticsSizeThrottling: %v, "
            "AgentDescriptor: %v)",
            runningJobsStatisticsSize,
            finishedJobsStatisticsSize,
            std::size(runningJobs),
            std::ssize(runningJobs) - consideredRunnigJobCount,
            consideredRunnigJobCount - reportedRunningJobCount,
            context->AgentDescriptor);
    }

    void DoProcessAgentHeartbeatResponse(
        TRspAgentHeartbeatPtr /*response*/,
        TAgentHeartbeatContextPtr /*context*/)
    {
        VERIFY_THREAD_AFFINITY(JobThread);
    }

    template <class TRspSchedulerHeartbeatPtr>
    void DoProcessSchedulerHeartbeatResponse(
        TRspSchedulerHeartbeatPtr response)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        {
            THashSet<TControllerAgentDescriptor> receivedRegisteredAgents;
            receivedRegisteredAgents.reserve(response->registered_controller_agents_size());
            for (const auto& protoAgentDescriptor : response->registered_controller_agents()) {
                auto descriptorOrError = TryParseControllerAgentDescriptor(protoAgentDescriptor);
                YT_LOG_FATAL_IF(
                    !descriptorOrError.IsOK(),
                    descriptorOrError,
                    "Failed to parse registered controller agent descriptor");

                EmplaceOrCrash(receivedRegisteredAgents, std::move(descriptorOrError.Value()));
            }

            const auto& controllerAgentConnectorPool = Bootstrap_->GetExecNodeBootstrap()->GetControllerAgentConnectorPool();
            controllerAgentConnectorPool->OnRegisteredAgentSetReceived(std::move(receivedRegisteredAgents));
        }

        for (const auto& protoJobToRemove : response->jobs_to_remove()) {
            auto jobToRemove = FromProto<TJobToRelease>(protoJobToRemove);
            auto jobId = jobToRemove.JobId;
            if (SpecFetchFailedJobIds_.erase(jobId) == 1) {
                continue;
            }

            if (auto job = FindJob(jobId)) {
                RemoveJob(job, jobToRemove.ReleaseFlags);
            } else {
                YT_LOG_WARNING("Requested to remove a non-existent job (JobId: %v)",
                    jobId);
            }
        }

        for (const auto& protoJobToAbort : GetAllocationsToAbort(response)) {
            auto allocationToAbort = ParseAllocationToAbort(protoJobToAbort);

            if (auto job = FindJob(allocationToAbort.AllocationId)) {
                AbortJob(job, std::move(allocationToAbort));
            } else {
                YT_LOG_WARNING("Requested to abort a non-existent job (JobId: %v, AbortReason: %v)",
                    allocationToAbort.AllocationId,
                    allocationToAbort.AbortReason);
            }
        }

        for (const auto& jobToInterrupt : GetAllocationsToInterrupt(response)) {
            auto timeout = FromProto<TDuration>(jobToInterrupt.timeout());
            auto jobId = ParseAllocationIdAsJobId(GetAllocationId(jobToInterrupt));

            YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

            if (auto job = FindJob(jobId)) {
                std::optional<TString> preemptionReason;
                if (jobToInterrupt.has_preemption_reason()) {
                    preemptionReason = jobToInterrupt.preemption_reason();
                }

                EInterruptReason interruptionReason = EInterruptReason::None;
                if (jobToInterrupt.has_interruption_reason()) {
                    interruptionReason = CheckedEnumCast<EInterruptReason>(jobToInterrupt.interruption_reason());
                }

                std::optional<NScheduler::TPreemptedFor> preemptedFor;
                if (jobToInterrupt.has_preempted_for()) {
                    preemptedFor = FromProto<NScheduler::TPreemptedFor>(jobToInterrupt.preempted_for());
                }

                job->Interrupt(timeout, interruptionReason, preemptionReason, preemptedFor);
            } else {
                YT_LOG_WARNING("Requested to interrupt a non-existing job (JobId: %v)",
                    jobId);
            }
        }

        for (const auto& protoJobId : response->jobs_to_fail()) {
            auto jobId = FromProto<TJobId>(protoJobId);

            YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

            if (auto job = FindJob(jobId)) {
                job->Fail();
            } else {
                YT_LOG_WARNING("Requested to fail a non-existent job (JobId: %v)",
                    jobId);
            }
        }

        for (const auto& protoJobId : response->jobs_to_store()) {
            auto jobId = FromProto<TJobId>(protoJobId);

            YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

            if (auto job = FindJob(jobId)) {

                YT_LOG_DEBUG("Storing job (JobId: %v)",
                    jobId);
                job->SetStored(true);
            } else {
                YT_LOG_WARNING("Requested to store a non-existent job (JobId: %v)",
                    jobId);
            }
        }

        std::vector<TJobId> jobIdsToConfirm;
        jobIdsToConfirm.reserve(response->jobs_to_confirm_size());
        for (auto& jobInfo : *response->mutable_jobs_to_confirm()) {
            auto jobId = FromProto<TJobId>(jobInfo.job_id());

            YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

            YT_LOG_DEBUG("Requested to confirm job (JobId: %v)", jobId);

            auto agentInfoOrError = TryParseControllerAgentDescriptor(*jobInfo.mutable_controller_agent_descriptor());
            if (!agentInfoOrError.IsOK()) {
                YT_LOG_WARNING(
                    agentInfoOrError,
                    "Skip job to confirm since no suitable controller agent address exists (JobId: %v)",
                    jobId);
                continue;
            }

            if (auto job = FindJob(jobId)) {
                job->UpdateControllerAgentDescriptor(std::move(agentInfoOrError.Value()));
            }

            jobIdsToConfirm.push_back(jobId);
        }

        JobIdsToConfirm_.clear();
        if (!jobIdsToConfirm.empty()) {
            JobIdsToConfirm_.insert(std::cbegin(jobIdsToConfirm), std::cend(jobIdsToConfirm));
        }

        // COMPAT(pogorelov)
        if constexpr (std::is_same_v<TRspSchedulerHeartbeatPtr, IJobController::TRspSchedulerHeartbeatPtr>) {
            for (const auto& protoOperationInfo : response->operation_infos()) {
                auto operationId = FromProto<TOperationId>(protoOperationInfo.operation_id());
                if (!protoOperationInfo.running()) {
                    HandleJobsOfNonRunningOperation(operationId);
                    continue;
                }

                if (!protoOperationInfo.has_controller_agent_descriptor()) {
                    UpdateOperationControllerAgent(operationId, TControllerAgentDescriptor{});
                    continue;
                }

                auto descriptorOrError = TryParseControllerAgentDescriptor(protoOperationInfo.controller_agent_descriptor());
                YT_LOG_FATAL_IF(
                    !descriptorOrError.IsOK(),
                    descriptorOrError,
                    "Failed to parse new controller agent descriptor for operation (OperationId: %v)",
                    operationId);

                UpdateOperationControllerAgent(operationId, std::move(descriptorOrError.Value()));
            }

            {
                auto minSpareResources = FromProto<NScheduler::TJobResources>(response->min_spare_resources());

                const auto& schedulerConnector = Bootstrap_->GetExecNodeBootstrap()->GetSchedulerConnector();
                schedulerConnector->SetMinSpareResources(minSpareResources);
            }
        }

        YT_VERIFY(response->Attachments().empty());

        std::vector<decltype(GetAllocationStartInfoType(response))> jobStartInfos;
        jobStartInfos.reserve(GetAllocationsToStart(response).size());
        for (const auto& startInfo : GetAllocationsToStart(response)) {
            jobStartInfos.push_back(startInfo);

            // We get vcpu here. Need to replace it with real cpu back.
            auto& resourceLimits = *jobStartInfos.back().mutable_resource_limits();
            resourceLimits.set_cpu(static_cast<double>(NVectorHdrf::TCpuResource(resourceLimits.cpu() / LastHeartbeatCpuToVCpuFactor_)));
        }

        auto error = WaitFor(RequestJobSpecsAndStartJobs(std::move(jobStartInfos)));
        YT_LOG_DEBUG_UNLESS(
            error.IsOK(),
            error,
            "Failed to request some job specs");
    }

    template <class TReqSchedulerHeartbeatPtr>
    void DoPrepareSchedulerHeartbeatRequest(
        TReqSchedulerHeartbeatPtr request)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Preparing scheduler heartbeat request");

        request->set_node_id(Bootstrap_->GetNodeId());
        ToProto(request->mutable_node_descriptor(), Bootstrap_->GetLocalDescriptor());
        *request->mutable_resource_limits() = JobResourceManager_->GetResourceLimits();
        *request->mutable_resource_usage() = JobResourceManager_->GetResourceUsage(/*includeWaiting*/ true);

        *request->mutable_disk_resources() = JobResourceManager_->GetDiskResources();

        const auto& jobReporter = Bootstrap_->GetExecNodeBootstrap()->GetJobReporter();
        request->set_job_reporter_write_failures_count(jobReporter->ExtractWriteFailuresCount());
        request->set_job_reporter_queue_is_too_large(jobReporter->GetQueueIsTooLarge());

        // Only for scheduler `cpu` stores `vcpu` actually.
        // In all resource limits and usages we send and get back vcpu instead of cpu.
        LastHeartbeatCpuToVCpuFactor_ = JobResourceManager_->GetCpuToVCpuFactor();
        ReplaceCpuWithVCpu(*request->mutable_resource_limits());
        ReplaceCpuWithVCpu(*request->mutable_resource_usage());

        auto* execNodeBootstrap = Bootstrap_->GetExecNodeBootstrap();
        if (execNodeBootstrap->GetSlotManager()->HasFatalAlert()) {
            // NB(psushin): if slot manager is disabled with fatal alert we might have experienced an unrecoverable failure (e.g. hanging Porto)
            // and to avoid inconsistent state with scheduler we decide not to report to it any jobs at all.
            // We also drop all scheduler jobs from |JobMap_|.
            RemoveSchedulerJobsOnFatalAlert();

            request->set_confirmed_job_count(0);

            return;
        }

        const bool totalConfirmation = NeedSchedulerTotalConfirmation();
        YT_LOG_INFO_IF(totalConfirmation, "Including all stored jobs in heartbeat");

        int confirmedJobCount = 0;

        bool shouldSendControllerAgentHeartbeatsOutOfBand = false;

        const bool requestOperationInfosForStoredJobs =
            TInstant::Now() > LastOperationInfosRequestTime_ +
                DynamicConfig_.Load()->OperationInfosRequestPeriod;
        THashSet<TOperationId> operationIdsToRequestInfo;

        for (const auto& job : GetJobs()) {
            auto jobId = job->GetId();

            YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

            if (job->GetStored() && requestOperationInfosForStoredJobs) {
                operationIdsToRequestInfo.insert(job->GetOperationId());
            }

            auto confirmIt = JobIdsToConfirm_.find(jobId);
            if (job->GetStored() && !totalConfirmation && confirmIt == std::cend(JobIdsToConfirm_)) {
                continue;
            }

            const bool sendConfirmedJobToControllerAgent = !(job->GetStored() &&
                confirmIt == std::cend(JobIdsToConfirm_) &&
                totalConfirmation);

            if (job->GetStored() || confirmIt != std::cend(JobIdsToConfirm_)) {
                YT_LOG_DEBUG("Confirming job (JobId: %v, OperationId: %v, Stored: %v, State: %v)",
                    jobId,
                    job->GetOperationId(),
                    job->GetStored(),
                    job->GetState());
                ++confirmedJobCount;
            }
            if (confirmIt != std::cend(JobIdsToConfirm_)) {
                JobIdsToConfirm_.erase(confirmIt);
            }

            auto* allocationStatus = AddAllocations(request);
            FillJobStatus(allocationStatus, job);
            switch (job->GetState()) {
                case EJobState::Running: {
                    auto& resourceUsage = *allocationStatus->mutable_resource_usage();
                    resourceUsage = job->GetResourceUsage();
                    ReplaceCpuWithVCpu(resourceUsage);
                    break;
                }
                case EJobState::Completed:
                case EJobState::Aborted:
                case EJobState::Failed: {
                    ToProto(allocationStatus->mutable_result()->mutable_error(), job->GetJobError());

                    if (sendConfirmedJobToControllerAgent) {
                        auto controllerAgentConnector = job->GetControllerAgentConnector();
                        if (controllerAgentConnector) {
                            controllerAgentConnector->EnqueueFinishedJob(job);
                            shouldSendControllerAgentHeartbeatsOutOfBand = true;
                        } else {
                            YT_LOG_DEBUG(
                                "Controller agent for job is not received yet; "
                                "finished job info will be reported later (JobId: %v, JobControllerAgentDescriptor: %v)",
                                jobId,
                                job->GetControllerAgentDescriptor());
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }

        request->set_confirmed_job_count(confirmedJobCount);

        for (auto [jobId, operationId] : GetSpecFetchFailedJobIds()) {
            auto* jobStatus = AddAllocations(request);
            ToProto(MutableAllocationId(jobStatus), jobId);
            ToProto(jobStatus->mutable_operation_id(), operationId);
            jobStatus->set_state(static_cast<int>(JobStateToAllocationState(EJobState::Aborted)));

            jobStatus->mutable_time_statistics();

            decltype(GetAllocationResultType(request)) jobResult;
            auto error = TError("Failed to get job spec")
                << TErrorAttribute("abort_reason", EAbortReason::GetSpecFailed);
            ToProto(jobResult.mutable_error(), error);
            *jobStatus->mutable_result() = jobResult;
        }

        if (!std::empty(JobIdsToConfirm_)) {
            YT_LOG_WARNING("Unconfirmed jobs found (UnconfirmedJobCount: %v)", std::size(JobIdsToConfirm_));
            for (auto jobId : JobIdsToConfirm_) {
                YT_LOG_DEBUG("Unconfirmed job (JobId: %v)", jobId);
            }
            ToProto(MutableUnconfirmedAllocations(request), JobIdsToConfirm_);
        }

        // COMPAT(pogorelov)
        if constexpr (std::is_same_v<TReqSchedulerHeartbeatPtr, IJobController::TReqSchedulerHeartbeatPtr>) {
            if (requestOperationInfosForStoredJobs) {
                YT_LOG_DEBUG("Adding operation info requests for stored jobs (Count: %v)", std::size(operationIdsToRequestInfo));

                for (auto operationId : operationIdsToRequestInfo) {
                    ToProto(request->add_operations_ids_to_request_info(), operationId);
                }

                LastOperationInfosRequestTime_ = TInstant::Now();
            }
        }

        YT_LOG_DEBUG("Scheduler heartbeat request prepared");

        if (shouldSendControllerAgentHeartbeatsOutOfBand) {
            Bootstrap_
                ->GetExecNodeBootstrap()
                ->GetControllerAgentConnectorPool()
                ->SendOutOfBandHeartbeatsIfNeeded();
        }
    }

    void StartWaitingJobs()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto resourceAcquiringContext = JobResourceManager_->GetResourceAcquiringProxy();

        for (const auto& job : GetJobs()) {
            if (job->GetState() != EJobState::Waiting) {
                continue;
            }

            auto jobId = job->GetId();
            YT_LOG_DEBUG("Trying to start job (JobId: %v)", jobId);

            if (!resourceAcquiringContext.TryAcquireResourcesFor(job->AsResourceHolder())) {
                YT_LOG_DEBUG("Job was not started (JobId: %v)", jobId);
            } else {
                YT_LOG_DEBUG("Job started (JobId: %v)", jobId);
            }
        }

        StartJobsScheduled_ = false;
    }

    TJobPtr CreateJob(
        TJobId jobId,
        TOperationId operationId,
        const TNodeResources& resourceLimits,
        TJobSpec&& jobSpec,
        const TControllerAgentDescriptor& controllerAgentDescriptor)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto type = CheckedEnumCast<EJobType>(jobSpec.type());
        auto factory = GetJobFactory(type);

        auto jobSpecExtId = NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext;
        auto waitingJobTimeout = Config_->WaitingJobsTimeout;

        YT_VERIFY(jobSpec.HasExtension(jobSpecExtId));
        const auto& jobSpecExt = jobSpec.GetExtension(jobSpecExtId);
        if (jobSpecExt.has_waiting_job_timeout()) {
            waitingJobTimeout = FromProto<TDuration>(jobSpecExt.waiting_job_timeout());
        }

        auto job = factory(
            jobId,
            operationId,
            resourceLimits,
            std::move(jobSpec),
            controllerAgentDescriptor);

        YT_LOG_INFO("Scheduler job created (JobId: %v, OperationId: %v, JobType: %v)",
            jobId,
            operationId,
            type);

        RegisterJob(jobId, job, waitingJobTimeout);

        return job;
    }

    void RegisterJob(TJobId jobId, const TJobPtr& job, TDuration waitingJobTimeout)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        {
            auto guard = WriterGuard(JobMapLock_);
            EmplaceOrCrash(JobMap_, jobId, job);
            EmplaceOrCrash(OperationIdToJobs_[job->GetOperationId()], job);
        }

        job->SubscribeResourcesUpdated(
            BIND_NO_PROPAGATE(&TJobController::OnJobResourcesUpdated, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetJobInvoker()));

        job->SubscribeJobPrepared(
            BIND_NO_PROPAGATE(&TJobController::OnJobPrepared, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetJobInvoker()));

        job->SubscribeJobFinished(
            BIND_NO_PROPAGATE(&TJobController::OnJobFinished, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetJobInvoker()));

        ScheduleStartJobs();

        TDelayedExecutor::Submit(
            BIND(&TJobController::OnWaitingJobTimeout, MakeWeak(this), MakeWeak(job), waitingJobTimeout),
            waitingJobTimeout,
            Bootstrap_->GetJobInvoker());
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

    void AbortJob(const TJobPtr& job, NScheduler::TAllocationToAbort&& abortAttributes)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Aborting job (JobId: %v, AbortReason: %v)",
            job->GetId(),
            abortAttributes.AbortReason);

        TError error(NExecNode::EErrorCode::AbortByScheduler, "Job aborted by scheduler");
        if (abortAttributes.AbortReason) {
            error = error << TErrorAttribute("abort_reason", *abortAttributes.AbortReason);
        }

        job->Abort(error);
    }

    void RemoveJob(
        const TJobPtr& job,
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

        UnregisterJob(job);

        YT_LOG_INFO("Job removed (JobId: %v, Save: %v)", job->GetId(), shouldSave);
    }

    TDuration GetTotalConfirmationPeriod() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return GetDynamicConfig()->TotalConfirmationPeriod.value_or(
            Config_->TotalConfirmationPeriod);
    }

    TDuration GetMemoryOverdraftTimeout() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return GetDynamicConfig()->MemoryOverdraftTimeout.value_or(
            Config_->MemoryOverdraftTimeout);
    }

    TDuration GetCpuOverdraftTimeout() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return GetDynamicConfig()->CpuOverdraftTimeout.value_or(
            Config_->CpuOverdraftTimeout);
    }

    TDuration GetRecentlyRemovedJobsStoreTimeout() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return GetDynamicConfig()->RecentlyRemovedJobsStoreTimeout.value_or(
            Config_->RecentlyRemovedJobsStoreTimeout);
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

        while (usage.user_memory() + mappedMemory > limits.user_memory() &&
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
            auto jobs = GetRunningJobsSortedByStartTime();

            while ((preemptCpuOverdraft && usage.cpu() > limits.cpu()) ||
                (preemptMemoryOverdraft && usage.user_memory() > limits.user_memory()))
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

    void RemoveSchedulerJobsOnFatalAlert()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        std::vector<TJobPtr> jobsToRemove;
        jobsToRemove.reserve(std::size(JobMap_));
        for (const auto& [jobId, job] : JobMap_) {
            YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

            YT_LOG_INFO("Removing job %v due to fatal alert", jobId);
            job->Abort(TError("Job aborted due to fatal alert"));

            jobsToRemove.push_back(job);
        }

        for (const auto& job : jobsToRemove) {
            UnregisterJob(job);
        }
    }

    bool NeedTotalConfirmation(TInstant& lastTotalConfirmationTime)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (const auto now = TInstant::Now();
            lastTotalConfirmationTime + GetTotalConfirmationPeriod() < now)
        {
            lastTotalConfirmationTime = now;
            return true;
        }

        return false;
    }

    bool NeedSchedulerTotalConfirmation() noexcept
    {
        return NeedTotalConfirmation(LastStoredJobsSendTime_);
    }

    std::vector<TJobPtr> GetRunningJobsSortedByStartTime() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TJobPtr> schedulerJobs;
        for (const auto& job : GetJobs()) {
            YT_VERIFY(TypeFromId(job->GetId()) == EObjectType::SchedulerJob);

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
        for (const auto& job : GetJobs()) {
            YT_VERIFY(TypeFromId(job->GetId()) == EObjectType::SchedulerJob);

            const auto& Logger = job->GetLogger();
            try {
                YT_LOG_DEBUG(error, "Trying to interrupt job");
                job->Interrupt(
                    /*timeout*/ {},
                    EInterruptReason::Unknown,
                    /*preemptionReason*/ {},
                    /*preemptedFor*/ {});
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to interrupt job");
            }
        }
    }

    void OnJobPrepared(const TWeakPtr<TJob>& weakJob)
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

    void OnJobFinished(const TWeakPtr<TJob>& weakJob)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto job = weakJob.Lock();
        if (!job || !job->IsStarted()) {
            return;
        }

        auto* jobFinalStateCounter = GetJobFinalStateCounter(job->GetState());
        jobFinalStateCounter->Increment();

        JobFinished_.Fire(job);
    }

    const THashMap<TJobId, TOperationId>& GetSpecFetchFailedJobIds() const noexcept
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        return SpecFetchFailedJobIds_;
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
        if (operationJobsIt == std::cend(OperationIdToJobs_)) {
            YT_LOG_DEBUG("There are no operation jobs on node (OperationId: %v)", operationId);
            return;
        }

        std::vector operationJobs(std::begin(operationJobsIt->second), std::end(operationJobsIt->second));
        for (auto job : operationJobs) {
            if (job->IsFinished()) {
                RemoveJob(job, TReleaseJobFlags{});
            } else {
                job->Abort(TError{"Operation %v is not running", operationId});
            }
        }
    }

    void UpdateOperationControllerAgent(
        TOperationId operationId,
        TControllerAgentDescriptor controllerAgentDescriptor)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto operationJobsIt = OperationIdToJobs_.find(operationId);
        if (operationJobsIt == std::cend(OperationIdToJobs_)) {
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
};

////////////////////////////////////////////////////////////////////////////////

IJobControllerPtr CreateJobController(NClusterNode::IBootstrapBase* bootstrap)
{
    return New<TJobController>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
