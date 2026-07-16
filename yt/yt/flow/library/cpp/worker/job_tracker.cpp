#include "job_tracker.h"

#include "buffer_state_manager.h"
#include "input_buffer.h"
#include "input_manager.h"
#include "job.h"
#include "job_spec.h"

#include "message_distributor.h"
#include "traced_invoker.h"


#include "private.h"

#include <yt/yt/flow/library/cpp/common/external_metrics_reporter.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/actions/codicil_guarded_invoker.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/ema_counter.h>

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/ytprof/heap_profiler.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NFlow::NWorker {

using namespace NApi;
using namespace NConcurrency;
using namespace NQueryClient;
using namespace NTracing;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IClientPtr TJobTrackerContext::GetClient() const
{
    THROW_ERROR_EXCEPTION_UNLESS(PipelinePath.GetCluster().has_value(), "Pipeline path must have cluster");
    return ClientsCache->GetClient(*PipelinePath.GetCluster());
}

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = WorkerLogger;

constexpr auto JobIdTag = "job_id";

////////////////////////////////////////////////////////////////////////////////

struct TFailedJob
    : public IJob
{
    TFailedJob(TJobId jobId, TComputationId computationId, TError error)
        : JobId_(jobId)
        , ComputationId_(std::move(computationId))
        , Error_(std::move(error))
        , Timestamp_(TInstant::Now())
    { }

    TFuture<void> Start() override
    {
        return OKFuture;
    }

    TFuture<void> Reconfigure(i64 /*specGeneration*/, TDynamicJobSpecPtr /*dynamicSpec*/) override
    {
        return OKFuture;
    }

    TFuture<void> UpdateWatermarkState(TWatermarkStatePtr /*watermarkState*/) override
    {
        return OKFuture;
    }

    TFuture<void> UpdateTraverseData(TToPartitionTraverseDataPtr /*traverse*/) override
    {
        return OKFuture;
    }

    void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr /*messageTransferingInfo*/) override
    { }

    TFuture<void> Stop() override
    {
        return OKFuture;
    }

    TFuture<void> Stop(TError /*error*/) override
    {
        return OKFuture;
    }

    TJobId GetJobId() override
    {
        return JobId_;
    }

    TComputationId GetComputationId() override
    {
        return ComputationId_;
    }

    IInputBufferPtr GetInputBuffer() override
    {
        return nullptr;
    }

    TFuture<TJobStatusPtr> GetStatus() override
    {
        auto status = New<TJobStatus>();
        status->JobId = GetJobId();
        status->IsFinished = true;
        status->StartTime = Timestamp_;
        status->FinishTime = Timestamp_;
        status->UpdateTime = TInstant::Now();
        status->Error = Error_;
        return MakeFuture(status);
    }

    TFuture<TJobOrchidStatePtr> GetOrchidState() override
    {
        return GetStatus().Apply(BIND([] (const TJobStatusPtr& status) {
            auto orchidState = New<TJobOrchidState>();
            orchidState->Status = status;
            return orchidState;
        }));
    }

private:
    const TJobId JobId_;
    const TComputationId ComputationId_;
    const TError Error_;
    const TInstant Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

class TExternalPerformanceMetricsReporter
    : public IExternalPerformanceMetricsReporter
{
public:
    TExternalPerformanceMetricsReporter()
        : ElapsedCpuTimeMicros_(0)
        , MemoryUsage_(0)
    { }

    void SetMemoryUsage(size_t memoryUsage) override
    {
        MemoryUsage_.store(memoryUsage, std::memory_order::relaxed);
    }

    void AddCpuTime(TDuration cpuTime) override
    {
        ElapsedCpuTimeMicros_.fetch_add(cpuTime.MicroSeconds(), std::memory_order::relaxed);
    }

    size_t GetMemoryUsage()
    {
        return MemoryUsage_.load(std::memory_order::relaxed);
    }

    TDuration GetElapsedCpuTime()
    {
        return TDuration::MicroSeconds(ElapsedCpuTimeMicros_.load(std::memory_order::relaxed));
    }

private:
    std::atomic<ui64> ElapsedCpuTimeMicros_;
    std::atomic<size_t> MemoryUsage_;
};

DEFINE_REFCOUNTED_TYPE(TExternalPerformanceMetricsReporter);

////////////////////////////////////////////////////////////////////////////////

int GetJobThreadPoolSize(const TDynamicPipelineSpecPtr& dynamicSpec, const TNodeInfoPtr& nodeInfo)
{
    if (auto size = dynamicSpec->JobTracker->JobThreads) {
        return *size;
    }
    if (nodeInfo->VcpuFactor && nodeInfo->VcpuLimit) {
        return std::ceil(*nodeInfo->VcpuLimit / 1000.0 / *nodeInfo->VcpuFactor);
    }
    return TDynamicJobTrackerSpec::DefaultJobThreads;
}

////////////////////////////////////////////////////////////////////////////////

class TJobTracker
    : public IJobTracker
{
public:
    explicit TJobTracker(TJobTrackerContextPtr context)
        : Context_(std::move(context))
        , ExecutionSpec_(New<TExecutionSpec>())
        , BufferStateManager_(CreateBufferStateManager(Context_->ControlInvoker, Context_->JobDirectory, GetDynamicPipelineSpec()->JobTracker->BufferStateManager))
        , EvaluatorCache_(CreateFastColumnEvaluatorCache())
        , JobControlThreadPool_(CreateFairShareThreadPool(GetDynamicPipelineSpec()->JobTracker->JobControlThreads, "JobControl"))
        , JobThreadPool_(CreateFairShareThreadPool(GetJobThreadPoolSize(GetDynamicPipelineSpec(), Context_->WorkerNodeInfo), "Jobs"))
        , LoadThroughputThrottler_(New<TLoadThroughputThrottler>("yt_load", Logger(), WorkerProfiler()))
        , StateCache_(New<TStateCache>(
            GetDynamicPipelineSpec()->JobTracker->StateCache,
            WorkerProfiler().WithPrefix("/state_cache")))
        , PerformanceCountersUpdater_(New<TPeriodicExecutor>(
            Context_->ControlInvoker,
            BIND(&TJobTracker::UpdatePerformanceCounters, MakeWeak(this)),
            TDuration::Seconds(1)))
        , JobStartCounter_(WorkerProfiler()
                .Counter("/job_start"))
        , HungStoppingJobs_(WorkerProfiler().Gauge("/hung_stopping_jobs"))
    {
        ResourceManager_ = CreateResourceManagerForPipelineSpec(
            ExecutionSpec_->PipelineSpec->GetValue()->Resources,
            ExecutionSpec_->DynamicPipelineSpec->GetValue()->Resources,
            ExecutionSpec_->PipelineSpec->GetValue()->Computations);

        PerformanceCountersUpdater_->Start();
    }

    TPipelineSpecPtr GetPipelineSpec() const
    {
        return ExecutionSpec_->PipelineSpec->GetValue();
    }

    TDynamicPipelineSpecPtr GetDynamicPipelineSpec() const
    {
        return ExecutionSpec_->DynamicPipelineSpec->GetValue();
    }

    void CancelAllJobs(TError error) override
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        TForbidContextSwitchGuard contextSwitchGuard;

        YT_LOG_INFO("Cancelling all jobs (JobCount: %v)",
            JobIdToRuntimeState_.size());

        for (const auto& [jobId, state] : JobIdToRuntimeState_) {
            YT_UNUSED_FUTURE(state.Job->Stop(error));
        }
    }

    TFuture<std::vector<TJobStatusPtr>> GetStatuses() override
    {
        YT_ASSERT_THREAD_AFFINITY(Control);
        UpdateMetrics();

        std::vector<TFuture<TJobStatusPtr>> futures;
        {
            TForbidContextSwitchGuard contextSwitchGuard;
            for (const auto& [jobId, state] : JobIdToRuntimeState_) {
                auto performanceMetrics = state.PerformanceCounters.BuildMetrics();
                futures.push_back(state.Job->GetStatus().Apply(BIND([performanceMetrics] (const TJobStatusPtr& status) {
                    // TODO(gryzlov-ad): Move status filling from Job to JobTracker.
                    status->PerformanceMetrics = performanceMetrics;
                    return status;
                })));
            }
        }

        return AllSucceeded(futures);
    }

    THashMap<TResourceId, TWorkerResourceStatusPtr> GetResourceStatuses() override
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        return ResourceManager_->CollectResourceStatuses();
    }

    THashMap<TResourceId, EPreloadedResourceState> GetPreloadedStates() override
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        return ResourceManager_->GetPreloadedStates();
    }

    void Reconfigure(
        TExecutionSpecPtr newExecutionSpec,
        const THashMap<TJobId, NYTree::IMapNodePtr>& newDynamicComputationPartitionSpecs) override
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        TForbidContextSwitchGuard contextSwitchGuard;

        auto oldExecutionSpec = ExecutionSpec_;
        ExecutionSpec_ = newExecutionSpec;
        auto oldExecutionSpecGeneration = ExecutionSpecGeneration_;
        ExecutionSpecGeneration_ = newExecutionSpec->GetEpoch();
        auto oldDynamicJobSpecs = DynamicJobSpecs_;
        DynamicJobSpecs_ = BuildNewJobSpecs(oldDynamicJobSpecs, newDynamicComputationPartitionSpecs, oldExecutionSpec, ExecutionSpec_);

        if (oldExecutionSpec->DynamicPipelineSpec->GetVersion() != ExecutionSpec_->DynamicPipelineSpec->GetVersion()) {
            const auto& jobTrackerSpec = ExecutionSpec_->DynamicPipelineSpec->GetValue()->JobTracker;
            JobControlThreadPool_->SetThreadCount(jobTrackerSpec->JobControlThreads);
            JobThreadPool_->SetThreadCount(GetJobThreadPoolSize(ExecutionSpec_->DynamicPipelineSpec->GetValue(), Context_->WorkerNodeInfo));
            BufferStateManager_->Reconfigure(jobTrackerSpec->BufferStateManager);
            LoadThroughputThrottler_->Reconfigure(jobTrackerSpec->LoadThroughputThrottler);
            StateCache_->Reconfigure(jobTrackerSpec->StateCache);

            // Reconfigure resources.
            ResourceManager_->Reconfigure(ExecutionSpec_->DynamicPipelineSpec->GetValue()->Resources);
            // Throttlers flow to computations through TDynamicComputationContext;
            // each computation's factory reconfigures itself on ApplyPendingStates.
        }

        // Drop too old jobs.
        if (oldExecutionSpec->PipelineSpec->GetVersion() != ExecutionSpec_->PipelineSpec->GetVersion()) {
            DropAllJobs();
            ResourceManager_ = CreateResourceManagerForPipelineSpec(
                ExecutionSpec_->PipelineSpec->GetValue()->Resources,
                ExecutionSpec_->DynamicPipelineSpec->GetValue()->Resources,
                ExecutionSpec_->PipelineSpec->GetValue()->Computations);
        } else {
            for (const auto& jobId : GetKeys(JobIdToRuntimeState_)) {
                if (!ExecutionSpec_->Layout->Jobs.contains(jobId)) {
                    DropJob(jobId);
                }
            }
        }

        // Update preloaded resources based on WorkerSpecs.
        auto workerSpecIt = ExecutionSpec_->Layout->WorkerSpecs.find(Context_->WorkerNodeInfo->RpcAddress);
        if (workerSpecIt != ExecutionSpec_->Layout->WorkerSpecs.end()) {
            ResourceManager_->UpdatePreloadedResources(workerSpecIt->second->PreloadResources);
        } else {
            ResourceManager_->UpdatePreloadedResources({});
        }

        for (const auto& [jobId, state] : JobIdToRuntimeState_) {
            if (auto newSpecIt = DynamicJobSpecs_.find(jobId); newSpecIt != DynamicJobSpecs_.end()) {
                auto specChanged = [&] {
                    if (auto oldSpecIt = oldDynamicJobSpecs.find(jobId); oldSpecIt != oldDynamicJobSpecs.end()) {
                        return newSpecIt->second != oldSpecIt->second;
                    }
                    return true;
                };
                if (specChanged() || oldExecutionSpecGeneration != ExecutionSpecGeneration_) {
                    YT_UNUSED_FUTURE(state.Job->Reconfigure(ExecutionSpecGeneration_, newSpecIt->second));
                }
            }
        }

        if (oldExecutionSpec->InputStreamsTraverse->GetVersion() != ExecutionSpec_->InputStreamsTraverse->GetVersion()) {
            for (const auto& [jobId, state] : JobIdToRuntimeState_) {
                auto job = GetOrCrash(ExecutionSpec_->Layout->Jobs, jobId);
                auto partitionState = GetOrCrash(ExecutionSpec_->Layout->Partitions, job->PartitionId)->State;
                YT_UNUSED_FUTURE(UpdateTraverseData(state.Job, partitionState));
            }
        }

        if (oldExecutionSpec->WatermarkState->GetVersion() != ExecutionSpec_->WatermarkState->GetVersion()) {
            for (const auto& [jobId, state] : JobIdToRuntimeState_) {
                YT_UNUSED_FUTURE(UpdateWatermarkState(state.Job));
            }
        }

        for (const auto& [jobId, newDynamicComputationPartitionSpec] : newDynamicComputationPartitionSpecs) {
            auto jobIt = ExecutionSpec_->Layout->Jobs.find(jobId);
            if (jobIt == ExecutionSpec_->Layout->Jobs.end() || JobIdToRuntimeState_.contains(jobId)) {
                continue;
            }
            auto job = jobIt->second;
            YT_LOG_FATAL_UNLESS(
                job->WorkerAddress == Context_->WorkerNodeInfo->RpcAddress && job->WorkerIncarnationId == Context_->WorkerNodeInfo->IncarnationId,
                "Worker received invalid dynamic computation partition specs (WrongJobId: %v, ActualIncarnationId: %v, JobAssignedIncarnationId: %v)",
                jobId,
                Context_->WorkerNodeInfo->IncarnationId,
                job->WorkerIncarnationId);
            YT_UNUSED_FUTURE(StartJob(jobId));
        }
    }

    void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) override
    {
        YT_ASSERT_THREAD_AFFINITY(Control);
        MessageTransferingInfo_ = std::move(messageTransferingInfo);
        // Update buffer state manager so new jobs start with warm demand estimates.
        BufferStateManager_->UpdateMessageTransferingInfo(MessageTransferingInfo_);
        for (const auto& [jobId, jobRuntimeState] : JobIdToRuntimeState_) {
            jobRuntimeState.Job->UpdateMessageTransferingInfo(MessageTransferingInfo_);
        }
    }

    void UpdateMetrics()
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        THashMap<TComputationId, ui64> counts;
        for (const auto& [jobId, state] : JobIdToRuntimeState_) {
            counts[state.Job->GetComputationId()] += 1;
        }

        auto now = TInstant::Now();
        i64 hungStoppingJobs = 0;
        for (auto it = JobStopTimes_.begin(); it != JobStopTimes_.end();) {
            const auto& [weakPtr, stopTime] = it->second;
            if (weakPtr.IsExpired()) {
                JobStopTimes_.erase(it++);
                continue;
            }
            if (stopTime != TInstant::Max()) {
                auto lag = now - stopTime;
                if (lag > TDuration::Minutes(1)) {
                    ++hungStoppingJobs;
                    YT_LOG_ERROR("Job can not stop in time (JobId: %v, StoppingLag: %v)", it->first, lag);
                }
            }
            ++it;
        }
        HungStoppingJobs_.Update(hungStoppingJobs);

        for (const auto& [computationId, count] : counts) {
            JobCountPerComputationGauges_.emplace(
                computationId,
                WorkerProfiler()
                    .WithTag("computation_id", computationId.Underlying())
                    .Gauge("/job_count"));
        }

        for (auto& [computationId, gauge] : JobCountPerComputationGauges_) {
            if (auto* count = counts.FindPtr(computationId)) {
                gauge.Update(*count);
            } else {
                gauge.Update(0);
            }
        }
    }

    NYTree::IYPathServicePtr CreateOrchidService() override
    {
        return IYPathService::FromProducer(BIND(&TJobTracker::BuildOrchid, MakeStrong(this)))
            ->Via(Context_->ControlInvoker);
    }

    ~TJobTracker() override
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        DropAllJobs();
    }

private:
    // TODO(gryzlov-ad): Add inner invoker for periodic job CPU/memory monitoring and buffer management.
    const TJobTrackerContextPtr Context_;
    TExecutionSpecPtr ExecutionSpec_;
    i64 ExecutionSpecGeneration_ = -1;
    THashMap<TJobId, TDynamicJobSpecPtr> DynamicJobSpecs_;

    const IBufferStateManagerPtr BufferStateManager_;
    const IColumnEvaluatorCachePtr EvaluatorCache_;
    const IFairShareThreadPoolPtr JobControlThreadPool_;
    const IFairShareThreadPoolPtr JobThreadPool_;
    const TLoadThroughputThrottlerPtr LoadThroughputThrottler_;
    const TStateCachePtr StateCache_;

    IResourceManagerPtr ResourceManager_;

    TMessageTransferingInfoPtr MessageTransferingInfo_;

    class TJobPerformanceCounters
    {
    public:
        explicit TJobPerformanceCounters(const NProfiling::TProfiler& profiler)
            : Profiler_(profiler)
            , CpuTimeCounter_(profiler.TimeCounter("/cpu_time"))
            , MemoryUsageGauge_(profiler.Gauge("/memory_usage"))
        { }

        void Update(TDuration cpuTime, size_t memoryUsage)
        {
            auto now = TInstant::Now();
            CpuTimeEmaCounter_.Update(cpuTime.SecondsFloat(), now);
            MemoryUsageCurrent_ = memoryUsage;
            MemoryUsage30s_.UpdateAt(now, memoryUsage);
            MemoryUsage10m_.UpdateAt(now, memoryUsage);

            CpuTimeCounter_.Add(std::max(cpuTime, TotalCpuTime_) - TotalCpuTime_);
            TotalCpuTime_ = cpuTime;
            MemoryUsageGauge_.Update(memoryUsage);
        }

        TNodePerformanceMetricsPtr BuildMetrics() const
        {
            auto metrics = New<TNodePerformanceMetrics>();
            metrics->CpuUsageCurrent = CpuTimeEmaCounter_.ImmediateRate;
            metrics->CpuUsage30s = CpuTimeEmaCounter_.GetRate(0);
            metrics->CpuUsage10m = CpuTimeEmaCounter_.GetRate(1);
            metrics->MemoryUsageCurrent = MemoryUsageCurrent_;
            metrics->MemoryUsage30s = MemoryUsage30s_.GetAverage();
            metrics->MemoryUsage10m = MemoryUsage10m_.GetAverage();
            return metrics;
        }

    private:
        static constexpr int TimeWindowsCount = 2;
        TEmaCounter<double, TimeWindowsCount> CpuTimeEmaCounter_{{
            TDuration::Seconds(30),
            TDuration::Minutes(10),
        }};
        size_t MemoryUsageCurrent_ = 0;
        TAdjustedExponentialMovingAverage MemoryUsage10m_{TDuration::Minutes(10)};
        TAdjustedExponentialMovingAverage MemoryUsage30s_{TDuration::Seconds(30)};

        const NProfiling::TProfiler Profiler_;
        TDuration TotalCpuTime_ = TDuration::Zero();
        NProfiling::TTimeCounter CpuTimeCounter_;
        NProfiling::TGauge MemoryUsageGauge_;
    };

    struct TJobRuntimeState
    {
        IJobPtr Job;
        TJobPerformanceCounters PerformanceCounters;
        TJobCpuTimeAccountantPtr CpuTimeAccountant;
        TExternalPerformanceMetricsReporterPtr ExternalMetricsReporter;
    };

    THashMap<TJobId, TJobRuntimeState> JobIdToRuntimeState_;
    THashMap<TJobId, std::pair<TWeakPtr<IJob>, TInstant>> JobStopTimes_; // To track hung jobs.
    TPeriodicExecutorPtr PerformanceCountersUpdater_;

    THashMap<TComputationId, NProfiling::TGauge> JobCountPerComputationGauges_;
    NProfiling::TCounter JobStartCounter_;
    NProfiling::TGauge HungStoppingJobs_;

    DECLARE_THREAD_AFFINITY_SLOT(Control);

    IResourceManagerPtr CreateResourceManagerForPipelineSpec(
        const THashMap<TResourceId, TResourceSpecPtr>& resources,
        const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicResourceSpecs,
        const THashMap<TComputationId, TComputationSpecPtr>& computations)
    {
        auto context = New<TResourceManagerContext>();
        context->PipelineAuthenticator = Context_->PipelineAuthenticator;
        context->Logger = WorkerLogger().WithTag("ResourceManager");
        context->Invoker = JobThreadPool_->GetInvoker("ResourceManager");
        context->Profiler = WorkerProfiler();
        context->StatusProfiler = Context_->StatusProfiler->WithPrefix("/resource_manager");
        context->IsController = false;
        context->Computations = computations;
        return CreateResourceManager(std::move(context), resources, dynamicResourceSpecs);
    }

    void UpdatePerformanceCounters()
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        auto snapshot = NYTProf::GetGlobalMemoryUsageSnapshot();
        for (auto& [jobId, state] : JobIdToRuntimeState_) {
            // A sum memory usage for particular job inside main Flow process and corresponding companion process.
            auto memoryUsage = snapshot->GetUsage(JobIdTag, ToString(jobId)) + state.ExternalMetricsReporter->GetMemoryUsage();
            // The maximum node CPU load is generally same in terms of VCPU load, but might be very different in CPU load.
            // Thus, to balance the load evenly on a diverse set of workers, we should recalculate to VCPU in performance counters.
            auto cpuTime = (state.CpuTimeAccountant->GetCpuTime() + state.ExternalMetricsReporter->GetElapsedCpuTime()) * Context_->WorkerNodeInfo->VcpuFactor.value_or(1);
            state.PerformanceCounters.Update(cpuTime, memoryUsage);
        }
    }

    TFuture<void> StartJob(const TJobId& jobId)
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        YT_VERIFY(!JobIdToRuntimeState_.contains(jobId));

        const auto& job = GetOrCrash(ExecutionSpec_->Layout->Jobs, jobId);
        const auto& partition = GetOrCrash(ExecutionSpec_->Layout->Partitions, job->PartitionId);

        YT_LOG_INFO("Executing start job (JobId: %v, PartitionId: %v, ComputationId: %v)",
            jobId,
            partition->PartitionId,
            partition->ComputationId);

        auto jobSpec = BuildJobSpec(jobId);

        auto jobContext = New<TJobContext>();
        jobContext->WorkerAddress = Context_->WorkerNodeInfo->RpcAddress;
        jobContext->ClientsCache = Context_->ClientsCache;
        jobContext->PipelinePath = Context_->PipelinePath;
        jobContext->EvaluatorCache = EvaluatorCache_;
        jobContext->ConverterCache = CreatePayloadConverterCache(EvaluatorCache_);
        jobContext->MessageDistributor = Context_->MessageDistributor;
        jobContext->StreamSpecStorage = Context_
            ->StreamSpecStorage
            ->GetComputationStreamSpecStorage(partition->ComputationId);

        auto jobIdString = ToString(jobId);
        auto traceContext = TTraceContext::NewRoot(Format("Job-%v", jobIdString));
        traceContext->SetAllocationTag(JobIdTag, jobIdString);
        auto loggingTag = Format("JobPropagatingTag: %v/%v", partition->ComputationId, partition->PartitionId);
        traceContext->SetLoggingTag(loggingTag);

        auto cpuTimeAccountant = New<TJobCpuTimeAccountant>();
        auto wrapInvoker = [&] (const auto& underlying) {
            auto tracedInvoker = CreateTracedInvoker(underlying, traceContext, cpuTimeAccountant);
            auto codicil = Format("ComputationId: %v, PartitionId: %v, JobId: %v, %v",
                partition->ComputationId,
                partition->PartitionId,
                jobId,
                loggingTag);
            auto codicilInvoker = CreateCodicilGuardedInvoker(tracedInvoker, codicil);
            return codicilInvoker;
        };

        jobContext->ControlSerializedInvoker = wrapInvoker(CreateSerializedInvoker(JobControlThreadPool_->GetInvoker("JobControl"), "JobControl"));
        jobContext->PoolInvoker = wrapInvoker(JobThreadPool_->GetInvoker(Format("Computation-%v", jobSpec->Partition->ComputationId)));
        jobContext->SerializedInvoker = CreateSerializedInvoker(jobContext->PoolInvoker, "Job");

        jobContext->ClockClusterTag = Context_->ClockClusterTag;
        jobContext->ResourceManager = ResourceManager_;

        auto streamLimitUsageStates = BufferStateManager_->RegisterJob(jobId, jobSpec);
        jobContext->LoadThroughputThrottler = LoadThroughputThrottler_;
        jobContext->JobStateCache = StateCache_->WithJob(
            jobId,
            WorkerProfiler()
                .WithTag("computation_id", jobSpec->Partition->ComputationId.Underlying())
                .WithPrefix("/computation")
                .WithPrefix("/job_state_cache"));

        auto externalMetricsReporter = New<TExternalPerformanceMetricsReporter>();
        jobContext->ExternalMetricsReporter = externalMetricsReporter;

        jobContext->HttpClient = Context_->HttpClient;
        jobContext->HttpsClient = Context_->HttpsClient;
        jobContext->Poller = Context_->Poller;
        jobContext->DistributedThrottlerControllerChannelProvider = Context_->DistributedThrottlerChannel;

        auto dynamicJobSpec = GetOrCrash(DynamicJobSpecs_, jobId);

        auto traverseData = BuildTraverseData(partition->ComputationId, partition->State);
        auto watermarkState = BuildWatermarkState(partition->ComputationId);

        auto preparedJob = [&] () -> IJobPtr {
            try {
                return CreateJob(
                    jobContext,
                    streamLimitUsageStates,
                    ExecutionSpec_->GetEpoch(),
                    jobSpec,
                    dynamicJobSpec,
                    traverseData,
                    watermarkState);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Job creation failed");
                return New<TFailedJob>(jobId, jobSpec->Partition->ComputationId, TError(ex));
            }
        }();

        preparedJob->UpdateMessageTransferingInfo(MessageTransferingInfo_);
        Context_->InputManager->AddJob(jobId, preparedJob->GetInputBuffer());

        EmplaceOrCrash(
            JobIdToRuntimeState_,
            jobId,
            TJobRuntimeState{
                .Job = preparedJob,
                .PerformanceCounters = TJobPerformanceCounters{
                    WorkerProfiler()
                        .WithTag("computation_id", jobSpec->Partition->ComputationId.Underlying())
                        .WithPrefix("/computation")},
                .CpuTimeAccountant = std::move(cpuTimeAccountant),
                .ExternalMetricsReporter = externalMetricsReporter,
            });
        EmplaceOrCrash(JobStopTimes_, jobId, std::pair{MakeWeak(preparedJob), TInstant::Max()});
        JobStartCounter_.Increment();
        return preparedJob->Start();
    }

    void DropAllJobs()
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        TForbidContextSwitchGuard contextSwitchGuard;

        for (const auto& jobId : GetKeys(JobIdToRuntimeState_)) {
            DropJob(jobId);
        }
    }

    void DropJob(const TJobId& jobId)
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        YT_LOG_INFO("Executing drop job (JobId: %v)",
            jobId);

        if (auto it = JobIdToRuntimeState_.find(jobId); it != JobIdToRuntimeState_.end()) {
            auto job = it->second.Job;
            JobIdToRuntimeState_.erase(it);
            GetOrCrash(JobStopTimes_, jobId).second = TInstant::Now();
            YT_UNUSED_FUTURE(job->Stop());
            Context_->InputManager->RemoveJob(jobId);
            BufferStateManager_->RemoveJob(jobId);
        } else {
            YT_LOG_WARNING("Unknown job (JobId: %v)",
                jobId);
        }
    }

    TFuture<void> UpdateTraverseData(const IJobPtr& job, EPartitionState state)
    {
        return job->UpdateTraverseData(BuildTraverseData(job->GetComputationId(), state));
    }

    TFuture<void> UpdateWatermarkState(const IJobPtr& job)
    {
        return job->UpdateWatermarkState(BuildWatermarkState(job->GetComputationId()));
    }

    TJobSpecPtr BuildJobSpec(const TJobId& jobId) const
    {
        const auto& job = GetOrCrash(ExecutionSpec_->Layout->Jobs, jobId);
        const auto& partition = GetOrCrash(ExecutionSpec_->Layout->Partitions, job->PartitionId);
        auto jobSpec = New<TJobSpec>();
        jobSpec->ComputationSpec = GetOrCrash(ExecutionSpec_->PipelineSpec->GetValue()->Computations, partition->ComputationId);
        jobSpec->ExtendedComputationSpec = GetOrCrash(ExecutionSpec_->ExtendedPipelineSpec->GetValue()->Computations, partition->ComputationId);
        jobSpec->Job = job;
        jobSpec->Partition = partition;
        return jobSpec;
    }

    static THashMap<TJobId, TDynamicJobSpecPtr> BuildNewJobSpecs(
        const THashMap<TJobId, TDynamicJobSpecPtr>& oldDynamicJobSpecs,
        const THashMap<TJobId, NYTree::IMapNodePtr>& newDynamicComputationPartitionSpecs,
        const TExecutionSpecPtr& oldExecutionSpec,
        const TExecutionSpecPtr& newExecutionSpec)
    {
        YT_VERIFY(newExecutionSpec);
        THashMap<TJobId, TDynamicJobSpecPtr> result;
        for (const auto& [jobId, newDynamicComputationPartitionSpec] : newDynamicComputationPartitionSpecs) {
            const auto layoutJobIt = newExecutionSpec->Layout->Jobs.find(jobId);
            if (layoutJobIt == newExecutionSpec->Layout->Jobs.end()) {
                continue;
            }
            const auto& partition = GetOrCrash(newExecutionSpec->Layout->Partitions, layoutJobIt->second->PartitionId);

            auto oldDynamicJobSpec = GetOrDefault(oldDynamicJobSpecs, jobId);

            auto newDynamicJobSpec = New<TDynamicJobSpec>();

            if (oldDynamicJobSpec && oldExecutionSpec && oldExecutionSpec->DynamicPipelineSpec->GetVersion() == newExecutionSpec->DynamicPipelineSpec->GetVersion()) {
                newDynamicJobSpec->DynamicComputationSpec = oldDynamicJobSpec->DynamicComputationSpec;
            } else {
                newDynamicJobSpec->DynamicComputationSpec = NYTree::CloneYsonStruct(GetOrDefault(
                    newExecutionSpec->DynamicPipelineSpec->GetValue()->Computations,
                    partition->ComputationId,
                    New<TDynamicComputationSpec>()));
                const auto& state = newExecutionSpec->PipelineState->GetValue();
                if (state == EPipelineState::Stopped || state == EPipelineState::Draining) {
                    newDynamicJobSpec->DynamicComputationSpec->Draining = true;
                }
            }

            if (oldDynamicJobSpec && AreNodesEqual(oldDynamicJobSpec->DynamicComputationPartitionSpec, newDynamicComputationPartitionSpec)) {
                newDynamicJobSpec->DynamicComputationPartitionSpec = oldDynamicJobSpec->DynamicComputationPartitionSpec;
            } else {
                newDynamicJobSpec->DynamicComputationPartitionSpec = newDynamicComputationPartitionSpec;
            }

            // Pointer-copy so the factory can diff specs by identity.
            newDynamicJobSpec->Throttlers = newExecutionSpec->DynamicPipelineSpec->GetValue()->Throttlers;

            if (oldDynamicJobSpec &&
                oldDynamicJobSpec->DynamicComputationPartitionSpec == newDynamicJobSpec->DynamicComputationPartitionSpec &&
                oldDynamicJobSpec->DynamicComputationSpec == newDynamicJobSpec->DynamicComputationSpec &&
                oldDynamicJobSpec->Throttlers == newDynamicJobSpec->Throttlers)
            {
                newDynamicJobSpec = oldDynamicJobSpec;
            }

            result[jobId] = newDynamicJobSpec;
        }
        return result;
    }

    TToPartitionTraverseDataPtr BuildTraverseData(const TComputationId& computationId, EPartitionState state) const
    {
        auto traverse = New<TToPartitionTraverseData>();
        for (const auto& streamId : GetOrCrash(ExecutionSpec_->PipelineSpec->GetValue()->Computations, computationId)->InputStreamIds) {
            if (state == EPartitionState::Executing || state == EPartitionState::Completing) {
                traverse->InputStreams[streamId] = GetOrCrash(ExecutionSpec_->InputStreamsTraverse->GetValue(), streamId);
            } else if (state == EPartitionState::Interrupting) {
                auto streamTraverse = GetOrCrash(ExecutionSpec_->InputStreamsTraverse->GetValue(), streamId);
                auto streamSpec = GetOrCrash(ExecutionSpec_->PipelineSpec->GetValue()->Streams, streamId);
                traverse->InputStreams[streamId] = MakeCompletedStreamTraverseData(
                    streamTraverse->Epoch,
                    streamTraverse->SystemWatermark);
            } else {
                YT_LOG_FATAL("Unexpected partition state (PartitionState: %v)", state);
            }
        }
        return traverse;
    }

    TWatermarkStatePtr BuildWatermarkState(const TComputationId& /*computationId*/) const
    {
        return ExecutionSpec_->WatermarkState->GetValue();
    }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        YT_ASSERT_THREAD_AFFINITY(Control);

        THashMap<TJobId, TFuture<TJobOrchidStatePtr>> jobOrchidStates;
        for (const auto& [jobId, state] : JobIdToRuntimeState_) {
            jobOrchidStates[jobId] = state.Job->GetOrchidState();
        }
        for (const auto& [jobId, stateFuture] : jobOrchidStates) {
            Y_UNUSED(WaitForFast(stateFuture));
        }

        // clang-format off
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("jobs").DoMapFor(jobOrchidStates, [] (auto fluent, const auto& jobIdWithState) {
                    const auto& [jobId, stateFuture] = jobIdWithState;
                    if (stateFuture.GetOrCrash().IsOK()) {
                        fluent.Item(ToString(jobId)).Value(stateFuture.GetOrCrash().Value());
                    }
                })
            .EndMap();
        // clang-format on
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobTrackerPtr CreateJobTracker(TJobTrackerContextPtr context)
{
    return New<TJobTracker>(std::move(context));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
