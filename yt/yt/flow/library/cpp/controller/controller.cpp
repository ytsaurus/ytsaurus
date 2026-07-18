#include "controller.h"

#include "private.h"

#include "config.h"
#include "job_manager.h"
#include "lease_manager.h"
#include "persisted_state_manager.h"
#include "throttler_host.h"
#include "worker.h"
#include "worker_tracker.h"
#include "yt_connector.h"

#include <yt/yt/flow/library/cpp/client/public.h>

#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/build/build.h>

#include <library/cpp/iterator/concatenate.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/datetime/base.h>

namespace NYT::NFlow::NController {

using namespace NProfiling;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using NTransactionClient::ETransactionType;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

// FuncGauge-based lag gauge: computes lag as (Now() - LastWatermark) at read time.
// Unlike TGauge, continues growing even if Update() stops being called (e.g. controller is stuck).
// Returns -1 if no watermark has been set yet.
class TLagGauge
    : public TRefCounted
{
public:
    static TIntrusivePtr<TLagGauge> Create(
        const TProfiler& profiler,
        const std::string& name)
    {
        auto gauge = New<TLagGauge>();
        profiler.AddFuncGauge(name, gauge, [gauge = gauge.Get()] () -> double {
            auto watermark = TSystemTimestamp(gauge->Watermark_.load(std::memory_order::relaxed));
            if (watermark == ZeroSystemTimestamp) {
                return -1.0;
            }
            auto now = static_cast<i64>(TInstant::Now().Seconds());
            return static_cast<double>(std::max<i64>(0, now - static_cast<i64>(watermark.Underlying())));
        });
        return gauge;
    }

    void Update(TSystemTimestamp watermark)
    {
        Watermark_.store(watermark.Underlying(), std::memory_order::relaxed);
    }

private:
    std::atomic<ui64> Watermark_{ZeroSystemTimestamp.Underlying()};
};

using TLagGaugePtr = TIntrusivePtr<TLagGauge>;

////////////////////////////////////////////////////////////////////////////////

TFlowEphemeralStatePtr ComputeInitialEphemeralState(
    const TFlowStatePtr& state,
    const TVersionedDynamicPipelineSpecPtr& dynamicPipelineSpec,
    const NYPath::TRichYPath& pipelinePath)
{
    bool missingJobsAreStopped =
        state->ExecutionSpec->PipelineState->GetValue() == EPipelineState::Stopped ||
        dynamicPipelineSpec->GetValue()->TargetState == EPipelineState::Paused;
    auto ephemeralState = New<TFlowEphemeralState>();
    for (const auto& [partitionId, partition] : state->ExecutionSpec->Layout->Partitions) {
        if (!(partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting)) {
            continue;
        }
        if (!partition->CurrentJobId.has_value() && missingJobsAreStopped) {
            auto& partitionState = ephemeralState->Partitions[partitionId];
            partitionState = New<TPartitionEphemeralState>();
            partitionState->PreviousJobFinishReason = EJobFinishReason::Stopped;
        }
    }
    for (const auto& [jobId, job] : state->ExecutionSpec->Layout->Jobs) {
        ephemeralState->WorkerIncarnationsJobs[job->WorkerIncarnationId].insert(jobId);
    }
    ephemeralState->PipelinePath = pipelinePath;
    return ephemeralState;
}

////////////////////////////////////////////////////////////////////////////////

class TControllerLeader
    : public TRefCounted
{
public:
    class TStreamMetrics
    {
    public:
        TStreamMetrics(const TStreamId& streamId, const TProfiler& profiler)
            : Profiler_(profiler
                    .WithPrefix("/streams")
                    .WithRequiredTag("stream_id", streamId.Underlying()))
            , TimeLagGauge_(TLagGauge::Create(Profiler_, "/time_lag"))
            , EventTimeLagGauge_(TLagGauge::Create(Profiler_, "/event_time_lag"))
            , LagGauge_(Profiler_.Gauge("/count_lag"))
            , ByteSizeLagGauge_()
        { }

        void Apply(const TSystemTimestamp /*now*/, const TStreamTraverseDataPtr& stream)
        {
            TimeLagGauge_->Update(stream->SystemWatermark);
            EventTimeLagGauge_->Update(stream->EventWatermark);

            const auto& metrics = stream->InflightMetrics;
            LagGauge_.Update(metrics->Count);

            auto update = [&] (auto& gauge, const std::string& name, auto value) {
                if (value) {
                    if (!gauge) {
                        gauge = Profiler_.Gauge(name);
                    }
                    gauge->Update(*value);
                } else {
                    gauge = {};
                }
            };

            update(ByteSizeLagGauge_, "/byte_size_lag", metrics->ByteSize);
        }

    private:
        TProfiler Profiler_;

        TLagGaugePtr TimeLagGauge_;
        TLagGaugePtr EventTimeLagGauge_;
        TGauge LagGauge_;
        std::optional<TGauge> ByteSizeLagGauge_;
    };

    struct TAggregatedStreamTraverseData
    {
        int Count = 0;
        TStreamTraverseDataPtr Total;
        TStreamTraverseDataPtr Max;
    };

    class TComputationMetrics
    {
    private:
        struct TStreamMetrics
        {
            TProfiler Profiler;
            TStreamId StreamId;

            TProfiler LocalProfiler = Profiler.WithRequiredTag("stream_id", StreamId.Underlying());

            TGauge InputMessagesPerSecondTotal = LocalProfiler.Gauge("/total_messages_per_second");
            TGauge PartitionInputMessagesPerSecondMax = LocalProfiler.Gauge("/partition_messages_per_second.max");
            TGauge PartitionInputMessagesPerSecondAvg = LocalProfiler.Gauge("/partition_messages_per_second.avg");
            TGauge PartitionInputBytesPerSecondMax = LocalProfiler.Gauge("/partition_bytes_per_second.max");
            TGauge PartitionInputBytesPerSecondAvg = LocalProfiler.Gauge("/partition_bytes_per_second.avg");

            TGauge SystemWatermarkMinMaxDifference = LocalProfiler.WithDefaultDisabled().Gauge("/partition_system_watermark_min_max_difference");
            TGauge EventWatermarkMinMaxDifference = LocalProfiler.WithDefaultDisabled().Gauge("/partition_event_watermark_min_max_difference");
        };

    public:
        TComputationMetrics(const TProfiler& profiler, const TComputationId& computationId)
            : Profiler_(profiler
                    .WithPrefix("/computations")
                    .WithRequiredTag("computation_id", computationId.Underlying()))
            , CpuUsageTotalGauge_(Profiler_.Gauge("/cpu_usage"))
            , MemoryUsageTotalGauge_(Profiler_.Gauge("/memory_usage"))
            , PartitionCpuUsageMaxGauge_(Profiler_.Gauge("/partition_cpu_usage.max"))
            , PartitionMemoryUsageMaxGauge_(Profiler_.Gauge("/partition_memory_usage.max"))
            , PartitionCpuUsageAvgGauge_(Profiler_.Gauge("/partition_cpu_usage.avg"))
            , PartitionMemoryUsageAvgGauge_(Profiler_.Gauge("/partition_memory_usage.avg"))
            , StreamMetrics_()
        { }

        void Apply(const TAggregatedNodePerformanceMetricsPtr& metrics)
        {
            CpuUsageTotalGauge_.Update(metrics->Total->CpuUsageCurrent.value_or(0));
            MemoryUsageTotalGauge_.Update(metrics->Total->MemoryUsageCurrent);

            // CpuUsageCurrent/MemoryUsageCurrent is bad metric for taking maximum.
            PartitionCpuUsageMaxGauge_.Update(metrics->Max->CpuUsage30s.value_or(0));
            // Per-partition memory metric is very noisy, so use 10-min smoothed metric.
            PartitionMemoryUsageMaxGauge_.Update(metrics->Max->MemoryUsage10m);

            PartitionCpuUsageAvgGauge_.Update(metrics->Avg->CpuUsageCurrent.value_or(0));
            PartitionMemoryUsageAvgGauge_.Update(metrics->Avg->MemoryUsageCurrent);
        }

        void Apply(
            const TAggregatedNodeInputMetricsPtr& inputMetrics,
            const THashMap<TStreamId, TAggregatedStreamTraverseData>& aggregatedStreamTraverseDatas,
            const THashMap<TStreamId, TStreamTraverseDataMetricsPtr>& streamsTraverseDataMetrics,
            const TComputationSpecPtr& computationSpec)
        {
            THashSet<TStreamId> streamIds;
            for (const auto& [streamId, stream] : inputMetrics->Streams) {
                if (!computationSpec->InputStreamIds.contains(streamId)) {
                    continue;
                }
                streamIds.insert(streamId);
                auto streamMetrics = StreamMetrics_.try_emplace(streamId, Profiler_, streamId).first->second;
                streamMetrics.InputMessagesPerSecondTotal.Update(stream->Total.MessagesPerSecond);
                streamMetrics.PartitionInputMessagesPerSecondMax.Update(stream->Max.MessagesPerSecond);
                streamMetrics.PartitionInputMessagesPerSecondAvg.Update(stream->Avg.MessagesPerSecond);
                streamMetrics.PartitionInputBytesPerSecondMax.Update(stream->Max.BytesPerSecond);
                streamMetrics.PartitionInputBytesPerSecondAvg.Update(stream->Avg.BytesPerSecond);
            }

            for (const auto& [streamId, aggregatedData] : aggregatedStreamTraverseDatas) {
                if (computationSpec->InputStreamIds.contains(streamId)) {
                    continue;
                }
                streamIds.insert(streamId);
                auto streamMetrics = StreamMetrics_.try_emplace(streamId, Profiler_, streamId).first->second;
                const auto totalInflightMetrics = aggregatedData.Total->InflightMetrics;
                const auto maxInflightMetrics = aggregatedData.Max->InflightMetrics;
                streamMetrics.InputMessagesPerSecondTotal.Update(totalInflightMetrics->ProcessedCountPerSec.value_or(0.0));
                streamMetrics.PartitionInputMessagesPerSecondMax.Update(maxInflightMetrics->ProcessedCountPerSec.value_or(0.0));
                streamMetrics.PartitionInputMessagesPerSecondAvg.Update(totalInflightMetrics->ProcessedCountPerSec.value_or(0.0) / aggregatedData.Count);
                streamMetrics.PartitionInputBytesPerSecondMax.Update(maxInflightMetrics->ProcessedBytesPerSec.value_or(0.0));
                streamMetrics.PartitionInputBytesPerSecondAvg.Update(totalInflightMetrics->ProcessedBytesPerSec.value_or(0.0) / aggregatedData.Count);

                auto streamTraverseDataMetricsIt = streamsTraverseDataMetrics.find(streamId);
                if (streamTraverseDataMetricsIt == streamsTraverseDataMetrics.end()) {
                    continue;
                }
                auto& streamTraverseDataMetrics = streamTraverseDataMetricsIt->second;
                streamMetrics.SystemWatermarkMinMaxDifference.Update(streamTraverseDataMetrics->SystemWatermarkMinMaxDifference);
                streamMetrics.EventWatermarkMinMaxDifference.Update(streamTraverseDataMetrics->EventWatermarkMinMaxDifference);
            }
            DropMissingKeys(StreamMetrics_, streamIds);
        }

    private:
        TProfiler Profiler_;

        TGauge CpuUsageTotalGauge_;
        TGauge MemoryUsageTotalGauge_;
        TGauge PartitionCpuUsageMaxGauge_;
        TGauge PartitionMemoryUsageMaxGauge_;
        TGauge PartitionCpuUsageAvgGauge_;
        TGauge PartitionMemoryUsageAvgGauge_;
        THashMap<TStreamId, TStreamMetrics> StreamMetrics_;
    };

    struct TMutationMetrics
    {
        TMutationMetrics(const TProfiler& profiler)
            : Profiler(profiler.WithPrefix("/mutations"))
            , CreatePartitionCounter(Profiler.Counter("/create_partition"))
            , UpdatePartitionStateCounter(Profiler.Counter("/update_partition_state"))
            , RemovePartitionCounter(Profiler.Counter("/remove_partition"))
            , CreateJobCounter(Profiler.Counter("/create_job"))
            , RemoveJobCounter(Profiler.Counter("/remove_job"))
            , UpdateJobLeaseCounter(Profiler.Counter("/update_job_lease"))
        { }

        TProfiler Profiler;

        TCounter CreatePartitionCounter;
        TCounter UpdatePartitionStateCounter;
        TCounter RemovePartitionCounter;
        TCounter CreateJobCounter;
        TCounter RemoveJobCounter;
        TCounter UpdateJobLeaseCounter;
    };

    struct TComputationJobStatusMetrics
    {
        struct TReasonMetrics
        {
            TProfiler Profiler;

            TGauge Unknown = Profiler.Gauge("/unknown");
            TGauge Preparing = Profiler.Gauge("/preparing");
            TGauge WorkingYoung = Profiler.Gauge("/working_young");
            TGauge WorkingOld = Profiler.Gauge("/working_old");
            TGauge WorkingWithRetryableError = Profiler.Gauge("/working_with_retryable_error");
            TGauge Stopped = Profiler.Gauge("/stopped");
        };

        TEnumIndexedArray<EJobFinishReason, TReasonMetrics> ReasonMetrics;

        TComputationJobStatusMetrics(const TProfiler& profiler = {}, const TComputationId& computationId = {})
        {
            auto localProfiler = profiler.WithPrefix("/job_status").WithTag("computation_id", computationId.Underlying());
            for (auto reason : TEnumTraits<EJobFinishReason>::GetDomainValues()) {
                ReasonMetrics[reason] = TReasonMetrics{localProfiler.WithTag("previous_job_finish_reason", ToString(reason))};
            }
        }
    };

    struct TComputationPartitionCountMetrics
    {
        TEnumIndexedArray<EPartitionState, TGauge> Counts;

        TComputationPartitionCountMetrics(const TProfiler& profiler = {}, const TComputationId& computationId = {})
        {
            auto localProfiler = profiler.WithTag("computation_id", computationId.Underlying());
            for (auto state : TEnumTraits<EPartitionState>::GetDomainValues()) {
                Counts[state] = localProfiler.WithTag("state", ToString(state)).Gauge("/partition_count");
            }
        }
    };

    struct TComputationFinishedPartitionMetrics
    {
        TProfiler Profiler;
        TComputationId ComputationId;

        TProfiler LocalProfiler = Profiler.WithTag("computation_id", ComputationId.Underlying());
        TCounter CompletedPartitionCount = LocalProfiler.WithTag("state", ToString(EPartitionState::Completed)).Counter("/finished_partitions");
        TCounter InterruptedPartitionCount = LocalProfiler.WithTag("state", ToString(EPartitionState::Interrupted)).Counter("/finished_partitions");
    };

    struct TPipelineStateMetrics
    {
        EPipelineState State = EPipelineState::Unknown;
        TGauge StateGauge = {};

        void UpdateState(const TProfiler& profiler, EPipelineState state)
        {
            if (StateGauge && state == State) {
                return;
            }
            State = state;
            StateGauge = profiler.WithRequiredTag("state", ToString(State)).Gauge("/pipeline_state");
            StateGauge.Update(1);
        }
    };

    struct TComputationStreamAlignmentTimestampBiasMetrics
    {
        TProfiler Profiler;
        TComputationId ComputationId;
        TStreamId StreamId;

        TGauge AlignmentTimestampBias = Profiler
            .WithTag("computation_id", ComputationId.Underlying())
            .WithTag("stream_id", StreamId.Underlying())
            .Gauge("/alignment_timestamp_bias");
    };

    TControllerLeader(
        const NProfiling::TProfiler& profiler,
        IYTConnectorPtr connector,
        IPipelineAuthenticatorPtr authenticator,
        TControllerConfigPtr config,
        TNodeInfoPtr nodeInfo,
        const IInvokerPtr& invoker,
        const IInvokerPtr& mainCycleInvoker,
        IThrottlerHostPtr throttlerHost,
        NObjectClient::TCellTag clockClusterTag,
        IStatusProfilerPtr statusProfiler)
        : Connector_(std::move(connector))
        , PipelineAuthenticator_(std::move(authenticator))
        , TimeProvider_(CreateRetryingTimeProvider(Connector_->GetClient(), clockClusterTag, invoker, statusProfiler, ControllerLogger()))
        , Config_(std::move(config))
        , NodeInfo_(std::move(nodeInfo))
        , Profiler_(profiler)
        , Invoker_(invoker)
        , MainCycleInvoker_(mainCycleInvoker)
        , ThrottlerHost_(std::move(throttlerHost))
        , LeaseManager_(CreateLeaseManager(Connector_, Config_->LeaseManager))
        , MutationMetrics_(Profiler_)
        , CurrentEpochGauge_(Profiler_.Gauge("/current_epoch"))
        , ComputationCountGauge_(Profiler_.Gauge("/computation_count"))
        , WorkerCountGauges_()
        , FlowCoreTargetMismatchWorkerCountGauge_(Profiler_.Gauge("/version_mismatch_worker_count"))
        , SpecVersion_(Profiler_.Gauge("/spec_version"))
        , DynamicSpecVersion_(Profiler_.Gauge("/dynamic_spec_version"))
        , StatusProfiler_(std::move(statusProfiler))
    { }

    void SyncWorkers(const TFlowViewPtr& flowView, const std::vector<TWorkerInfo>& workers)
    {
        THashMap<EWorkerState, ui64> counts;
        flowView->State->Workers.clear();
        flowView->EphemeralState->FlowCoreTargetMismatchedWorkers.clear();
        THashSet<TIncarnationId> incarnations;

        const auto& flowCoreTarget = flowView->State->ExecutionSpec->FlowCoreTarget;
        ui64 flowCoreTargetMismatchCount = 0;

        for (const auto& w : workers) {
            if (w.State == EWorkerState::Registered) {
                if (!flowCoreTarget->GetValue().Underlying().empty() && flowCoreTarget->GetValue().Underlying() != w.FlowCoreVersion)
                {
                    flowCoreTargetMismatchCount += 1;
                    auto& group = flowView->EphemeralState->FlowCoreTargetMismatchedWorkers[w.FlowCoreVersion];
                    if (group.Count == 0) {
                        group.ExampleAddress = w.RpcAddress;
                    }
                    group.Count += 1;
                } else {
                    auto worker = New<NFlow::TWorker>();
                    static_cast<TNodeInfoBase&>(*worker) = static_cast<const TNodeInfoBase&>(w);
                    worker->Groups = w.Groups;
                    worker->Capabilities = w.Capabilities;
                    worker->RegisterTime = w.RegisterTime;
                    worker->LegacyAddress = worker->RpcAddress;
                    flowView->State->Workers[worker->RpcAddress] = worker;
                    incarnations.insert(w.IncarnationId);
                }
            }
            counts[w.State] += 1;
        }
        DropMissingKeys(flowView->EphemeralState->WorkerIncarnationsJobs, incarnations);
        for (const auto& [state, count] : counts) {
            if (!WorkerCountGauges_.contains(state)) {
                WorkerCountGauges_[state] = Profiler_.WithTag("state", ToString(state)).Gauge("/worker_count");
            }
            auto& gauge = GetOrCrash(WorkerCountGauges_, state);
            gauge.Update(count);
        }
        DropMissingKeys(WorkerCountGauges_, GetKeySet(counts));

        FlowCoreTargetMismatchWorkerCountGauge_.Update(flowCoreTargetMismatchCount);

        YT_TLOG_INFO("SyncWorkers completed")
            .With("TotalRegistered", counts.contains(EWorkerState::Registered) ? counts[EWorkerState::Registered] : 0)
            .With("FlowCoreTargetMismatched", flowCoreTargetMismatchCount)
            .With("Active", flowView->State->Workers.size());
    }

    void DoIteration(const TFlowViewPtr& flowView)
    {
        YT_ASSERT_INVOKER_AFFINITY(MainCycleInvoker_);

        const auto flowState = flowView->State;
        flowState->StartMutation(New<TFlowStateMutationControllerNotifier>(MakeWeak(this), flowView->EphemeralState));
        const auto spec = flowView->CurrentSpec;
        const auto dynamicSpec = flowView->CurrentDynamicSpec;
        const auto currentEpoch = flowState->ExecutionSpec->GetEpoch();

        SpecVersion_.Update(spec->GetVersion().Underlying());
        DynamicSpecVersion_.Update(dynamicSpec->GetVersion().Underlying());

        // TODO(mikari): revise order
        flowState->CurrentTimestamp = WaitFor(TimeProvider_->GetTimestamp(/*barrier*/ true))
            .ValueOrThrow();

        if (!UpdateSpecs(flowState, spec, dynamicSpec)) {
            YT_TLOG_WARNING("No job manager, fast stop");
            auto context = New<TJobManagerContext>();
            context->ClientsCache = Connector_->GetClientsCache();
            context->PipelinePath = Connector_->GetPipelinePath();
            context->Invoker = Invoker_;
            context->MainCycleInvoker = MainCycleInvoker_;
            context->TimeProvider = TimeProvider_;
            context->StatusProfiler = StatusProfiler_->WithPrefix("/job_manager");
            JobManager_ = CreateJobManager(std::move(context), New<TPipelineSpec>(), New<TDynamicPipelineSpec>(), New<TJobManagerState>(), /*authenticator*/ nullptr);
            try {
                auto pipelineState = flowState->ExecutionSpec->PipelineState->GetValue();
                if (pipelineState != EPipelineState::Paused && pipelineState != EPipelineState::Stopped && pipelineState != EPipelineState::Completed) {
                    flowState->ExecutionSpec->PipelineState->SetValue(EPipelineState::Pausing);
                }
                DoScheduling(flowView);
            } catch (const std::exception& ex) {
                JobManager_ = nullptr;
                throw;
            }
            JobManager_ = nullptr;
        } else {
            YT_VERIFY(JobManager_);
            YT_VERIFY(LeaseManager_);

            JobManager_->CheckCompletedPartitions(flowView);
            SyncTraverseDataWithSpec(flowView);
            JobManager_->AggregateTraverseData(flowView);
            JobManager_->UpdateInputStreamsTraverse(flowView);
            JobManager_->UpdateWatermarkState(flowView);

            bool flowCoreTargetMatched = CheckFlowCoreTarget(flowView, NodeInfo_->FlowCoreVersion);
            if (flowCoreTargetMatched != FlowCoreTargetMatched_) {
                const auto& flowCoreTarget = flowView->State->ExecutionSpec->FlowCoreTarget->GetValue();
                if (flowCoreTargetMatched) {
                    YT_TLOG_EVENT_FLUENT(PublicControllerLogger(), NLogging::ELogLevel::Info, "Controller FlowCoreVersion matches FlowCoreTarget, resuming scheduling")
                        .With("FlowCoreVersion", NodeInfo_->FlowCoreVersion)
                        .With("FlowCoreTarget", flowCoreTarget);
                } else {
                    YT_TLOG_EVENT_FLUENT(PublicControllerLogger(), NLogging::ELogLevel::Info, "Controller FlowCoreVersion mismatches FlowCoreTarget, scheduling is paused")
                        .With("FlowCoreVersion", NodeInfo_->FlowCoreVersion)
                        .With("FlowCoreTarget", flowCoreTarget);
                }
                FlowCoreTargetMatched_ = flowCoreTargetMatched;
            }

            if (flowCoreTargetMatched) {
                CheckTargetState(flowView);
            } else {
                auto state = flowView->State->ExecutionSpec->PipelineState->GetValue();
                if (state == EPipelineState::Working || state == EPipelineState::Draining) {
                    YT_TLOG_WARNING("FlowCoreTarget mismatch, pausing pipeline")
                        .With("Target", flowView->State->ExecutionSpec->FlowCoreTarget->GetValue())
                        .With("Actual", NodeInfo_->FlowCoreVersion);
                    flowView->State->ExecutionSpec->PipelineState->SetValue(EPipelineState::Pausing);
                }
            }

            DoScheduling(flowView);

            SyncJobManagerState(flowView);

            UpdateStreamStatistics(flowView);
            UpdateMessageTransferingInfo(flowView);

            CleanUpFlowView(flowView);
        }

        // Cleanup.
        {
            CleanUpFlowView(flowView);
            DropMissingKeys(FinishedPartitionMetrics_, spec->GetValue()->Computations);
        }

        flowState->Epoch = flowState->ExecutionSpec->GetEpoch();

        const auto newEpoch = flowState->ExecutionSpec->GetEpoch();
        if (newEpoch != currentEpoch) {
            YT_TLOG_INFO("Controller leader iteration completed, execution spec versions updated")
                .With("Epoch", newEpoch)
                .With("ExecutionSpecVersions", ConvertToYsonString(BuildExecutionSpecVersions(flowState->ExecutionSpec), EYsonFormat::Text));
        } else {
            YT_TLOG_INFO("Controller leader iteration completed")
                .With("Epoch", newEpoch);
        }
        YT_TLOG_INFO("New traverse data")
            .With("Epoch", flowState->ExecutionSpec->GetEpoch())
            .With("TraverseData", ConvertToYsonString(flowState->TraverseData, EYsonFormat::Text));
        YT_TLOG_INFO("New watermark state")
            .With("WatermarkState", ConvertToYsonString(flowState->ExecutionSpec->WatermarkState, EYsonFormat::Text));
    }

    void Commit(const TFlowViewPtr& flowView)
    {
        if (JobManager_) {
            JobManager_->Commit(flowView);
        }
    }

    void UpdateMetrics(const TFlowViewPtr& flowView)
    {
        const auto& traverseData = flowView->State->TraverseData;
        const auto& executionSpec = flowView->State->ExecutionSpec;
        const auto& pipelineSpec = executionSpec->PipelineSpec->GetValue();

        {
            auto now = WaitFor(TimeProvider_->GetTimestamp(/*barrier*/ false))
                .ValueOrThrow();

            auto registerStream = [&] (const auto& streamId, const auto& stream) {
                auto& metrics = StreamMetrics_
                    .emplace(streamId, TStreamMetrics(streamId, Profiler_))
                    .first->second;
                metrics.Apply(now, stream);
            };

            for (const auto& [streamId, stream] : traverseData->Streams) {
                registerStream(streamId, stream);
            }
            static const TStreamId unitedStreamId = TStreamId("__united__");
            registerStream(unitedStreamId, traverseData->UnitedStream);
            static const TStreamId unitedSourceStreamId = TStreamId("__united_source__");
            registerStream(unitedSourceStreamId, traverseData->UnitedSourceStream);
            static const TStreamId unitedTimerStreamId = TStreamId("__united_timer__");
            registerStream(unitedTimerStreamId, traverseData->UnitedTimerStream);
            static const TStreamId unitedKeyVisitorStreamId = TStreamId("__united_key_visitor__");
            registerStream(unitedKeyVisitorStreamId, traverseData->UnitedKeyVisitorStream);
            static const TStreamId unitedOutputStreamId = TStreamId("__united_output__");
            registerStream(unitedOutputStreamId, traverseData->UnitedOutputStream);

            auto streamIds = ConcatVectors(
                GetKeys(traverseData->Streams),
                std::vector<TStreamId>{
                    unitedStreamId,
                    unitedSourceStreamId,
                    unitedTimerStreamId,
                    unitedKeyVisitorStreamId,
                    unitedOutputStreamId});
            DropMissingKeys(StreamMetrics_, THashSet<TStreamId>(streamIds.begin(), streamIds.end()));
        }

        auto performanceMetrics = AggregatePerformanceMetrics(flowView);
        auto inputMetrics = AggregateInputMetricsByComputation(flowView);
        auto streamTraverseDatas = AggregateStreamTraverseData(flowView);
        for (const auto& [computationId, node] : traverseData->Computations) {
            auto& metrics = ComputationMetrics_.try_emplace(computationId, Profiler_, computationId).first->second;
            if (auto* computationPerformanceMetrics = performanceMetrics.FindPtr(computationId)) {
                metrics.Apply(*computationPerformanceMetrics);
            }
            if (auto* computationInputMetrics = inputMetrics.FindPtr(computationId)) {
                metrics.Apply(
                    *computationInputMetrics,
                    streamTraverseDatas[computationId],
                    flowView->EphemeralState->StreamTraverseDataMetrics[computationId],
                    GetOrCrash(pipelineSpec->Computations, computationId));
            }
        }
        DropMissingKeys(ComputationMetrics_, traverseData->Computations);

        CurrentEpochGauge_.Update(traverseData->UnitedStream->Epoch);
        ComputationCountGauge_.Update(flowView->State->ExecutionSpec->PipelineSpec->GetValue()->Computations.size());

        {
            THashMap<TComputationId, TEnumIndexedArray<EPartitionState, i64>> counts;
            for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
                counts[computationId] = {};
            }

            for (const auto& [partitionId, partition] : executionSpec->Layout->Partitions) {
                ++counts[partition->ComputationId][partition->State];
            }

            DropMissingKeys(PartitionCountMetrics_, counts);

            for (const auto& [computationId, computationCounts] : counts) {
                auto& computationMetrics = PartitionCountMetrics_.try_emplace(computationId, Profiler_, computationId).first->second;
                for (auto state : TEnumTraits<EPartitionState>::GetDomainValues()) {
                    computationMetrics.Counts[state].Update(computationCounts[state]);
                }
            }
        }

        UpdateJobStatusMetrics(flowView);

        PipelineStateMetrics_.UpdateState(Profiler_, executionSpec->PipelineState->GetValue());

        UpdateAlignmentTimestampBiasMetrics(flowView);
    }

private:
    const IYTConnectorPtr Connector_;
    const IPipelineAuthenticatorPtr PipelineAuthenticator_;
    const ITimeProviderPtr TimeProvider_;
    const TControllerConfigPtr Config_;
    const TNodeInfoPtr NodeInfo_;
    const TProfiler Profiler_;
    const IInvokerPtr Invoker_;
    const IInvokerPtr MainCycleInvoker_;
    const IThrottlerHostPtr ThrottlerHost_;
    const ILeaseManagerPtr LeaseManager_;
    IJobManagerPtr JobManager_;

    TMutationMetrics MutationMetrics_;
    THashMap<TStreamId, TStreamMetrics> StreamMetrics_;
    THashMap<TComputationId, TComputationMetrics> ComputationMetrics_; // Updated from UpdateMetrics.
    TGauge CurrentEpochGauge_;
    TGauge ComputationCountGauge_;
    THashMap<EWorkerState, TGauge> WorkerCountGauges_;
    TGauge FlowCoreTargetMismatchWorkerCountGauge_;
    THashMap<TComputationId, TComputationPartitionCountMetrics> PartitionCountMetrics_;       // Updated from UpdateMetrics.
    THashMap<TComputationId, TComputationJobStatusMetrics> JobStatusMetrics_;                 // Updated from UpdateMetrics -> UpdateJobStatusMetrics.
    THashMap<TComputationId, TComputationFinishedPartitionMetrics> FinishedPartitionMetrics_; // Updated from DoIteration.
    THashMap<TComputationId, THashMap<TStreamId, TComputationStreamAlignmentTimestampBiasMetrics>> AlignmentTimestampBiasMetrics_;
    TGauge SpecVersion_;
    TGauge DynamicSpecVersion_;
    TPipelineStateMetrics PipelineStateMetrics_;

    const IStatusProfilerPtr StatusProfiler_;

    bool FlowCoreTargetMatched_ = true;

    bool UpdateSpecs(const TFlowStatePtr& flowState, const TVersionedPipelineSpecPtr& spec, const TVersionedDynamicPipelineSpecPtr& dynamicSpec)
    {
        const auto& executionSpec = flowState->ExecutionSpec;
        if (executionSpec->PipelineSpec->GetVersion() != spec->GetVersion() || !JobManager_) {
            executionSpec->PipelineSpec = spec;
            executionSpec->ExtendedPipelineSpec->SetValue(BuildExtendedPipelineSpec(executionSpec->PipelineSpec->GetValue()));
            UpdateStreamSpecStorageState(executionSpec->StreamSpecStorageState, *spec->GetValue(), TimeProvider_);
            try {
                auto context = New<TJobManagerContext>();
                context->ClientsCache = Connector_->GetClientsCache();
                context->PipelinePath = Connector_->GetPipelinePath();
                context->Invoker = Invoker_;
                context->MainCycleInvoker = MainCycleInvoker_;
                context->TimeProvider = TimeProvider_;
                context->StatusProfiler = StatusProfiler_->WithPrefix("/job_manager");
                JobManager_ = CreateJobManager(std::move(context), executionSpec->PipelineSpec->GetValue(), executionSpec->DynamicPipelineSpec->GetValue(), flowState->JobManagerState, PipelineAuthenticator_);
            } catch (const std::exception& ex) {
                YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Error, "Failed to create job manager")
                    .With(ex);
                JobManager_ = nullptr;
            }
        }
        if (executionSpec->DynamicPipelineSpec->GetVersion() != dynamicSpec->GetVersion()) {
            executionSpec->DynamicPipelineSpec = dynamicSpec;
            if (JobManager_) {
                JobManager_->Reconfigure(executionSpec->DynamicPipelineSpec->GetValue());
            }
        }

        if (ThrottlerHost_) {
            // A freshly elected leader starts with an empty throttler host at an
            // unchanged dynamic spec version, so reconfigure it unconditionally.
            ThrottlerHost_->Reconfigure(executionSpec->DynamicPipelineSpec->GetValue());
        }

        return JobManager_ != nullptr;
    }

    bool CheckBinaryCompatibility(const TFlowViewPtr& flowView) const
    {
        const bool isBinaryVersionValid = !flowView->State->ExecutionSpec->PipelineSpec->GetValue()->ValidateBinaryVersion ||
            flowView->State->ExecutionSpec->PipelineSpec->GetValue()->BinaryVersion == GetVersion();
        const bool isBinaryChecksumValid = !flowView->State->ExecutionSpec->PipelineSpec->GetValue()->ValidateBinaryChecksum ||
            flowView->State->ExecutionSpec->PipelineSpec->GetValue()->BinaryChecksum == GetBinaryChecksum();
        return isBinaryVersionValid && isBinaryChecksumValid;
    }

    void CheckTargetState(const TFlowViewPtr& flowView) const
    {
        const auto& executionSpec = flowView->State->ExecutionSpec;
        const auto& dynamicSpec = executionSpec->DynamicPipelineSpec->GetValue();
        if (CheckBinaryCompatibility(flowView)) {
            if (executionSpec->PipelineState->GetValue() != EPipelineState::Completed && executionSpec->PipelineState->GetValue() != dynamicSpec->TargetState) {
                switch (dynamicSpec->TargetState) {
                    case EPipelineState::Completed:
                        if (executionSpec->PipelineState->GetValue() != EPipelineState::Working) {
                            executionSpec->PipelineState->SetValue(EPipelineState::Working);
                        }
                        break;
                    case EPipelineState::Stopped:
                        if (executionSpec->PipelineState->GetValue() != EPipelineState::Draining) {
                            executionSpec->PipelineState->SetValue(EPipelineState::Draining);
                        }
                        break;
                    default:
                        if (executionSpec->PipelineState->GetValue() != EPipelineState::Pausing && executionSpec->PipelineState->GetValue() != EPipelineState::Stopped && executionSpec->PipelineState->GetValue() != EPipelineState::Completed) {
                            executionSpec->PipelineState->SetValue(EPipelineState::Pausing);
                        }
                        break;
                }
            }
        } else {
            YT_TLOG_WARNING("Binary compatibility mismatch");
            if (executionSpec->PipelineState->GetValue() != EPipelineState::Completed && executionSpec->PipelineState->GetValue() != EPipelineState::Paused && executionSpec->PipelineState->GetValue() != EPipelineState::Stopped) {
                executionSpec->PipelineState->SetValue(EPipelineState::Pausing);
            }
        }
    }

    class TFlowStateMutationControllerNotifier
        : public TFlowStateMutationNotifier
    {
    public:
        TFlowStateMutationControllerNotifier(const TWeakPtr<TControllerLeader>& controllerLeader, const TFlowEphemeralStatePtr& ephemeralState)
            : WeakLeader_(controllerLeader)
            , EphemeralState_(ephemeralState)
        { }

        void OnCreatePartition(const TPartitionPtr& /*newPartition*/) override
        {
            if (auto strongLeader = WeakLeader_.Lock()) {
                strongLeader->MutationMetrics_.CreatePartitionCounter.Increment();
            }
        }

        void OnUpdatePartition(const TPartitionPtr& oldPartition, const TPartitionPtr& newPartition) override
        {
            YT_VERIFY(oldPartition->PartitionId == newPartition->PartitionId);

            auto strongLeader = WeakLeader_.Lock();
            if (!strongLeader) {
                return;
            }

            YT_ASSERT_INVOKER_AFFINITY(strongLeader->MainCycleInvoker_);

            strongLeader->MutationMetrics_.UpdatePartitionStateCounter.Increment();

            if (oldPartition->State != newPartition->State) {
                auto computationId = newPartition->ComputationId;
                auto& computationMetrics = strongLeader->FinishedPartitionMetrics_
                    .try_emplace(computationId, strongLeader->Profiler_, computationId)
                    .first->second;
                if (newPartition->State == EPartitionState::Completed) {
                    computationMetrics.CompletedPartitionCount.Increment(1);
                } else if (newPartition->State == EPartitionState::Interrupted) {
                    computationMetrics.InterruptedPartitionCount.Increment(1);
                }
            }
        }

        void OnRemovePartition(const TPartitionPtr& /*oldPartition*/) override
        {
            if (auto strongLeader = WeakLeader_.Lock()) {
                strongLeader->MutationMetrics_.RemovePartitionCounter.Increment();
            }
        }

        void OnCreateJob(const TJobPtr& newJob) override
        {
            if (auto strongLeader = WeakLeader_.Lock()) {
                strongLeader->MutationMetrics_.CreateJobCounter.Increment();
                EphemeralState_->WorkerIncarnationsJobs[newJob->WorkerIncarnationId].insert(newJob->JobId);
            }
        }

        void OnUpdateJob(const TJobPtr& /*oldJob*/, const TJobPtr& /*newJob*/) override
        {
            if (auto strongLeader = WeakLeader_.Lock()) {
                strongLeader->MutationMetrics_.UpdateJobLeaseCounter.Increment();
            }
        }

        void OnRemoveJob(const TJobPtr& oldJob, EJobFinishReason reason) override
        {
            if (auto strongLeader = WeakLeader_.Lock()) {
                YT_ASSERT_INVOKER_AFFINITY(strongLeader->MainCycleInvoker_);

                strongLeader->MutationMetrics_.RemoveJobCounter.Increment();

                auto& state = EphemeralState_->Partitions[oldJob->PartitionId];
                if (!state) {
                    state = New<TPartitionEphemeralState>();
                }
                state->PreviousJobFinishReason = reason;

                EphemeralState_->WorkerIncarnationsJobs[oldJob->WorkerIncarnationId].erase(oldJob->JobId);
            }
        }

    private:
        TWeakPtr<TControllerLeader> WeakLeader_;
        TFlowEphemeralStatePtr EphemeralState_;
    };

    void DoScheduling(const TFlowViewPtr& flowView)
    {
        auto checkLeases = [&] {
            LeaseManager_->CheckLeases(flowView);
        };
        auto terminateAndPrepareLeases = [&] {
            LeaseManager_->TerminateStrayLeases(flowView);
            LeaseManager_->PrepareLeases(flowView);
        };
        auto stopJobsAndResetState = [&] (EPipelineState newState) {
            JobManager_->StopAllJobs(flowView);
            LeaseManager_->TerminateStrayLeases(flowView);
            flowView->State->ExecutionSpec->PipelineState->SetValue(newState);
        };
        auto manageJobs = [&] {
            JobManager_->RemoveFailedJobs(flowView);
            JobManager_->RemoveLostJobs(flowView);
            JobManager_->DoPartitioning(flowView);
            JobManager_->DistributeJobs(flowView);
        };

        auto state = flowView->State->ExecutionSpec->PipelineState->GetValue();
        checkLeases();
        if (state == EPipelineState::Working || state == EPipelineState::Draining) {
            manageJobs();
            terminateAndPrepareLeases();
        }
        if (state == EPipelineState::Working && JobManager_->CheckPipelineCompleted(flowView)) {
            stopJobsAndResetState(EPipelineState::Completed);
        } else if (state == EPipelineState::Draining && JobManager_->CheckPipelineStopped(flowView)) {
            stopJobsAndResetState(EPipelineState::Stopped);
        } else if (state == EPipelineState::Pausing) {
            stopJobsAndResetState(EPipelineState::Paused);
        }
    }

    void UpdateJobStatusMetrics(const TFlowViewPtr& flowView)
    {
        struct TCounters
        {
            ui64 Unknown = 0;
            ui64 Preparing = 0;
            ui64 WorkingYoung = 0;
            ui64 WorkingOld = 0;
            ui64 WorkingWithRetryableError = 0;
            ui64 Stopped = 0;
        };

        auto youngWorkingJobThreshold = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying()) - YoungWorkingJobThreshold;

        const auto& executionSpec = flowView->State->ExecutionSpec;
        const auto& pipelineSpec = executionSpec->PipelineSpec->GetValue();

        THashMap<TComputationId, TEnumIndexedArray<EJobFinishReason, TCounters>> counters;
        for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
            counters[computationId] = {};
        }

        for (const auto& [partitionId, partition] : executionSpec->Layout->Partitions) {
            if (!(partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting)) {
                continue;
            }

            EJobFinishReason previousJobFinishReason = EJobFinishReason::Unknown;
            if (auto it = flowView->EphemeralState->Partitions.find(partitionId); it != flowView->EphemeralState->Partitions.end()) {
                previousJobFinishReason = it->second->PreviousJobFinishReason;
            }
            auto& reasonCounters = counters[partition->ComputationId][previousJobFinishReason];
            bool hasJob = partition->CurrentJobId.has_value();
            if (!hasJob && previousJobFinishReason == EJobFinishReason::Stopped) {
                reasonCounters.Stopped += 1;
                continue;
            }

            auto handlePartitionWithoutJobStatus = [&] {
                if (hasJob) {
                    reasonCounters.Preparing += 1;
                } else {
                    reasonCounters.Unknown += 1;
                }
            };

            auto statusIt = flowView->Feedback->PartitionJobStatuses.find(partitionId);
            if (statusIt == flowView->Feedback->PartitionJobStatuses.end()) {
                handlePartitionWithoutJobStatus();
                continue;
            }
            const auto& status = statusIt->second;
            if (!status->CurrentJobStatus) {
                handlePartitionWithoutJobStatus();
            } else if (!status->CurrentJobStatus->Error.IsOK()) {
                reasonCounters.Unknown += 1;
            } else if (!status->CurrentJobStatus->RetryableErrors.empty()) {
                reasonCounters.WorkingWithRetryableError += 1;
            } else if (!status->CurrentJobStatus->InitedTime.has_value()) {
                reasonCounters.Preparing += 1;
            } else if (*status->CurrentJobStatus->InitedTime > youngWorkingJobThreshold) {
                reasonCounters.WorkingYoung += 1;
            } else {
                reasonCounters.WorkingOld += 1;
            }
        }

        DropMissingKeys(JobStatusMetrics_, counters);

        TCounters totalCounters;
        for (const auto& [computationId, computationCounters] : counters) {
            auto& computationMetrics = JobStatusMetrics_.try_emplace(computationId, Profiler_, computationId).first->second;

            for (auto reason : TEnumTraits<EJobFinishReason>::GetDomainValues()) {
                auto& reasonCounters = computationCounters[reason];

                auto& reasonMetrics = computationMetrics.ReasonMetrics[reason];
                reasonMetrics.Unknown.Update(reasonCounters.Unknown);
                reasonMetrics.Preparing.Update(reasonCounters.Preparing);
                reasonMetrics.WorkingYoung.Update(reasonCounters.WorkingYoung);
                reasonMetrics.WorkingOld.Update(reasonCounters.WorkingOld);
                reasonMetrics.WorkingWithRetryableError.Update(reasonCounters.WorkingWithRetryableError);
                reasonMetrics.Stopped.Update(reasonCounters.Stopped);

                totalCounters.Unknown += reasonCounters.Unknown;
                totalCounters.Preparing += reasonCounters.Preparing;
                totalCounters.WorkingYoung += reasonCounters.WorkingYoung;
                totalCounters.WorkingOld += reasonCounters.WorkingOld;
                totalCounters.WorkingWithRetryableError += reasonCounters.WorkingWithRetryableError;
                totalCounters.Stopped += reasonCounters.Stopped;
            }
        }

        const auto pipelineState = flowView->IsSynced()
            ? flowView->State->ExecutionSpec->PipelineState->GetValue()
            : EPipelineState::Unknown;
        YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Info, "Jobs status")
            .With("PipelineState", pipelineState)
            .With("Workers", std::ssize(flowView->State->Workers))
            .With("WorkingOld", totalCounters.WorkingOld)
            .With("WorkingYoung", totalCounters.WorkingYoung)
            .With("WorkingWithRetryableError", totalCounters.WorkingWithRetryableError)
            .With("Preparing", totalCounters.Preparing)
            .With("Unknown", totalCounters.Unknown)
            .With("Stopped", totalCounters.Stopped)
            .With("FlowViewAge", TInstant::Now() - TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying()));

        if (const auto& computations = flowView->EphemeralState->TraverseUncoveredComputations) {
            YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Some computations has partial traverse coverage")
                .With("Computations", computations);
        }
    }

    void UpdateAlignmentTimestampBiasMetrics(const TFlowViewPtr& flowView)
    {
        const auto& executionSpec = flowView->State->ExecutionSpec;
        const auto& pipelineSpec = executionSpec->PipelineSpec->GetValue();
        DropMissingKeys(AlignmentTimestampBiasMetrics_, pipelineSpec->Computations);
        for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
            auto& computationMetrics = AlignmentTimestampBiasMetrics_[computationId];
            DropMissingKeys(computationMetrics, computationSpec->OutputStreamIds);
            for (const auto& streamId : computationSpec->OutputStreamIds) {
                auto statistics = GetOrDefault(flowView->EphemeralState->MessageTransferingInfo->StreamTimestampStatistics, streamId);
                if (statistics.MessageCount != 0) {
                    auto& metrics = computationMetrics.try_emplace(streamId, Profiler_, computationId, streamId).first->second;
                    metrics.AlignmentTimestampBias.Update(statistics.EstimateAlignmentToEventTimestampBias());
                }
            }
        }
    }

    static THashMap<TComputationId, TAggregatedNodePerformanceMetricsPtr> AggregatePerformanceMetrics(const TFlowViewPtr& flowView)
    {
        const auto& flowLayout = flowView->State->ExecutionSpec->Layout;
        THashMap<TComputationId, std::vector<TNodePerformanceMetricsPtr>> groupedMetrics;
        for (const auto& [partitionId, partition] : flowLayout->Partitions) {
            if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
                auto statusIt = flowView->Feedback->PartitionJobStatuses.find(partitionId);
                if (statusIt == flowView->Feedback->PartitionJobStatuses.end()) {
                    continue;
                }
                const auto& status = statusIt->second;
                if (!status->CurrentJobStatus) {
                    continue;
                }
                groupedMetrics[partition->ComputationId].push_back(status->CurrentJobStatus->PerformanceMetrics);
            }
        }
        THashMap<TComputationId, TAggregatedNodePerformanceMetricsPtr> result;
        for (const auto& [computationId, metrics] : groupedMetrics) {
            result[computationId] = AggregateNodePerformanceMetrics(metrics);
        }
        return result;
    }

    static THashMap<TComputationId, THashMap<TStreamId, TAggregatedStreamTraverseData>> AggregateStreamTraverseData(const TFlowViewPtr& flowView)
    {
        const auto& flowLayout = flowView->State->ExecutionSpec->Layout;
        THashMap<TComputationId, THashMap<TStreamId, std::vector<TStreamTraverseDataPtr>>> groupedMetrics;
        for (const auto& [partitionId, partition] : flowLayout->Partitions) {
            if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
                auto statusIt = flowView->Feedback->PartitionJobStatuses.find(partitionId);
                if (statusIt == flowView->Feedback->PartitionJobStatuses.end()) {
                    continue;
                }
                const auto& status = statusIt->second->CurrentJobStatus;
                if (!status || !status->FromPartitionTraverseData || !status->FromPartitionTraverseData->Node) {
                    continue;
                }
                for (const auto& [streamId, streamTraverseData] : status->FromPartitionTraverseData->Node->Streams) {
                    groupedMetrics[partition->ComputationId][streamId].push_back(streamTraverseData);
                }
            }
        }
        THashMap<TComputationId, THashMap<TStreamId, TAggregatedStreamTraverseData>> result;
        for (const auto& [computationId, streams] : groupedMetrics) {
            for (const auto& [streamId, streamTraverseDatas] : streams) {
                YT_VERIFY(!streamTraverseDatas.empty());

                auto& data = result[computationId][streamId];
                data.Count = streamTraverseDatas.size();
                data.Total = MergeStreamTraverseData(streamTraverseDatas, EInflightMerge::Sum, /*allowPartial*/ true);
                data.Max = MergeStreamTraverseData(streamTraverseDatas, EInflightMerge::Max, /*allowPartial*/ true);
            }
        }
        return result;
    }

    void SyncJobManagerState(const TFlowViewPtr& flowView)
    {
        YT_VERIFY(JobManager_);
        auto currentJobManagerState = JobManager_->GetState();
        if (!AreNodesEqual(ConvertToNode(currentJobManagerState), ConvertToNode(flowView->State->JobManagerState))) {
            YT_TLOG_INFO("Updating job manager state in flow view")
                .With("NewState", NYson::ConvertToYsonString(currentJobManagerState, EYsonFormat::Text));
            flowView->State->JobManagerState = currentJobManagerState;
        }
    }

    void UpdateStreamStatistics(const TFlowViewPtr& flowView)
    {
        const auto now = flowView->State->CurrentTimestamp;
        const auto& traverseData = flowView->State->TraverseData;
        auto& speedStatistics = flowView->State->SpeedStatistics;

        YT_VERIFY(now >= speedStatistics.LastUpdated,
            Format("Flow view current timestamp must not decrease (Now: %v, LastUpdated: %v)", now, speedStatistics.LastUpdated));

        const double timeDelta = static_cast<double>(now.Underlying() - speedStatistics.LastUpdated.Underlying());

        const double halfDecayPeriodSeconds = TDuration::Days(1).SecondsFloat();
        const double decay = std::exp2(-timeDelta / halfDecayPeriodSeconds);

        for (const auto& [streamId, streamTraverseData] : traverseData->Streams) {
            const auto& metrics = streamTraverseData->InflightMetrics;
            if (!metrics) {
                continue;
            }
            auto& stats = speedStatistics.StreamSpeed1d[streamId];

            const double currentMessages = metrics->ProcessedCountPerSec.value_or(0.0);
            const double currentBytes = metrics->ProcessedBytesPerSec.value_or(0.0);
            stats.ProcessedMessagesPerSecond = currentMessages * (1.0 - decay) + stats.ProcessedMessagesPerSecond * decay;
            stats.ProcessedBytesPerSecond = currentBytes * (1.0 - decay) + stats.ProcessedBytesPerSecond * decay;
        }
        speedStatistics.LastUpdated = now;

        // Remove statistics for streams that no longer exist.
        DropMissingKeys(speedStatistics.StreamSpeed1d, traverseData->Streams);
    }

    void UpdateMessageTransferingInfo(const TFlowViewPtr& flowView)
    {
        // TODO: Must be smoothed some way. There is many gaps now.
        THashMap<TStreamId, TTimestampStatistics> timestampStatistics;
        for (const auto& [workerAddress, workerStatus] : flowView->Feedback->WorkerStatuses) {
            for (const auto& [streamId, statistics] : workerStatus->MessageDistributorStatus->StreamTimestampStatistics) {
                timestampStatistics[streamId] += statistics;
            }
        }
        flowView->EphemeralState->MessageTransferingInfo->StreamTimestampStatistics = std::move(timestampStatistics);

        // Pass total (summed over all partitions) smoothed stream speeds to workers.
        // Workers divide by their local partition count to get per-partition baseline demand.
        flowView->EphemeralState->MessageTransferingInfo->SpeedStatistics = flowView->State->SpeedStatistics;
    }

    void CleanUpFlowView(const TFlowViewPtr& flowView)
    {
        const auto now = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
        const auto& flowLayout = flowView->State->ExecutionSpec->Layout;

        std::vector<TPartitionId> interruptedPartitions;
        for (const auto& [partitionId, partition] : flowLayout->Partitions) {
            if (partition->State == EPartitionState::Interrupted) {
                YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Info, "Removing interrupted partition")
                    .With("PartitionId", partitionId)
                    .With("Partition", NYson::ConvertToYsonString(partition, EYsonFormat::Text));
                interruptedPartitions.push_back(partitionId);
            }
        }
        for (const auto& partitionId : interruptedPartitions) {
            flowLayout->RemovePartition(partitionId);
        }

        EraseNodesIf(
            flowView->EphemeralState->Partitions,
            [&] (auto keyValue) {
                return !flowLayout->Partitions.contains(keyValue.first);
            });

        for (const auto& [partitionId, partitionState] : flowView->EphemeralState->Partitions) {
            if (partitionState->PreviousJobFailInstant + YoungWorkingJobThreshold < now) {
                partitionState->PreviousJobFailInstant = {};
                partitionState->PreviousJobFailError = {};
            }
        }

        // Remove WorkerSpecs entries for workers that no longer exist.
        std::vector<std::string> staleWorkerAddresses;
        for (const auto& [workerAddress, workerSpec] : flowLayout->WorkerSpecs) {
            if (!flowView->State->Workers.contains(workerAddress)) {
                staleWorkerAddresses.push_back(workerAddress);
            }
        }
        for (const auto& workerAddress : staleWorkerAddresses) {
            flowLayout->WorkerSpecs.erase(workerAddress);
        }
    }
};

class TController
    : public IController
{
public:
    TController(
        TControllerConfigPtr config,
        TNodeInfoPtr controllerNodeInfo,
        IWorkerTrackerPtr workerTracker,
        IThrottlerHostPtr throttlerHost,
        IInvokerPtr invoker,
        IYTConnectorPtr connector,
        IPersistedStateManagerPtr persistedStateManager,
        IPipelineAuthenticatorPtr authenticator,
        bool ignoreSingletonsDynamicConfig,
        NObjectClient::TCellTag clockClusterTag,
        IStatusProfilerPtr rootStatusProfiler)
        : Config_(config)
        , NodeInfo_(controllerNodeInfo)
        , WorkerTracker_(std::move(workerTracker))
        , ThrottlerHost_(std::move(throttlerHost))
        , Invoker_(std::move(invoker))
        , Connector_(std::move(connector))
        , PersistedStateManager_(persistedStateManager)
        , PipelineAuthenticator_(std::move(authenticator))
        , IgnoreSingletonsDynamicConfig_(ignoreSingletonsDynamicConfig)
        , FlowViewKeeper_(New<TFlowViewKeeper>())
        , ClockClusterTag_(clockClusterTag)
        , RootStatusProfiler_(std::move(rootStatusProfiler))
    { }

    void Initialize() override
    {
        Connector_->SubscribeLeadingStarted(BIND(&TController::DoStartSafe, MakeWeak(this)));
        Connector_->SubscribeLeadingEnded(BIND(&TController::DoStopSafe, MakeWeak(this)));
    }

    void EnsureIsLeader() const override
    {
        if (!Connector_->IsLeader()) {
            THROW_ERROR_EXCEPTION("This instance is not leading");
        }
    }

    TFlowViewKeeperPtr GetFlowViewKeeper() const override
    {
        EnsureIsLeader();
        return FlowViewKeeper_;
    }

    TNodeInfoPtr GetNodeInfo() const override
    {
        return NodeInfo_;
    }

    void RegisterJobStatus(const TJobId& jobId, TJobStatusPtr jobStatus) override
    {
        EnsureIsLeader();
        auto guard = Guard(FreshStatusesLock_);
        FreshJobStatuses_[jobId] = jobStatus;
    }

    void RegisterWorkerStatus(TStringBuf workerAddress, TWorkerStatusPtr status) override
    {
        EnsureIsLeader();
        auto guard = Guard(FreshStatusesLock_);
        FreshWorkerStatuses_[std::string(workerAddress)] = status;
    }

private:
    struct TRegularActivityContext
    {
        // Arguments.
        TProfiler LeaderProfiler;
        IStatusProfilerPtr RootStatusProfiler;
        std::string ActivityName;

        // Ordinary fields;
        TProfiler Profiler = LeaderProfiler.WithPrefix(Format("/%v", ActivityName));

        TCounter TotalIterations = Profiler.Counter("/iterations_total");
        TCounter FailedIterations = Profiler.Counter("/iterations_failed");
        TEventTimer IterationTime = Profiler.Timer("/iteration_time");

        IStatusErrorStatePtr ErrorState = RootStatusProfiler->ErrorState(Format("/%v", ActivityName));
    };

    const TControllerConfigPtr Config_;
    const TNodeInfoPtr NodeInfo_;
    const IWorkerTrackerPtr WorkerTracker_;
    const IThrottlerHostPtr ThrottlerHost_;
    const IInvokerPtr Invoker_;
    const IYTConnectorPtr Connector_;
    const IPersistedStateManagerPtr PersistedStateManager_;
    const IPipelineAuthenticatorPtr PipelineAuthenticator_;
    const IFlowExecutorPtr FlowExecutor_;
    const bool IgnoreSingletonsDynamicConfig_;
    const TFlowViewKeeperPtr FlowViewKeeper_;
    const NObjectClient::TCellTag ClockClusterTag_;
    const IStatusProfilerPtr RootStatusProfiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StartLock_);
    TCancelableContextPtr CancelableContext_;
    // Tracks the current leader; used for double-start detection and the post-stop watchdog.
    TWeakPtr<TControllerLeader> WeakLeader_;
    static constexpr TDuration LeaderStopTimeout = TDuration::Minutes(5);

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, FreshStatusesLock_);
    THashMap<TJobId, TJobStatusPtr> FreshJobStatuses_;
    THashMap<std::string, TWorkerStatusPtr> FreshWorkerStatuses_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void DoStartSafe() noexcept
    {
        try {
            DoStart();
        } catch (const std::exception& ex) {
            YT_TLOG_FATAL("Unexpected exception escaped TController::DoStart")
                .With(TError(ex));
        }
    }

    void DoStopSafe() noexcept
    {
        try {
            DoStop();
        } catch (const std::exception& ex) {
            YT_TLOG_FATAL("Unexpected exception escaped TController::DoStop")
                .With(TError(ex));
        }
    }

    void DoStart()
    {
        auto guard = Guard(StartLock_);

        YT_TLOG_FATAL_IF(WeakLeader_.Get() != nullptr,
            "DoStart invoked while previous leader is still active (double-start)");

        YT_TLOG_INFO("Started leading");
        CancelableContext_ = New<TCancelableContext>();
        auto cancelableInvoker = CancelableContext_->CreateInvoker(Invoker_);
        auto mainCycleInvoker = CreateSerializedInvoker(cancelableInvoker);

        auto pipelinePath = Connector_->GetPipelinePath();

        auto leaderProfiler = ControllerProfiler()
            .WithDefaultDisabled()
            .WithGlobal()
            .WithRequiredTag("pipeline_path", pipelinePath.GetPath())
            .WithRequiredTag("pipeline_cluster", *pipelinePath.GetCluster());

        auto leader = New<TControllerLeader>(
            leaderProfiler,
            Connector_,
            PipelineAuthenticator_,
            Config_,
            NodeInfo_,
            cancelableInvoker,
            mainCycleInvoker,
            ThrottlerHost_,
            ClockClusterTag_,
            RootStatusProfiler_);
        WeakLeader_ = leader;

        auto schedulerActivityContext = TRegularActivityContext{.LeaderProfiler = leaderProfiler, .RootStatusProfiler = RootStatusProfiler_, .ActivityName = "schedule"};
        YT_UNUSED_FUTURE(BIND([this, this_ = MakeStrong(this), leader, schedulerActivityContext] {
            auto dynamicSpecVersion = TVersion(-1);
            while (true) {
                try {
                    schedulerActivityContext.TotalIterations.Increment();
                    YT_TLOG_INFO("Recovering")
                        .With("DynamicSpecVersion", dynamicSpecVersion);

                    {
                        auto flowState = PersistedStateManager_->RecoverFlowState();
                        auto spec = PersistedStateManager_->RecoverSpec();
                        auto dynamicSpec = PersistedStateManager_->RecoverDynamicSpec();
                        flowState->ExecutionSpec->FlowCoreTarget = PersistedStateManager_->RecoverFlowCoreTarget();
                        auto flowEphemeralState = ComputeInitialEphemeralState(flowState, dynamicSpec, Connector_->GetPipelinePath());
                        FlowViewKeeper_->Init(flowState, flowEphemeralState, spec, dynamicSpec);
                        YT_TLOG_INFO("Flow state recovered")
                            .With("PartitionCount", flowState->ExecutionSpec->Layout->Partitions.size())
                            .With("JobCount", flowState->ExecutionSpec->Layout->Jobs.size())
                            .With("FlowCoreTarget", flowState->ExecutionSpec->FlowCoreTarget->GetValue());
                    }

                    YT_TLOG_INFO("Initialized. Sleep to WarmUp")
                        .With("WarmUpTime", Config_->WarmUpTime);

                    TDelayedExecutor::WaitForDuration(Config_->WarmUpTime);

                    while (true) {
                        try {
                            auto guard = TEventTimerGuard(schedulerActivityContext.IterationTime);
                            {
                                YT_TLOG_INFO("Reloading specs")
                                    .With("DynamicSpecVersion", dynamicSpecVersion);
                                FlowViewKeeper_->SetSpecs(PersistedStateManager_->RecoverSpec(), PersistedStateManager_->RecoverDynamicSpec());

                                YT_TLOG_INFO("Reloading flow core target")
                                    .With("DynamicSpecVersion", dynamicSpecVersion);
                                FlowViewKeeper_->SetFlowCoreTarget(PersistedStateManager_->RecoverFlowCoreTarget());
                            }

                            {
                                auto currentFlowView = FlowViewKeeper_->GetFlowView();

                                if (currentFlowView->CurrentDynamicSpec->GetVersion() != dynamicSpecVersion && !IgnoreSingletonsDynamicConfig_) {
                                    dynamicSpecVersion = currentFlowView->CurrentDynamicSpec->GetVersion();
                                    TSingletonManager::Reconfigure(currentFlowView->CurrentDynamicSpec->GetValue()->Singletons);
                                    WorkerTracker_->Reconfigure(currentFlowView->CurrentDynamicSpec->GetValue());
                                }

                                YT_TLOG_INFO("Schedule iteration started")
                                    .With("PipelineState", currentFlowView->State->ExecutionSpec->PipelineState->GetValue())
                                    .With("Version", currentFlowView->State->ExecutionSpec->PipelineState->GetVersion())
                                    .With("TargetState", currentFlowView->CurrentDynamicSpec->GetValue()->TargetState);
                                auto expectedVersions = MakePipelineImportantVersions(currentFlowView->State, currentFlowView->CurrentSpec);

                                auto newFlowView = currentFlowView->CopyPtr();
                                newFlowView->State = newFlowView->State->Clone();
                                newFlowView->EphemeralState = CloneYsonStruct(newFlowView->EphemeralState);
                                YT_TLOG_INFO("Iteration flow view parts cloned")
                                    .With("Version", newFlowView->State->ExecutionSpec->PipelineState->GetVersion());

                                leader->SyncWorkers(newFlowView, WorkerTracker_->GetWorkers());
                                YT_TLOG_INFO("Iteration sync workers finished")
                                    .With("Version", newFlowView->State->ExecutionSpec->PipelineState->GetVersion());
                                leader->DoIteration(newFlowView);
                                YT_TLOG_INFO("Iteration finished")
                                    .With("Version", newFlowView->State->ExecutionSpec->PipelineState->GetVersion());

                                PersistedStateManager_->PersistFlowState(newFlowView->State, expectedVersions);
                                YT_TLOG_INFO("Flow state persisted")
                                    .With("Version", newFlowView->State->ExecutionSpec->PipelineState->GetVersion());
                                leader->Commit(newFlowView);
                                PersistedStateManager_->AdvanceInputMessagesWatermark(newFlowView->State->TraverseData->InputSystemWatermark);
                                YT_TLOG_INFO("Watermark advanced")
                                    .With("Version", newFlowView->State->ExecutionSpec->PipelineState->GetVersion());
                                FlowViewKeeper_->SetStates(newFlowView->State, newFlowView->EphemeralState);
                                YT_TLOG_INFO("State and ephemeral state updated")
                                    .With("Version", newFlowView->State->ExecutionSpec->PipelineState->GetVersion());

                                YT_TLOG_INFO("Schedule iteration finished")
                                    .With("PipelineState", newFlowView->State->ExecutionSpec->PipelineState->GetValue())
                                    .With("Version", newFlowView->State->ExecutionSpec->PipelineState->GetVersion())
                                    .With("TargetState", newFlowView->CurrentDynamicSpec->GetValue()->TargetState);
                                schedulerActivityContext.ErrorState->ClearError();
                            }
                        } catch (const std::exception& ex) {
                            auto error = TError(ex);
                            if (!error.FindMatching(NFlow::EErrorCode::SpecVersionMismatch) && !error.FindMatching(NFlow::EErrorCode::FlowCoreTargetVersionMismatch)) {
                                THROW_ERROR_EXCEPTION("Schedule iteration failed")
                                    << error;
                            }
                            schedulerActivityContext.FailedIterations.Increment();
                            schedulerActivityContext.ErrorState->SetError(error);
                            YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Schedule iteration failed")
                                .With(error);
                        }
                        TDelayedExecutor::WaitForDuration(Config_->SchedulerPeriod);
                        schedulerActivityContext.TotalIterations.Increment();
                    }
                } catch (const std::exception& ex) {
                    auto error = TError(ex);
                    schedulerActivityContext.FailedIterations.Increment();
                    schedulerActivityContext.ErrorState->SetError(error);
                    DoCleanUp();
                    YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Error, "Scheduler Executor thread failed and restarted")
                        .With(error);
                    TDelayedExecutor::WaitForDuration(Config_->SchedulerPeriod);
                }
            }
        })
                .AsyncVia(mainCycleInvoker)
                .Run());

        static auto getLogLevel = [] (const TError& error) {
            return error.FindMatching(NFlow::EErrorCode::FlowViewKeeperIsNotInitialized) ? NLogging::ELogLevel::Info : NLogging::ELogLevel::Error;
        };

        auto startRegularActivity = [&] (std::string name, TDuration period, auto&& callback) {
            auto activityContext = TRegularActivityContext{.LeaderProfiler = leaderProfiler, .RootStatusProfiler = RootStatusProfiler_, .ActivityName = name};
            YT_UNUSED_FUTURE(BIND([=, callback = std::forward<decltype(callback)>(callback)] () mutable {
                while (true) {
                    try {
                        activityContext.TotalIterations.Increment();
                        auto guard = TEventTimerGuard(activityContext.IterationTime);
                        YT_TLOG_INFO("Starting iteration")
                            .With("Name", name);
                        callback();
                        YT_TLOG_INFO("Completed iteration")
                            .With("Name", name);
                        activityContext.ErrorState->ClearError();
                    } catch (const std::exception& ex) {
                        auto error = TError("Failed to execute %v iteration", name) << ex;
                        activityContext.FailedIterations.Increment();
                        activityContext.ErrorState->SetError(error);
                        YT_TLOG_EVENT_FLUENT(Logger, getLogLevel(error), "")
                            .With(error);
                    }
                    TDelayedExecutor::WaitForDuration(period);
                }
            })
                    .AsyncVia(cancelableInvoker)
                    .Run());
        };

        startRegularActivity(
            "build_cache",
            Config_->CachePeriod,
            [this, this_ = MakeStrong(this)] {
                FlowViewKeeper_->RebuildNodeCache(Invoker_);
            });

        startRegularActivity(
            "collect_feedback",
            Config_->FeedbackPeriod,
            [this, this_ = MakeStrong(this)] {
                auto currentView = FlowViewKeeper_->GetFlowView();
                auto expectedSpecVersion = currentView->CurrentSpec->GetVersion();
                auto newFeedback = CollectFeedback(currentView);
                if (!FlowViewKeeper_->SetFeedback(newFeedback, expectedSpecVersion)) {
                    YT_TLOG_INFO("Skipping stale feedback installation: spec version moved past barrier")
                        .With("ExpectedVersion", expectedSpecVersion);
                }
            });

        startRegularActivity(
            "update_metrics",
            Config_->MetricsPeriod,
            [this, this_ = MakeStrong(this), leader] {
                auto flowView = FlowViewKeeper_->GetFlowView();
                leader->UpdateMetrics(flowView);
            });

        startRegularActivity(
            "write_own_retryable_errors",
            Config_->WriteOwnRetryableErrorsPeriod,
            [this, this_ = MakeStrong(this), previousErrors = THashMap<std::string, TError>()] () mutable {
                auto currentStatus = RootStatusProfiler_->GetStatus();
                for (const auto& [component, currentError] : currentStatus.Errors) {
                    TError* previousError = previousErrors.FindPtr(component);
                    if (!previousError || currentError != *previousError) {
                        YT_TLOG_EVENT_FLUENT(PublicControllerLogger, getLogLevel(currentError), "Found new retryable errors in controller")
                            .With("Component", component)
                            .With(currentError);
                    }
                }
                previousErrors = std::move(currentStatus.Errors);
            });
    }

    void DoStop()
    {
        auto guard = Guard(StartLock_);

        YT_TLOG_INFO("Stopping leading");

        if (CancelableContext_) {
            CancelableContext_->Cancel(TError("Controller disconnected"));
            CancelableContext_.Reset();
        }
        DoCleanUp();

        // Watchdog: if leader still alive after the timeout, abort to avoid zombies.
        auto weakLeader = std::move(WeakLeader_);
        TDelayedExecutor::Submit(
            BIND([weakLeader = std::move(weakLeader)] {
                if (auto leader = weakLeader.Lock()) {
                    YT_TLOG_FATAL("Controller leader is still alive after DoStop; aborting to avoid zombie")
                        .With("Timeout", LeaderStopTimeout);
                }
            }),
            LeaderStopTimeout);
    }

    void DoCleanUp()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        FlowViewKeeper_->Reset();
    }

    TFlowFeedbackPtr CollectFeedback(const TFlowViewPtr& flowView)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& currentLayout = flowView->State->ExecutionSpec->Layout;
        const auto& currentPartitionJobStatuses = flowView->Feedback->PartitionJobStatuses;

        auto feedback = New<TFlowFeedback>();

        THashMap<TJobId, TJobStatusPtr> freshJobStatuses;
        THashMap<std::string, TWorkerStatusPtr> freshWorkerStatuses;
        {
            auto guard = Guard(FreshStatusesLock_);
            std::swap(freshJobStatuses, FreshJobStatuses_);
            std::swap(freshWorkerStatuses, FreshWorkerStatuses_);
        }

        auto now = TInstant::Now();
        feedback->UpdateTime = now;
        for (const auto& [partitionId, partition] : currentLayout->Partitions) {
            auto newStatus = New<TPartitionJobStatus>();
            if (auto iter = currentPartitionJobStatuses.find(partitionId); iter != currentPartitionJobStatuses.end()) {
                static_cast<TPartitionJobStatusBase&>(*newStatus) = *iter->second;
            }
            if (partition->CurrentJobId != newStatus->CurrentJobId) {
                newStatus->CurrentJobId = partition->CurrentJobId;
                newStatus->CurrentJobStatusUpdateTime = now;
                newStatus->CurrentJobStatus = {};
            }
            feedback->PartitionJobStatuses.emplace(partitionId, newStatus);
        }

        for (auto& [jobId, status] : freshJobStatuses) {
            YT_VERIFY(status);
            YT_VERIFY(jobId == status->JobId);
            const auto jobPtr = currentLayout->Jobs.FindPtr(jobId);
            if (!jobPtr) {
                continue;
            }
            const auto job = *jobPtr;
            const auto partitionId = job->PartitionId;
            const auto partitionPtr = currentLayout->Partitions.FindPtr(partitionId);
            if (!partitionPtr) {
                continue;
            }
            const auto partition = *partitionPtr;
            if (partition->CurrentJobId != std::optional(jobId)) {
                continue;
            }

            auto& partitionJobStatus = GetOrCrash(feedback->PartitionJobStatuses, partitionId);
            const auto previousJobStatus = partitionJobStatus->CurrentJobStatus;
            partitionJobStatus->CurrentJobStatus = status;
            partitionJobStatus->CurrentJobStatusUpdateTime = now;
            if (status->FromPartitionTraverseData) {
                partitionJobStatus->LastTraverseData = status->FromPartitionTraverseData;
            }
            if (status->PartitionStatus) {
                partitionJobStatus->LastPartitionStatus = status->PartitionStatus;
            }

            if (!previousJobStatus && status) {
                YT_TLOG_INFO("Received first job status")
                    .With("JobId", jobId)
                    .With("PartitionId", partition->PartitionId)
                    .With("ComputationId", partition->ComputationId);
            }

            if (!status->IsFinished) {
                for (const auto& [component, error] : status->RetryableErrors) {
                    TError* currentError = previousJobStatus ? previousJobStatus->RetryableErrors.FindPtr(component) : nullptr;
                    if (!currentError || *currentError != error) {
                        partitionJobStatus->LastRetryableErrorInstant = std::min(std::max(partitionJobStatus->LastRetryableErrorInstant, error.GetDatetime()), now);
                        YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Received job retryable error")
                            .With("Component", component)
                            .With("JobId", jobId)
                            .With("PartitionId", partition->PartitionId)
                            .With("ComputationId", partition->ComputationId)
                            .With(error);
                    }
                }
            }
        }

        feedback->WorkerStatuses = flowView->Feedback->WorkerStatuses; // THashMap is copied, values are reused.
        DropMissingKeys(feedback->WorkerStatuses, flowView->State->Workers);
        for (const auto& [workerAddress, newWorkerStatus] : freshWorkerStatuses) {
            auto& workerStatus = feedback->WorkerStatuses[workerAddress];
            if (!newWorkerStatus->PreviousCrashError.IsOK() && (!workerStatus || workerStatus->PreviousCrashError != newWorkerStatus->PreviousCrashError)) {
                YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Received worker crash error")
                    .With("WorkerAddress", workerAddress)
                    .With(newWorkerStatus->PreviousCrashError);
            }
            for (const auto& [component, error] : newWorkerStatus->Errors) {
                TError* currentError = workerStatus ? workerStatus->Errors.FindPtr(component) : nullptr;
                if (!currentError || *currentError != error) {
                    YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Warning, "Received worker error")
                        .With("Component", component)
                        .With("WorkerAddress", workerAddress)
                        .With(error);
                }
            }
            workerStatus = newWorkerStatus;
        }

        return feedback;
    }
};

////////////////////////////////////////////////////////////////////////////////

IControllerPtr CreateController(
    TControllerConfigPtr config,
    TNodeInfoPtr controllerNodeInfo,
    IWorkerTrackerPtr workerTracker,
    IThrottlerHostPtr throttlerHost,
    IInvokerPtr invoker,
    IYTConnectorPtr connector,
    IPersistedStateManagerPtr stateManager,
    IPipelineAuthenticatorPtr authenticator,
    bool ignoreSingletonsDynamicConfig,
    NObjectClient::TCellTag clockClusterTag,
    IStatusProfilerPtr rootStatusProfiler)
{
    return New<TController>(
        std::move(config),
        std::move(controllerNodeInfo),
        std::move(workerTracker),
        std::move(throttlerHost),
        std::move(invoker),
        std::move(connector),
        std::move(stateManager),
        std::move(authenticator),
        ignoreSingletonsDynamicConfig,
        clockClusterTag,
        std::move(rootStatusProfiler));
}

////////////////////////////////////////////////////////////////////////////////

void SyncTraverseDataWithSpec(const TFlowViewPtr& flowView)
{
    const auto& executionSpec = flowView->State->ExecutionSpec;
    const auto& pipelineSpec = executionSpec->PipelineSpec->GetValue();
    const auto& extendedPipelineSpec = executionSpec->ExtendedPipelineSpec->GetValue();
    const auto& traverseData = flowView->State->TraverseData;

    // New stream can be added only when pipeline is stopped. So every new message will get greater or equal system timestamp.
    // Use this default to achieve 2 goals:
    // * Better behaviour in monitorings.
    // * Zero flap in traverseData->Streams (YTFLOW-447).
    const auto& defaultSystemWatermark = flowView->State->CurrentTimestamp;

    // Add new streams with default system watermark, remove old ones.
    auto normalizeStreamsTraverseData = [&] (auto& streams, const auto& desiredStreamIds) {
        for (const auto& streamId : desiredStreamIds) {
            auto [it, inserted] = streams.emplace(streamId, New<TStreamTraverseData>());
            if (inserted) {
                it->second->SystemWatermark = defaultSystemWatermark;
            }
        }
        DropMissingKeys(streams, desiredStreamIds);
    };

    THashSet<TStreamId> allStreamIds;
    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
        const auto& computationStreamIds = GetOrCrash(extendedPipelineSpec->Computations, computationId)->AllStreamIds;

        auto it = traverseData->Computations.emplace(computationId, New<TNodeTraverseData>()).first;
        normalizeStreamsTraverseData(it->second->Streams, computationStreamIds);

        for (const auto& streamId : computationStreamIds) {
            allStreamIds.insert(MakeGlobalStreamId(computationId, streamId, computationSpec));
        }
    }
    DropMissingKeys(traverseData->Computations, pipelineSpec->Computations);

    normalizeStreamsTraverseData(traverseData->Streams, allStreamIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
