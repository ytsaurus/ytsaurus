#include "job.h"

#include "buffer_state_manager.h"
#include "input_buffer.h"
#include "job_spec.h"

#include "message_distributor.h"
#include "private.h"
#include <yt/yt/flow/library/cpp/computation/stores/output_store.h>

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/common/message_migration.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/computation/message_filter.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/client.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/config.h>

#include <yt/yt/flow/library/cpp/misc/remedian_splitter.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/lib/native_client/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/heavy_hitters/misra_gries.h>

#include <library/cpp/yt/string/guid.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NFlow::NWorker {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

using TKeyHeavyHittersCounter = TMisraGriesHeavyHitters<TKey>;
using TKeyHeavyHittersCounterPtr = TIntrusivePtr<TKeyHeavyHittersCounter>;

////////////////////////////////////////////////////////////////////////////////

class TJob;
class TComputationRunContext;

////////////////////////////////////////////////////////////////////////////////

void TJobOrchidState::Register(TRegistrar registrar)
{
    registrar.Parameter("status", &TThis::Status)
        .Default();
    registrar.Parameter("computation", &TThis::Computation)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TError MakeExecutionInterruptedError()
{
    return TError("Job execution has been interrupted");
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TComputationRunContext
    : public IComputationRunContext
{
public:
    explicit TComputationRunContext(TWeakPtr<TJob> job);

    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(const THashSet<TStreamId>& allowedStreams) override;
    void MarkPersisted(std::span<const TMessageId> messageIds) override;
    void RegisterOutputMessages(
        std::span<const TOutputMessageConstPtr> messages,
        std::span<TDistributingTracker> trackers) override;
    void Commit() override;
    TSystemTimestamp GetInputStabilizedEventTimestamp() const override;

private:
    const TWeakPtr<TJob> Job_;
};

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public IJob
{
public:
    TJob(
        TJobContextPtr jobContext,
        TJobStreamLimitUsageStates streamLimitUsageStates,
        i64 specGeneration,
        TJobSpecPtr jobSpec,
        TDynamicJobSpecPtr dynamicJobSpec,
        TToPartitionTraverseDataPtr traverseData,
        TWatermarkStatePtr watermarkState)
        : JobContext_(std::move(jobContext))
        , StreamLimitUsageStates_(std::move(streamLimitUsageStates))
        , SpecGeneration_(specGeneration)
        , JobSpec_(std::move(jobSpec))
        , DynamicJobSpec_(std::move(dynamicJobSpec))
        , TraverseData_(std::move(traverseData))
        , WatermarkState_(std::move(watermarkState))
        , Logger(
            WorkerLogger().WithTag("JobId: %v, PartitionId: %v, ComputationId: %v",
                JobSpec_->Job->JobId,
                JobSpec_->Partition->PartitionId,
                JobSpec_->Partition->ComputationId))
        , Profiler(
            WorkerProfiler()
                .WithTag("computation_id", JobSpec_->Partition->ComputationId.Underlying())
                .WithPrefix("/computation"))
        , JobRootStatusProfiler_(CreateStatusProfiler())
        , Distributor_(JobContext_->MessageDistributor)
        , InputBuffer_(
            CreateInputBuffer(
                JobSpec_->Job->JobId,
                StreamLimitUsageStates_.Input,
                JobSpec_->ComputationSpec,
                JobSpec_->Partition->ComputationId,
                DynamicJobSpec_->DynamicComputationSpec,
                JobContext_->PoolInvoker,
                Profiler))
        , ControlSerializedInvoker_(JobContext_->ControlSerializedInvoker)
        , JobSerializedInvoker_(JobContext_->SerializedInvoker)
        , EvaluatorCache_(JobContext_->EvaluatorCache)
        , MessageFilter_(CreateMessageFilter(DynamicJobSpec_->DynamicComputationSpec->SkipIfExpression))
        , SkippedByExpressionCounter_(Profiler.WithPrefix("/input_streams").Counter("/skipped_by_expression_count"))
        , GlobalHeavyHittersCounter_(CreateGlobalHeavyHitterCounter(JobSpec_->ComputationSpec))
        , StreamHeavyHittersCounters_(CreateStreamHeavyHitterCounters(JobSpec_->ComputationSpec))
        , GlobalInputBytesCounter_(CreateGlobalInputBytesCounter(JobSpec_->ComputationSpec))
        , StreamInputBytesCounters_(CreateStreamInputBytesCounters(JobSpec_->ComputationSpec))
        , JobInputMetrics_(New<TNodeInputMetrics>())
    {
        YT_VERIFY(
            JobSpec_->Partition->State == EPartitionState::Executing ||
            JobSpec_->Partition->State == EPartitionState::Completing ||
            JobSpec_->Partition->State == EPartitionState::Interrupting);

        const auto& pivotFinder = JobSpec_->ComputationSpec->PivotFinder;
        if (pivotFinder.PartCount >= 2) {
            RemedianSplitter_.emplace(pivotFinder.PartCount, pivotFinder.WindowSize);
        }
    }

    TFuture<void> Start() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND_NO_PROPAGATE(&TJob::StartSync, MakeWeak(this))
            .AsyncVia(ControlSerializedInvoker_)
            .Run()
            .ToUncancelable();
    }

    void StartSync()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlSerializedInvoker_);

        YT_VERIFY(!ExecutorFiberFuture_, "Job cannot be started twice");
        YT_VERIFY(!IsStoppingRequested_, "Job cannot be started after stopping");

        StartTime_ = TInstant::Now();
        IsRunning_.store(true);
        YT_LOG_INFO("Job execution started");
        ExecutorFiberFuture_ = BIND(&TJob::ExecutorFiber, MakeStrong(this))
            .AsyncVia(JobSerializedInvoker_)
            .Run()
            .ToImmediatelyCancelable();
        ExecutorFiberFuture_.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& result) {
            if (result.IsOK()) {
                YT_LOG_INFO("Job execution completed");
            } else if (result.FindMatching(EErrorCode::GracefulShutdown)) {
                // Graceful rebalance signals job completion via a typed error (the controller then
                // recreates the job on the target worker). This is a normal lifecycle event, not a
                // failure, so log it at Info like an ordinary completion rather than at Error.
                YT_LOG_INFO(result, "Job finished by graceful shutdown signal");
            } else {
                YT_LOG_ERROR(result, "Job execution failed");
            }
            IsFinished_ = true;
            FinishTime_ = TInstant::Now();
            ResultError_ = result;
        }).Via(ControlSerializedInvoker_));
    }

    TFuture<void> Reconfigure(i64 specGeneration, TDynamicJobSpecPtr dynamicJobSpec) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Scheduled job reconfigure");

        ReconfigureFuture_ = ReconfigureFuture_
            .Apply(BIND_NO_PROPAGATE(&TJob::ReconfigureSync, MakeWeak(this), specGeneration, dynamicJobSpec)
                    .AsyncVia(ControlSerializedInvoker_))
            .ToUncancelable();

        return ReconfigureFuture_;
    }

    void ReconfigureSync(i64 specGeneration, TDynamicJobSpecPtr dynamicJobSpec)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlSerializedInvoker_);

        YT_LOG_DEBUG("Job reconfiguration started (OldSpecGeneration: %v, NewSpecGeneration: %v, Draining: %v)",
            SpecGeneration_,
            specGeneration,
            dynamicJobSpec->DynamicComputationSpec->Draining);

        SpecGeneration_ = specGeneration;
        DynamicJobSpec_ = std::move(dynamicJobSpec);
        YT_VERIFY(InitializePromise_.IsSet());

        auto dynamicComputationContext = New<TDynamicComputationContext>();
        dynamicComputationContext->SpecGeneration = SpecGeneration_;
        dynamicComputationContext->DynamicComputationSpec = DynamicJobSpec_->DynamicComputationSpec;
        dynamicComputationContext->DynamicPartitionSpec = DynamicJobSpec_->DynamicComputationPartitionSpec;
        dynamicComputationContext->Throttlers = DynamicJobSpec_->Throttlers;
        YT_VERIFY(Computation_);
        Computation_->Reconfigure(dynamicComputationContext);

        InputBuffer_->Reconfigure(DynamicJobSpec_->DynamicComputationSpec);

        MessageFilter_->Reconfigure(DynamicJobSpec_->DynamicComputationSpec->SkipIfExpression);

        YT_LOG_DEBUG("Job reconfiguration completed (Draining: %v)",
            DynamicJobSpec_->DynamicComputationSpec->Draining);
    }

    TFuture<void> UpdateWatermarkState(TWatermarkStatePtr watermarkState) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        ReconfigureFuture_ = ReconfigureFuture_
            .Apply(BIND_NO_PROPAGATE(&TJob::UpdateWatermarkStateSync, MakeWeak(this), watermarkState)
                    .AsyncVia(ControlSerializedInvoker_))
            .ToUncancelable();

        return ReconfigureFuture_;
    }

    void UpdateWatermarkStateSync(TWatermarkStatePtr watermarkState)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlSerializedInvoker_);

        YT_LOG_DEBUG("Watermark state updated");
        WatermarkState_ = std::move(watermarkState);
        YT_VERIFY(InitializePromise_.IsSet());
        Computation_->UpdateWatermarkState(WatermarkState_);
    }

    TFuture<void> UpdateTraverseData(TToPartitionTraverseDataPtr traverse) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        ReconfigureFuture_ = ReconfigureFuture_
            .Apply(BIND_NO_PROPAGATE(&TJob::UpdateTraverseDataSync, MakeWeak(this), traverse)
                    .AsyncVia(ControlSerializedInvoker_))
            .ToUncancelable();

        return ReconfigureFuture_;
    }

    void UpdateTraverseDataSync(TToPartitionTraverseDataPtr traverse)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlSerializedInvoker_);

        TraverseData_ = std::move(traverse);
        YT_VERIFY(InitializePromise_.IsSet());
        Computation_->SetInputTraverse(TraverseData_->InputStreams);
        YT_LOG_DEBUG("Traverse data updated");
    }

    void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        InputBuffer_->UpdateMessageTransferingInfo(messageTransferingInfo);
    }

    TFuture<void> Stop() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Stop(MakeExecutionInterruptedError());
    }

    TFuture<void> Stop(TError error) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        IsRunning_.store(false);
        return BIND_NO_PROPAGATE(&TJob::StopSync, MakeWeak(this), std::move(error))
            .AsyncVia(ControlSerializedInvoker_)
            .Run()
            .ToUncancelable();
    }

    void StopSync(const TError& error)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlSerializedInvoker_);

        if (IsStoppingRequested_) {
            return;
        }

        IsStoppingRequested_ = true;

        if (!ExecutorFiberFuture_ || IsFinished_) {
            return;
        }
        YT_LOG_INFO("Job stopping (Error: %v)",
            error);
        ExecutorFiberFuture_.Cancel(error);
    }

    TJobId GetJobId() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return JobSpec_->Job->JobId;
    }

    TComputationId GetComputationId() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return JobSpec_->Partition->ComputationId;
    }

    IInputBufferPtr GetInputBuffer() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return InputBuffer_;
    }

    TSystemTimestamp GetInputStabilizedEventTimestamp()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return InputBuffer_->GetMinStabilizedEventTimestamp();
    }

    TFuture<TJobStatusPtr> GetStatus() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND_NO_PROPAGATE(&TJob::GetStatusSync, MakeStrong(this))
            .AsyncVia(ControlSerializedInvoker_)
            .Run()
            .ToUncancelable();
    }

    TJobStatusPtr GetStatusSync()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlSerializedInvoker_);

        auto status = New<TJobStatus>();
        status->JobId = GetJobId();
        status->IsFinished = IsFinished_;
        status->StartTime = StartTime_;
        status->FinishTime = FinishTime_;
        status->UpdateTime = TInstant::Now();
        status->Error = ResultError_;

        status->Epoch = SpecGeneration_;

        if (IsRunning_.load() && InitializePromise_.IsSet()) {
            try {
                auto computationStatus = Computation_->GetStatus();
                if (computationStatus->NodeTraverse) {
                    auto traverseData = New<TFromPartitionTraverseData>();
                    traverseData->Node = computationStatus->NodeTraverse;
                    // NB: No need to clamp stream->Epoch here — GetNodeTraverse() already returns nullptr
                    // if any stream's Epoch < PendingSpecGeneration_, so the traverse data is always fresh.
                    status->FromPartitionTraverseData = traverseData;
                    if (!GotFirstTraverseDataTime_.has_value()) {
                        GotFirstTraverseDataTime_ = status->UpdateTime;
                    }
                    status->InitedTime = GotFirstTraverseDataTime_;

                    YT_LOG_INFO("Report status (TraverseData: %v)",
                        ConvertToYsonString(status->FromPartitionTraverseData, NYson::EYsonFormat::Text));

                    if (!JobSpec_->ComputationSpec->InputStreamIds.empty()) {
                        auto guard = Guard(Lock_);
                        status->InputMetrics = JobInputMetrics_;
                    }
                }
                status->PartitionStatus = computationStatus->PartitionStatus;
                status->EpochPartTimes = computationStatus->EpochPartTimes;
                // Get internal computation limits. Will be enriched later.
                status->InputLimits = computationStatus->InputLimits;
                status->OutputLimits = computationStatus->OutputLimits;

                status->RetryableErrors = std::move(JobRootStatusProfiler_->GetStatus().Errors);

                // Limit/Used/Pending are all reported in inflated bytes (raw payload plus the
                // per-message technical cost), so back-pressure and status read in the same units.
                auto fillBufferLimits = [] (const auto& name, const NFlow::TStreamLimitUsageStateMap& states, auto& allLimits) {
                    if (states.empty()) {
                        return;
                    }
                    auto& limits = allLimits[name];
                    for (const auto& [streamId, state] : states) {
                        auto usage = state->Read();
                        auto& entityLimitStatus = limits[streamId];
                        entityLimitStatus.Limit = state->GetLimitBytes();
                        entityLimitStatus.Used = usage.GetInflatedInflightBytes(state->GetInflationPerMessage());
                        entityLimitStatus.Pending = usage.PendingInflatedBytes;
                    }
                };

                fillBufferLimits("input_buffer_bytes", StreamLimitUsageStates_.Input, status->InputLimits);
                fillBufferLimits("output_buffer_bytes", StreamLimitUsageStates_.Output, status->OutputLimits);
            } catch (const std::exception& ex) {
                status->Error = TError(ex);
                status->FromPartitionTraverseData = nullptr;
                StopSync(status->Error);
            }
        }
        return status;
    }

    TFuture<TJobOrchidStatePtr> GetOrchidState() override
    {
        return BIND_NO_PROPAGATE(&TJob::GetOrchidStateSync, MakeStrong(this))
            .AsyncVia(ControlSerializedInvoker_)
            .Run()
            .ToUncancelable();
    }

    TJobOrchidStatePtr GetOrchidStateSync()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlSerializedInvoker_);

        auto orchidState = New<TJobOrchidState>();
        orchidState->Status = GetStatusSync();
        if (IsRunning_.load() && InitializePromise_.IsSet()) {
            orchidState->Computation = Computation_->GetOrchidState();
        }
        return orchidState;
    }

    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(const THashSet<TStreamId>& allowedStreams)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (JobSpec_->ComputationSpec->InputStreamIds.empty()) {
            return MakeFuture(std::vector<TInputMessageConstPtr>{});
        }

        return InputBuffer_->GetInputBatch(allowedStreams)
            .AsUnique()
            .Apply(BIND([weakThis = MakeWeak(this)] (TErrorOr<std::vector<TInputMessageConstPtr>>&& errorOrMessages) -> std::vector<TInputMessageConstPtr> {
                if (auto this_ = weakThis.Lock()) {
                    auto inputMessages = std::move(errorOrMessages).ValueOrThrow();
                    if (this_->MessageFilter_->IsEnabled()) {
                        inputMessages = this_->DropSkippedMessages(std::move(inputMessages));
                    }
                    this_->RegisterInputBatch(inputMessages);
                    return inputMessages;
                } else {
                    THROW_ERROR_EXCEPTION(MakeExecutionInterruptedError());
                }
            }).AsyncVia(JobContext_->PoolInvoker));
    }

    void MarkPersisted(std::span<const TMessageId> messageIds)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (messageIds.empty()) {
            return;
        }

        // Auto-batching of persisted messages.
        bool queueWasEmpty = false;
        {
            auto guard = Guard(MarkPersistedQueueLock_);
            queueWasEmpty = MarkPersistedQueue_.empty();
            MarkPersistedQueue_.insert(MarkPersistedQueue_.end(), messageIds.begin(), messageIds.end());
        }
        if (queueWasEmpty) {
            JobSerializedInvoker_->Invoke(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] () {
                std::deque<TMessageId> queuedMessageIds;
                {
                    auto guard = Guard(MarkPersistedQueueLock_);
                    std::swap(queuedMessageIds, MarkPersistedQueue_);
                }
                InputBuffer_->MarkPersisted(std::move(queuedMessageIds));
            }));
        }
    }

    void RegisterOutputMessages(
        std::span<const TOutputMessageConstPtr> messages,
        std::span<TDistributingTracker> trackers)
    {
        YT_VERIFY(messages.size() == trackers.size());
        auto guard = Guard(DistributingLock_);
        for (int i = 0; i < std::ssize(messages); ++i) {
            const auto& message = messages[i];
            auto& tracker = trackers[i];
            auto it = JobSpec_->ExtendedComputationSpec->Subscribers.find(message->StreamId);
            if (it == JobSpec_->ExtendedComputationSpec->Subscribers.end() || it->second.empty()) {
                continue;
            }
            for (const auto& computationId : it->second) {
                DistributingQueue_.push_back(TDistributorOutputMessage{
                    .ComputationId = computationId,
                    .Message = message,
                    .OnDistributed = tracker.AddDestination(),
                });
            }
        }
    }

    void Commit()
    {
        std::deque<TDistributorOutputMessage> toDistribute;
        {
            auto guard = Guard(DistributingLock_);
            std::swap(toDistribute, DistributingQueue_);
        }
        if (toDistribute.empty()) {
            return;
        }
        Distributor_->DistributeOutputMessages(
            GetJobId(),
            std::move(toDistribute));
    }

    const IInvokerPtr& GetJobInvoker()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return JobSerializedInvoker_;
    }

private:
    const TJobContextPtr JobContext_;
    const TJobStreamLimitUsageStates StreamLimitUsageStates_;
    i64 SpecGeneration_;
    const TJobSpecPtr JobSpec_;
    TDynamicJobSpecPtr DynamicJobSpec_;
    TToPartitionTraverseDataPtr TraverseData_;
    TWatermarkStatePtr WatermarkState_;

    TPromise<void> InitializePromise_ = NewPromise<void>();
    IComputationPtr Computation_ = nullptr; // Can be used after InitializePromise_ is set.
    TFuture<void> ReconfigureFuture_ = InitializePromise_.ToFuture();

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;
    const IStatusProfilerPtr JobRootStatusProfiler_;

    const IMessageDistributorPtr Distributor_;

    const IInputBufferPtr InputBuffer_;
    const IInvokerPtr ControlSerializedInvoker_;
    const IInvokerPtr JobSerializedInvoker_;
    const NQueryClient::IColumnEvaluatorCachePtr EvaluatorCache_;

    const IMessageFilterPtr MessageFilter_;
    const NProfiling::TCounter SkippedByExpressionCounter_;

    std::atomic<bool> IsRunning_ = false;

    TInstant StartTime_;
    std::optional<TInstant> GotFirstTraverseDataTime_;

    bool IsStoppingRequested_ = false;
    bool IsFinished_ = false;
    std::optional<TInstant> FinishTime_;
    TError ResultError_;

    TFuture<void> ExecutorFiberFuture_;

    TKeyHeavyHittersCounterPtr GlobalHeavyHittersCounter_;
    THashMap<TStreamId, TKeyHeavyHittersCounterPtr> StreamHeavyHittersCounters_;
    TEmaCounter<double, 1> GlobalInputBytesCounter_;
    THashMap<TStreamId, TEmaCounter<double, 1>> StreamInputBytesCounters_;
    std::optional<TRemedianSplitter<TKey>> RemedianSplitter_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TNodeInputMetricsPtr JobInputMetrics_;

    struct TDeliveryLogEntry
    {
        TMessageId MessageId;
        std::optional<TMessage> Message;
        TComputationId DestinationComputation;
    };

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DistributingLock_);
    std::deque<TDistributorOutputMessage> DistributingQueue_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, MarkPersistedQueueLock_);
    std::deque<TMessageId> MarkPersistedQueue_;

private:
    void ExecutorFiber()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(JobSerializedInvoker_);

        YT_LOG_INFO("Computation construction started");
        YT_VERIFY(IsRunning_.load() == true);
        YT_VERIFY(InitializePromise_.IsSet() == false);
        Computation_ = CreateComputation(
            SpecGeneration_,
            JobSpec_,
            DynamicJobSpec_,
            WatermarkState_,
            TraverseData_,
            JobContext_,
            StreamLimitUsageStates_.Output,
            JobSerializedInvoker_,
            JobContext_->PoolInvoker,
            Logger,
            Profiler,
            JobRootStatusProfiler_);
        InitializePromise_.Set();
        YT_LOG_INFO("Computation::Run started");
        auto context = New<TComputationRunContext>(MakeWeak(this));
        Computation_->Run(context);
        YT_LOG_INFO("Computation::Run completed");
    }

    std::vector<TInputMessageConstPtr> DropSkippedMessages(std::vector<TInputMessageConstPtr> messages)
    {
        auto [kept, skipped] = MessageFilter_->Partition(std::move(messages));

        if (!skipped.empty()) {
            std::vector<TMessageId> skippedMessageIds;
            skippedMessageIds.reserve(skipped.size());
            for (const auto& message : skipped) {
                skippedMessageIds.push_back(message->MessageId);
            }
            SkippedByExpressionCounter_.Increment(skippedMessageIds.size());
            YT_LOG_INFO("Skipped input messages by expression (Skipped: %v, Kept: %v)",
                skippedMessageIds.size(),
                kept.size());
            MarkPersisted(skippedMessageIds);
        }

        return std::move(kept);
    }

    void RegisterInputBatch(const std::vector<TInputMessageConstPtr>& messages)
    {
        if (messages.empty()) {
            return;
        }

        auto now = TInstant::Now();

        struct TStreamState
        {
            std::vector<TKey> Keys;
            i64 Bytes = 0;
        };

        THashMap<TStreamId, TStreamState> streamStates;
        for (const auto& message : messages) {
            auto& streamState = streamStates[message->StreamId];
            streamState.Keys.push_back(message->Key);
            streamState.Bytes += message->ByteSize;
        }

        i64 totalBytes = 0;
        for (const auto& [streamId, streamState] : streamStates) {
            totalBytes += streamState.Bytes;
            GetOrCrash(StreamInputBytesCounters_, streamId).Update(streamState.Bytes, now);
        }
        GlobalInputBytesCounter_.Update(totalBytes, now);

        std::vector<TKey> allKeys;
        for (const auto& [streamId, streamState] : streamStates) {
            GetOrCrash(StreamHeavyHittersCounters_, streamId)->Register(streamState.Keys, now);
            for (auto& key : streamState.Keys) {
                allKeys.push_back(key);
            }
        }
        GlobalHeavyHittersCounter_->Register(allKeys, now);

        auto inputMetrics = New<TNodeInputMetrics>();
        auto globalStats = GlobalHeavyHittersCounter_->GetStatistics(now);
        inputMetrics->Global.MessagesPerSecond = globalStats.Total;
        inputMetrics->Global.BytesPerSecond = GlobalInputBytesCounter_.GetRate(0).value_or(0);
        for (const auto& [key, fraction] : globalStats.Fractions) {
            inputMetrics->Global.HeavyHitters.emplace_back(fraction, key);
        }

        if (RemedianSplitter_) {
            for (auto& key : allKeys) {
                RemedianSplitter_->Push(std::move(key));
            }
            allKeys.clear();
            if (RemedianSplitter_->IsResultReady()) {
                inputMetrics->Global.Pivots = RemedianSplitter_->Result();
            }
        }

        for (const auto& [streamId, counter] : StreamHeavyHittersCounters_) {
            auto stats = counter->GetStatistics(now);
            inputMetrics->Streams[streamId].MessagesPerSecond = stats.Total;
            for (const auto& [key, fraction] : stats.Fractions) {
                inputMetrics->Streams[streamId].HeavyHitters.emplace_back(fraction, key);
            }
        }
        for (const auto& [streamId, counter] : StreamInputBytesCounters_) {
            inputMetrics->Streams[streamId].BytesPerSecond = counter.GetRate(0).value_or(0);
        }

        {
            auto guard = Guard(Lock_);
            std::swap(JobInputMetrics_, inputMetrics);
        }
    }

    static IComputationPtr CreateComputation(
        i64 specGeneration,
        const TJobSpecPtr& jobSpec,
        const TDynamicJobSpecPtr& dynamicJobSpec,
        const TWatermarkStatePtr& watermarkState,
        const TToPartitionTraverseDataPtr& partitionData,
        const TJobContextPtr& jobContext,
        const NFlow::TStreamLimitUsageStateMap& outputStreamLimitUsageStates,
        const IInvokerPtr& jobSerializedInvoker,
        const IInvokerPtr& jobPoolInvoker,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler,
        const IStatusProfilerPtr& statusProfiler)
    {
        auto computationContext = New<TComputationContext>();
        computationContext->ComputationSpec = jobSpec->ComputationSpec;
        computationContext->ClientsCache = jobContext->ClientsCache;
        computationContext->PipelinePath = jobContext->PipelinePath;
        computationContext->Partition = jobSpec->Partition;
        computationContext->Job = jobSpec->Job;
        computationContext->SerializedInvoker = jobSerializedInvoker;
        computationContext->PoolInvoker = jobPoolInvoker;
        computationContext->Logger = logger;
        computationContext->Profiler = profiler;
        computationContext->StatusProfiler = statusProfiler;
        computationContext->ClockClusterTag = jobContext->ClockClusterTag;
        computationContext->EvaluatorCache = jobContext->EvaluatorCache;
        computationContext->ConverterCache = jobContext->ConverterCache;
        computationContext->LoadThroughputThrottler = jobContext->LoadThroughputThrottler;
        computationContext->OutputStreamLimitUsageStates = outputStreamLimitUsageStates;
        computationContext->StreamSpecStorage = jobContext->StreamSpecStorage;
        computationContext->JobStateCache = jobContext->JobStateCache;
        computationContext->ExternalMetricsReporter = jobContext->ExternalMetricsReporter;
        computationContext->HttpClient = jobContext->HttpClient;
        computationContext->HttpsClient = jobContext->HttpsClient;
        computationContext->Poller = jobContext->Poller;
        auto dynamicComputationContext = New<TDynamicComputationContext>();
        dynamicComputationContext->SpecGeneration = specGeneration;
        dynamicComputationContext->DynamicComputationSpec = dynamicJobSpec->DynamicComputationSpec;
        dynamicComputationContext->DynamicPartitionSpec = dynamicJobSpec->DynamicComputationPartitionSpec;
        dynamicComputationContext->Throttlers = dynamicJobSpec->Throttlers;

        THashSet<TResourceId> requiredResourceIds;
        for (const auto& [resourceId, resourceDescription] : jobSpec->ComputationSpec->RequiredResourceIds) {
            if (!resourceDescription->Worker) {
                continue;
            }

            requiredResourceIds.insert(resourceId);
            auto aliasResourceId = resourceDescription->Alias ? *resourceDescription->Alias : resourceId;
            EmplaceOrCrash(computationContext->StaticResources, aliasResourceId, jobContext->ResourceManager->Get(resourceId));
        }

        // LoadRequiredResources also awaits the always-on resources (loaded eagerly, outside
        // RequiredResourceIds), so it is called even when requiredResourceIds is empty. This is a
        // context-switch-allowed point, unlike the resource-manager construction in the heartbeat path.
        WaitFor(jobContext->ResourceManager->LoadRequiredResources(requiredResourceIds))
            .ThrowOnError();

        computationContext->DistributedThrottlerControllerChannelProvider =
            jobContext->DistributedThrottlerControllerChannelProvider;

        auto computation = TRegistry::Get()->CreateComputation(
            computationContext,
            dynamicComputationContext);

        // Fire-and-forget: new spec will be applied at the start of the first run iteration.
        computation->UpdateWatermarkState(watermarkState);
        computation->SetInputTraverse(partitionData->InputStreams);

        return computation;
    }

    static TKeyHeavyHittersCounterPtr CreateGlobalHeavyHitterCounter(const TComputationSpecPtr& spec)
    {
        return New<TKeyHeavyHittersCounter>(spec->HeavyHitters.Threshold, spec->HeavyHitters.Window, spec->HeavyHitters.Limit);
    }

    static THashMap<TStreamId, TKeyHeavyHittersCounterPtr> CreateStreamHeavyHitterCounters(const TComputationSpecPtr& spec)
    {
        THashMap<TStreamId, TKeyHeavyHittersCounterPtr> counters;
        for (const auto& streamId : spec->InputStreamIds) {
            counters[streamId] = New<TKeyHeavyHittersCounter>(spec->HeavyHitters.Threshold, spec->HeavyHitters.Window, spec->HeavyHitters.Limit);
        }
        return counters;
    }

    static TEmaCounter<double, 1> CreateGlobalInputBytesCounter(const TComputationSpecPtr& spec)
    {
        // Use the same window as for messages counter (embedded in heavy hitters counter).
        return TEmaCounter<double, 1>({spec->HeavyHitters.Window});
    }

    static THashMap<TStreamId, TEmaCounter<double, 1>> CreateStreamInputBytesCounters(const TComputationSpecPtr& spec)
    {
        THashMap<TStreamId, TEmaCounter<double, 1>> counters;
        for (const auto& streamId : spec->InputStreamIds) {
            // Use the same window as for messages counter (embedded in heavy hitters counter).
            counters.emplace(streamId, TEmaCounter<double, 1>({spec->HeavyHitters.Window}));
        }
        return counters;
    }
};

////////////////////////////////////////////////////////////////////////////////

TComputationRunContext::TComputationRunContext(TWeakPtr<TJob> job)
    : Job_(std::move(job))
{ }

TFuture<std::vector<TInputMessageConstPtr>> TComputationRunContext::GetNextBatch(const THashSet<TStreamId>& allowedStreams)
{
    if (auto job = Job_.Lock()) {
        return job->GetNextBatch(allowedStreams);
    }
    return MakeFuture<std::vector<TInputMessageConstPtr>>(MakeExecutionInterruptedError());
}

void TComputationRunContext::MarkPersisted(std::span<const TMessageId> messageIds)
{
    if (auto job = Job_.Lock()) {
        return job->MarkPersisted(messageIds);
    }
}

void TComputationRunContext::RegisterOutputMessages(
    std::span<const TOutputMessageConstPtr> messages,
    std::span<TDistributingTracker> trackers)
{
    if (auto job = Job_.Lock()) {
        job->RegisterOutputMessages(messages, trackers);
        return;
    }
    THROW_ERROR(MakeExecutionInterruptedError());
}

void TComputationRunContext::Commit()
{
    if (auto job = Job_.Lock()) {
        return job->Commit();
    }
}

TSystemTimestamp TComputationRunContext::GetInputStabilizedEventTimestamp() const
{
    if (auto job = Job_.Lock()) {
        return job->GetInputStabilizedEventTimestamp();
    }
    return InfinitySystemTimestamp;
}

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateJob(
    TJobContextPtr jobContext,
    TJobStreamLimitUsageStates streamLimitUsageStates,
    i64 specGeneration,
    TJobSpecPtr jobSpec,
    TDynamicJobSpecPtr dynamicJobSpec,
    TToPartitionTraverseDataPtr traverseData,
    TWatermarkStatePtr watermarkState)
{
    return New<TJob>(
        std::move(jobContext),
        std::move(streamLimitUsageStates),
        specGeneration,
        std::move(jobSpec),
        std::move(dynamicJobSpec),
        std::move(traverseData),
        std::move(watermarkState));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
