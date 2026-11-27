#include "transform_ordered_source_computation.h"

#include "computation_tracer.h"
#include "message_filter.h"
#include "meta_setter.h"
#include "read_delay.h"
#include "stores/input_store.h"
#include "stores/output_store.h"
#include "stores/timer_store.h"
#include "watermark_aligner.h"

#include "job_state/state_manager.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/source.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NTracing;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// Held reads are dropped on restart and reprocessed, which is exactly-once only without mutable state.
class TReadDelayJobInitContext
    : public IJobInitContext
{
public:
    TReadDelayJobInitContext(TJobStateManagerPtr stateManager, IJobInitContextPtr underlying)
        : StateManager_(std::move(stateManager))
        , Underlying_(std::move(underlying))
    { }

    void RegisterAsyncProcessor(TAsyncProcessorDescriptorPtr descriptor) const override
    {
        Underlying_->RegisterAsyncProcessor(std::move(descriptor));
    }

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> /*ctor*/) const override
    {
        return MakeFuture<IMutableStateKeyProviderPtr>(MakeError());
    }

    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override
    {
        return Underlying_->CreateJoinedStateKeyProvider(std::move(ctor));
    }

    IInitContextPtr AsPartition() const override
    {
        THROW_ERROR MakeError();
    }

    IInitContextPtr AsKey(TKey /*key*/) const override
    {
        THROW_ERROR MakeError();
    }

    IJobInitContextPtr WithPrefix(TStringBuf prefix) const override
    {
        return New<TReadDelayJobInitContext>(StateManager_, Underlying_->WithPrefix(prefix));
    }

    const std::string& GetPrefix() const override
    {
        return Underlying_->GetPrefix();
    }

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& /*name*/) const override
    {
        THROW_ERROR MakeError();
    }

    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const override
    {
        return StateManager_->GetExternalStateJoinerOrThrow(name);
    }

private:
    const TJobStateManagerPtr StateManager_;
    const IJobInitContextPtr Underlying_;

    TError MakeError() const
    {
        return TError("TTransformOrderedSourceComputation with \"read_delays\" does not support mutable state");
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TTransformOrderedSourceComputation::ValidateSpec(const TComputationSpec& spec)
{
    ValidateOrderedSourceSpec(spec, "TTransformOrderedSourceComputation");
    if (spec.GroupBySchema && spec.GroupBySchema->GetColumnCount() != 0) {
        THROW_ERROR_EXCEPTION("TTransformOrderedSourceComputation does not support group_by_schema");
    }
    if (spec.SourceStreams.size() != 1) {
        THROW_ERROR_EXCEPTION("TTransformOrderedSourceComputation requires exactly one source stream, but got %v",
            spec.SourceStreams.size());
    }
}

TMessageId TTransformOrderedSourceComputation::GetMaxPersistedMessageIdExclusive()
{
    return OrderedSource_->GetMaxPersistedMessageIdExclusive();
}

bool TTransformOrderedSourceComputation::HasPersistedKeyedOutput() const
{
    return true;
}

void TTransformOrderedSourceComputation::DoExecute(const IComputationRunContextPtr& context, TTraceContextGuard&& initTraceContextGuard)
{
    YT_TLOG_INFO("Started DoExecute");
    YT_VERIFY(TimerStore_);
    YT_VERIFY(InputStore_);
    YT_VERIFY(OrderedSource_);
    WaitFor(InputStore_->Init()).ThrowOnError();
    WaitFor(TimerStore_->Init()).ThrowOnError();
    if (!TimerStore_->IsEmpty()) {
        THROW_ERROR_EXCEPTION("Persisted timer store expected to be empty");
    }

    const auto sourceInitContext = StateManager_->CreateContext()->AsKey(*GetContext()->Partition->SourceKey);

    WatermarkGenerator_ = CreateWatermarkGenerator(
        GetSpec()->WatermarkStrategy->WatermarkGenerator,
        GetContext()->Profiler.WithPrefix("/watermark_generator"),
        Logger);
    WatermarkGenerator_->Init(sourceInitContext->WithPrefix(WatermarkStateName));

    const auto watermarkAligner = CreateWatermarkAligner(
        GetSpec()->WatermarkStrategy->WatermarkAlignment,
        Logger);

    OrderedSource_->Init(sourceInitContext->WithPrefix(ActiveSourceStateName));

    bool isFinished = true;
    {
        auto iterGuard = StartRunIteration(context);
        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();
        auto initContext = StateManager_->CreateContext();
        if (HasReadDelays()) {
            initContext = New<TReadDelayJobInitContext>(StateManager_, std::move(initContext));
        }

        DoInit(std::move(initContext));
        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, WatermarkGenerator_->Apply(BuildInflights(context, now), {*ActiveSourceStreamId_}));
        FinishRunIteration();
    }
    initTraceContextGuard.Release();
    YT_TLOG_INFO("Init completed");

    while (!isFinished) {
        auto iterGuard = StartRunIteration(context);
        auto dynamicSpec = GetDynamicSpec();

        const auto outputLimitsCheckResult = CheckOutputLimits(dynamicSpec, GetDynamicPartitionSpec());

        const auto partitionReadWatermark = WatermarkGenerator_->GetPartitionReadWatermark(OrderedSource_->GetReadEventWatermark());
        const bool alignmentCheck = dynamicSpec->Draining
            ? true
            : watermarkAligner->IsAllowToRead(partitionReadWatermark, GetEpochWatermarkState());
        const bool allowRead = !HeldRead_ && outputLimitsCheckResult.AllowedInputStreams.contains(*ActiveSourceStreamId_) && alignmentCheck;

        std::vector<ISource::TMessageBatch> sourceMessageBatches;
        if (allowRead) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Fetch"));
            sourceMessageBatches = WaitFor(OrderedSource_->GetNextBatch(TMessageBatcherSettingsPtr(dynamicSpec))).ValueOrThrow();
            i64 sourceMessageCount = 0;
            for (const auto& sourceBatch : sourceMessageBatches) {
                sourceMessageCount += std::ssize(sourceBatch.Messages);
            }
            context->RegisterSourceMessages(sourceMessageCount);
        }

        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();

        const THashMap<TStreamId, TSystemTimestamp> inputWatermarks{{*ActiveSourceStreamId_, partitionReadWatermark}};

        THashMap<TStreamId, TBatchStatistics> skippedStatistics;
        i64 skippedCount = 0;
        for (auto& sourceBatch : sourceMessageBatches) {
            RegisterInputBeforeProcessing(sourceBatch.Messages, {}, {}, inputWatermarks);
            if (Filter_->IsEnabled()) {
                auto [kept, skipped, statistics] = Filter_->Partition(std::move(sourceBatch.Messages));
                skippedCount += std::ssize(skipped);
                for (const auto& [streamId, totals] : statistics) {
                    skippedStatistics[streamId] += totals;
                }
                sourceBatch.Messages = std::move(kept);
            }
        }
        if (skippedCount > 0) {
            SkippedByExpressionCounter_.Increment(skippedCount);
            YT_TLOG_INFO("Skipped source messages by expression")
                .With("Skipped", skippedCount);
        }

        std::vector<TInputMessageConstPtr> sourceMessages;
        {
            size_t totalMessages = 0;
            for (const auto& sourceBatch : sourceMessageBatches) {
                totalMessages += sourceBatch.Messages.size();
            }
            sourceMessages.reserve(totalMessages);
            for (const auto& sourceBatch : sourceMessageBatches) {
                sourceMessages.insert(sourceMessages.end(), sourceBatch.Messages.begin(), sourceBatch.Messages.end());
            }
        }

        ThrottleInputBatch(sourceMessages, {}, {});

        auto inputContext = New<TInputContext>(sourceMessages, std::vector<TInputTimerConstPtr>{});
        TRootOutputCollector::TTransformResult processResult;
        if (!sourceMessages.empty()) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Process"));
            auto metaSetter = CreateUniqueMetaSetter(GetSpec(), uniqueSeqNo, now, EventTimestampAssigner_);
            auto outputCollector = New<TRootOutputCollector>(GetSpec(), metaSetter, /*supportsDistribute*/ true);
            PreloadKeyStates(inputContext);
            DoProcess(inputContext, outputCollector->SetParents(inputContext->GetMessages(), inputContext->GetTimers(), {}));
            processResult = outputCollector->CollectResult();
        }

        THROW_ERROR_EXCEPTION_UNLESS(processResult.OutputTimers.empty(), "TTransformOrderedSourceComputation does not support timers");
        RegisterResults(inputContext, std::move(processResult.LineageDelta), std::move(skippedStatistics));

        if (!sourceMessageBatches.empty()) {
            auto& heldRead = HeldRead_.emplace();
            heldRead.BatchCookies.reserve(sourceMessageBatches.size());
            for (auto& sourceBatch : sourceMessageBatches) {
                heldRead.BatchCookies.push_back(std::move(sourceBatch.Cookie));
            }

            heldRead.EventTimestamp = GetReadDelayBatchTimestamp(processResult.OutputMessages);
            heldRead.OutputMessages = std::move(processResult.OutputMessages);
            heldRead.OutputMessagesDistribute = std::move(processResult.OutputMessagesDistribute);

            YT_TLOG_INFO("Process completed")
                .With("SourceBatches", std::ssize(sourceMessageBatches))
                .With("SourceMessages", std::ssize(sourceMessages))
                .With("CandidateOutputMessages", std::ssize(heldRead.OutputMessages));
        }

        const auto readDelayThreshold = GetReadDelayThreshold();
        // A held read keeps the drained source suspended, so publishing it while draining would cross the drain boundary.
        const bool canPublish = !(dynamicSpec->Draining && HasReadDelays());
        if (HeldRead_ && canPublish && HeldRead_->EventTimestamp <= readDelayThreshold) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.Start"));
            // Registered only now: a reprocessed held read need not reproduce its event timestamps.
            auto watermarkGeneratorCookie = WatermarkGenerator_->RegisterRead(HeldRead_->OutputMessages);

            std::vector<TOutputMessageConstPtr> outputMessages;
            outputMessages.reserve(HeldRead_->OutputMessages.size());
            const auto& specStorage = GetContext()->StreamSpecStorage;
            for (auto&& [outputMessage, isOutput] : Zip(HeldRead_->OutputMessages, HeldRead_->OutputMessagesDistribute)) {
                if (!isOutput) {
                    continue;
                }

                // A held read may be published after its processing epoch has reported the system watermark.
                outputMessage.SystemTimestamp = now;
                outputMessages.push_back(New<TOutputMessage>(std::move(outputMessage), specStorage));
            }

            OutputStore_->TryRegisterKeyedBatch(outputMessages, *GetContext()->Partition->SourceKey, /*persist*/ true);
            RegisterOutputMessages(context, outputMessages, *GetContext()->Partition->SourceKey);

            for (const auto& cookie : HeldRead_->BatchCookies) {
                OrderedSource_->MarkPublished(cookie);
                OrderedSource_->MarkPersisted(cookie);
            }

            WatermarkGenerator_->MarkPersisted(std::move(watermarkGeneratorCookie));

            YT_TLOG_INFO("Publishing read")
                .With("SourceBatches", std::ssize(HeldRead_->BatchCookies))
                .With("OutputMessages", std::ssize(outputMessages));

            // Nothing is read while a read is held, so the leftovers join this iteration's cleanup.
            processResult.OutputMessages = std::move(HeldRead_->OutputMessages);
            HeldRead_.reset();
        } else if (HeldRead_) {
            YT_TLOG_INFO("Read delayed")
                .With("EventTimestamp", HeldRead_->EventTimestamp)
                .With("ReadDelayThreshold", readDelayThreshold);
        }

        auto tx = PrepareTransaction(context);

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Sync"));
            // User side effects of a held read must commit with its output and source progress.
            if (!HeldRead_) {
                DoSync(tx);
            }
        }
        Commit(context, tx);

        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, WatermarkGenerator_->Apply(BuildInflights(context, now), {*ActiveSourceStreamId_}));

        if (!sourceMessageBatches.empty()) {
            NoteNonEmptyRunIteration();
        }
        FinishRunIteration();

        WaitForBackoff(dynamicSpec, outputLimitsCheckResult, /*emptyInput*/ sourceMessageBatches.empty());

        ClearAsynchronously(std::move(sourceMessageBatches), std::move(sourceMessages), std::move(processResult));
    }
    YT_TLOG_INFO("Completed DoExecute");
}

void TTransformOrderedSourceComputation::ProcessDistributedMessages(const IComputationRunContextPtr& /*context*/, std::deque<TOutputMessageConstPtr>&& messages)
{
    OutputStore_->TryUnregisterBatch(messages);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
