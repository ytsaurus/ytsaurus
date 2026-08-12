#include "transform_ordered_source_computation.h"

#include "computation_tracer.h"
#include "message_filter.h"
#include "meta_setter.h"
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
    if (TimerStore_->GetCount() != 0) {
        THROW_ERROR_EXCEPTION("Persisted timer store expected to be empty, but it has %v records",
            TimerStore_->GetCount());
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
        DoInit(StateManager_->CreateContext());
        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, WatermarkGenerator_->Apply(BuildInflights(context), {*ActiveSourceStreamId_}));
        FinishRunIteration();
    }
    initTraceContextGuard.Release();
    YT_TLOG_INFO("Init completed");

    while (!isFinished) {
        auto iterGuard = StartRunIteration(context);
        auto dynamicSpec = GetDynamicSpec();

        const auto outputLimitsCheckResult = CheckOutputLimits(dynamicSpec, GetDynamicPartitionSpec());

        const auto partitionReadWatermark = WatermarkGenerator_->GetPartitionReadWatermark(OrderedSource_->GetReadEventWatermark());
        const auto readDelayThreshold = GetReadDelayThreshold();
        const bool alignmentCheck = dynamicSpec->Draining
            ? true
            : watermarkAligner->IsAllowToRead(partitionReadWatermark, GetEpochWatermarkState()) && OrderedSource_->GetReadAlignmentTimestamp() <= readDelayThreshold;
        const bool allowRead = outputLimitsCheckResult.AllowedInputStreams.contains(*ActiveSourceStreamId_) && alignmentCheck;

        std::vector<ISource::TMessageBatch> sourceMessageBatches;
        if (allowRead) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Fetch"));
            sourceMessageBatches = WaitFor(OrderedSource_->GetNextBatch(TMessageBatcherSettingsPtr(dynamicSpec))).ValueOrThrow();
        }

        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();

        const THashMap<TStreamId, TSystemTimestamp> inputWatermarks{{*ActiveSourceStreamId_, partitionReadWatermark}};

        i64 skippedCount = 0;
        for (auto& sourceBatch : sourceMessageBatches) {
            RegisterInputBeforeProcessing(sourceBatch.Messages, {}, {}, inputWatermarks);
            if (Filter_->IsEnabled()) {
                auto [kept, skipped] = Filter_->Partition(std::move(sourceBatch.Messages));
                skippedCount += std::ssize(skipped);
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

        TRootOutputCollector::TTransformResult processResult;
        if (!sourceMessages.empty()) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Process"));
            auto inputContext = New<TInputContext>(sourceMessages, std::vector<TInputTimerConstPtr>{});
            auto metaSetter = CreateUniqueMetaSetter(GetSpec(), uniqueSeqNo, now, EventTimestampAssigner_);
            auto outputCollector = New<TRootOutputCollector>(GetSpec(), metaSetter, /*supportsDistribute*/ true);
            PreloadKeyStates(inputContext);
            DoProcess(inputContext, outputCollector->SetParents(inputContext->GetMessages(), inputContext->GetTimers(), {}));
            processResult = outputCollector->CollectResult();
        }

        THROW_ERROR_EXCEPTION_UNLESS(processResult.OutputTimers.empty(), "TTransformOrderedSourceComputation does not support timers");

        std::optional<TWatermarkGeneratorCookie> watermarkGeneratorCookie;
        if (!sourceMessageBatches.empty()) {
            watermarkGeneratorCookie = WatermarkGenerator_->RegisterRead(processResult.OutputMessages);
        }

        std::vector<TOutputMessageConstPtr> outputMessages;
        outputMessages.reserve(processResult.OutputMessages.size());
        const auto& specStorage = GetContext()->StreamSpecStorage;
        for (auto&& [outputMessage, isOutput] : Zip(processResult.OutputMessages, processResult.OutputMessagesDistribute)) {
            if (!isOutput) {
                continue;
            }
            outputMessages.push_back(New<TOutputMessage>(std::move(outputMessage), specStorage));
        }

        YT_TLOG_INFO("Process completed")
            .With("SourceBatches", std::ssize(sourceMessageBatches))
            .With("SourceMessages", std::ssize(sourceMessages))
            .With("OutputMessages", std::ssize(outputMessages));

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.Start"));
            OutputStore_->TryRegisterKeyedBatch(outputMessages, *GetContext()->Partition->SourceKey, /*persist*/ true);
            RegisterOutputMessages(context, outputMessages, *GetContext()->Partition->SourceKey, dynamicSpec);
        }

        for (const auto& sourceBatch : sourceMessageBatches) {
            OrderedSource_->MarkPublished(sourceBatch.Cookie);
            OrderedSource_->MarkPersisted(sourceBatch.Cookie);
        }
        if (watermarkGeneratorCookie) {
            WatermarkGenerator_->MarkPersisted(std::move(*watermarkGeneratorCookie));
        }

        auto tx = PrepareTransaction(context);

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Sync"));
            DoSync(tx);
        }
        Commit(context, tx);

        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, WatermarkGenerator_->Apply(BuildInflights(context), {*ActiveSourceStreamId_}));

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
