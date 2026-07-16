#include "swift_ordered_source_computation.h"

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
#include <yt/yt/flow/library/cpp/misc/ordered_memory.h>
#include <yt/yt/flow/library/cpp/misc/prefetch.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NTracing;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TSwiftOrderedSourceComputationOutputMessage
    : public TOutputMessage
{
    using TOutputMessage::TOutputMessage;

    TOnDistributedCallback OnDistributedCallback;
};

using TSwiftOrderedSourceComputationOutputMessagePtr = TIntrusivePtr<TSwiftOrderedSourceComputationOutputMessage>;

////////////////////////////////////////////////////////////////////////////////

void TSwiftOrderedSourceTimestampMemory::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TSwiftOrderedSourceComputation::TExtendedParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TSwiftOrderedSourceComputation::TExtendedDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_read_window", &TThis::MaxReadWindow)
        .Default(TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

void TSwiftOrderedSourceComputation::ValidateSpec(const TComputationSpec& spec)
{
    if (!spec.InputStreamIds.empty()) {
        THROW_ERROR_EXCEPTION("TSwiftOrderedSourceComputation does not support input streams");
    }

    if (!spec.TimerStreams.empty()) {
        THROW_ERROR_EXCEPTION("TSwiftOrderedSourceComputation does not support timers");
    }

    if (!spec.KeyVisitorStreams.empty()) {
        THROW_ERROR_EXCEPTION("TSwiftOrderedSourceComputation does not support key_visitor_streams");
    }
}

TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TUniversalComputationBase(std::move(context), std::move(dynamicContext))
{
    if (GetContext()->Partition->State == EPartitionState::Executing) {
        if (!ActiveSource_) {
            THROW_ERROR_EXCEPTION("Active source is undefined");
        }

        if (!ActiveSourceStreamId_) {
            THROW_ERROR_EXCEPTION("Active source streamId is undefined");
        }

        if (!GetContext()->Partition->SourceKey) {
            THROW_ERROR_EXCEPTION("Source Key is undefined");
        }

        OrderedSource_ = DynamicPointerCast<IOrderedSource>(ActiveSource_);
        THROW_ERROR_EXCEPTION_UNLESS(OrderedSource_, "Expected IOrderedSource for source %Qv in computation %Qv", ActiveSourceStreamId_, GetComputationId());
    }

    Filter_ = CreateMessageFilter(GetDynamicSpec()->SkipIfExpression);
    SkippedByExpressionCounter_ = GetContext()->Profiler.WithPrefix("/source_streams").Counter("/skipped_by_expression_count");
    SubscribeOnReconfigure(
        BIND([this] {
            Filter_->Reconfigure(GetDynamicSpec()->SkipIfExpression);
        }),
        EWatchReconfigure::DynamicComputationSpec);
}

void TSwiftOrderedSourceComputation::DoPrepare(const IComputationRunContextPtr& context)
{
    InitOutputStoreDistribution(context, /*allowOutputDuplicates*/ false);
}

void TSwiftOrderedSourceComputation::DoExecute(const IComputationRunContextPtr& context, TTraceContextGuard&& initTraceContextGuard)
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

    const auto initContext = StateManager_->CreateContext()->AsKey(*GetContext()->Partition->SourceKey);
    auto timestampMemory = WaitFor(initContext->CreateMutableStateClient<TSwiftOrderedSourceTimestampMemory>(TimestampMemoryStateName)).ValueOrThrow();

    WatermarkGenerator_ = CreateWatermarkGenerator(
        GetSpec()->WatermarkStrategy->WatermarkGenerator,
        GetContext()->Profiler.WithPrefix("/watermark_generator"),
        Logger);
    WatermarkGenerator_->Init(initContext->WithPrefix(WatermarkStateName));

    OrderedSource_->Init(initContext->WithPrefix(ActiveSourceStateName));

    const auto watermarkAligner = CreateWatermarkAligner(
        GetSpec()->WatermarkStrategy->WatermarkAlignment,
        Logger);

    auto applyTimestampMemory = [&] (auto&& inflights) {
        if (!timestampMemory->empty()) {
            auto minTimestamp = timestampMemory->front().second;
            for (const auto& streamId : GetSpec()->OutputStreamIds) {
                auto& currentTimestamp = GetOrCrash(inflights, streamId)->MinSystemTimestamp;
                if (currentTimestamp) {
                    currentTimestamp = std::min(*currentTimestamp, minTimestamp);
                } else {
                    currentTimestamp = minTimestamp;
                }
            }
        }
        return std::forward<decltype(inflights)>(inflights);
    };

    bool isFinished = true;
    {
        auto iterGuard = StartRunIteration(context);
        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();
        DoInit(StateManager_->CreateContext());
        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, applyTimestampMemory(WatermarkGenerator_->Apply(BuildInflights(), {*ActiveSourceStreamId_})));
        FinishRunIteration();
    }
    initTraceContextGuard.Release();
    YT_TLOG_INFO("Init completed");


    while (!isFinished) {
        auto iterGuard = StartRunIteration(context);
        auto dynamicSpec = GetDynamicSpec();

        const auto outputLimitsCheckResult = CheckOutputLimits(dynamicSpec, GetDynamicPartitionSpec());
        const auto partitionReadWatermark = WatermarkGenerator_->GetPartitionReadWatermark(OrderedSource_->GetReadEventWatermark());
        const bool alignmentCheck = !dynamicSpec->Draining
            ? watermarkAligner->IsAllowToRead(partitionReadWatermark, GetEpochWatermarkState())
            : true;
        const bool windowCheck = OrderedSource_->GetAlignmentTimestampWindow() < GetDynamicParameters()->MaxReadWindow;
        const bool allowRead = outputLimitsCheckResult.AllowedInputStreams.contains(*ActiveSourceStreamId_) && alignmentCheck && windowCheck;

        std::vector<ISource::TMessageBatch> sourceMessageBatches;
        if (DelayedMessages_.empty() && allowRead) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Fetch"));
            sourceMessageBatches = WaitFor(OrderedSource_->GetNextBatch(TMessageBatcherSettingsPtr(dynamicSpec))).ValueOrThrow();
            i64 sourceMessageCount = 0;
            for (const auto& sourceBatch : sourceMessageBatches) {
                sourceMessageCount += std::ssize(sourceBatch.Messages);
            }
            YT_TLOG_INFO("Got batch")
                .With("SourceBatches", sourceMessageBatches.size())
                .With("SourceMessages", sourceMessageCount);
        }

        auto generateSeqNoFuture = GetTimeProvider()->GenerateGlobalUniqueSeqNo();

        auto inputWatermarks = THashMap<TStreamId, TSystemTimestamp>{{*ActiveSourceStreamId_, partitionReadWatermark}};
        for (const auto& messageBatch : sourceMessageBatches) {
            RegisterInputBeforeProcessing(messageBatch.Messages, {}, {}, inputWatermarks);
        }
        ProcessSourceBatches(std::move(sourceMessageBatches));

        const auto [now, uniqueSeqNo] = [&] {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("GenerateGlobalUniqueSeqNo"));
            return WaitFor(generateSeqNoFuture).ValueOrThrow();
        }();

        bool emptyEpoch = CheckDelayedMessages(context, iterGuard.TraceContext, now, MakeStrong(&*timestampMemory), dynamicSpec);

        auto tx = PrepareTransaction(context);

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Sync"));
            DoSync(tx);
            timestampMemory->AdvanceExclusive(OrderedSource_->GetMaxPersistedMessageIdExclusive());
        }
        Commit(context, tx);

        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, applyTimestampMemory(WatermarkGenerator_->Apply(BuildInflights(), {*ActiveSourceStreamId_})));
        FinishRunIteration();

        if (emptyEpoch) {
            if (!DelayedMessages_.empty()) {
                TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.InjectionDelay"));
                YT_TLOG_INFO("Injection delayed epoch")
                    .With("DelayedTimestamp", DelayedMessages_.front().Timestamp)
                    .With("DelayedUnparsedMessages", DelayedMessages_.size());
                TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
            } else if (!alignmentCheck) {
                TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.WatermarkAlignment"));
                YT_TLOG_INFO("Watermark unaligned epoch")
                    .With("PartitionReadWatermark", partitionReadWatermark);
                TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
            } else if (!windowCheck) {
                TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.ReadWindow"));
                YT_TLOG_INFO("Read window by alignment timestamp is too long")
                    .With("Window", OrderedSource_->GetAlignmentTimestampWindow())
                    .With("MaxWindow", GetDynamicParameters()->MaxReadWindow);
                TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
            } else {
                WaitForBackoff(dynamicSpec, outputLimitsCheckResult, /*emptyInput*/ true);
            }
        }

        // NOLINTNEXTLINE(bugprone-use-after-move): ProcessSourceBatches takes an rvalue reference and leaves the input messages in place.
        ClearAsynchronously(std::move(sourceMessageBatches));
    }
}

TSystemTimestamp TSwiftOrderedSourceComputation::GetTriggerTimestamp(const std::vector<TMessage>& messages)
{
    TSystemTimestamp timestamp = ZeroSystemTimestamp;
    for (const auto& message : messages) {
        timestamp = std::max(timestamp, message.EventTimestamp);
    }
    return timestamp;
}

void TSwiftOrderedSourceComputation::ProcessSourceBatches(std::vector<ISource::TMessageBatch>&& sourceMessageBatches)
{
    if (sourceMessageBatches.empty()) {
        return;
    }

    // Skip messages before processing. A fully skipped batch is persisted
    // right away; in a mixed batch the skipped messages ride the batch's cookie.
    if (Filter_->IsEnabled()) {
        i64 skippedCount = 0;
        std::vector<ISource::TMessageBatch> keptBatches;
        keptBatches.reserve(sourceMessageBatches.size());
        for (auto& batch : sourceMessageBatches) {
            auto [kept, skipped] = Filter_->Partition(std::move(batch.Messages));
            skippedCount += std::ssize(skipped);
            if (kept.empty()) {
                OrderedSource_->MarkPublished(batch.Cookie);
                OrderedSource_->MarkPersisted(batch.Cookie);
                continue;
            }
            batch.Messages = std::move(kept);
            keptBatches.push_back(std::move(batch));
        }
        if (skippedCount > 0) {
            SkippedByExpressionCounter_.Increment(skippedCount);
            YT_TLOG_INFO("Skipped source messages by expression")
                .With("Skipped", skippedCount)
                .With("KeptBatches", keptBatches.size());
        }
        sourceMessageBatches = std::move(keptBatches);
        if (sourceMessageBatches.empty()) {
            return;
        }
    }

    // Each source message batch corresponds to one offset and may contain multiple messages.
    // We treat each source batch as its own unit for watermark purposes.
    // The indices map is keyed by raw pointer because the source batches outlive this function.
    absl::flat_hash_map<const TInputMessage*, int> sourceMessageBatchIndices;
    std::vector<TInputMessageConstPtr> allSourceMessages;
    size_t totalMessages = 0;
    for (const auto& batch : sourceMessageBatches) {
        totalMessages += batch.Messages.size();
    }
    allSourceMessages.reserve(totalMessages);
    sourceMessageBatchIndices.reserve(totalMessages);
    for (int i = 0; i < std::ssize(sourceMessageBatches); ++i) {
        for (const auto& message : sourceMessageBatches[i].Messages) {
            allSourceMessages.push_back(message);
            sourceMessageBatchIndices[message.Get()] = i;
        }
    }

    ThrottleInputBatch(allSourceMessages, {}, {});

    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Process"));
    auto inputContext = New<TInputContext>(allSourceMessages, std::vector<TInputTimerConstPtr>{});

    auto metaSetter = CreateDeterministicMetaSetter(GetSpec(), EventTimestampAssigner_);
    auto outputCollector = New<TRootOutputCollector>(GetSpec(), metaSetter, /*supportsDistribute*/ true);
    DoProcess(inputContext, outputCollector->SetParents(inputContext->GetMessages(), inputContext->GetTimers(), {}));

    auto result = outputCollector->CollectResult();
    THROW_ERROR_EXCEPTION_UNLESS(result.OutputTimers.empty(), "SwiftSourceComputation does not support timers");

    const auto& flatDistribute = result.OutputMessagesDistribute;
    YT_VERIFY(flatDistribute.size() == result.OutputMessages.size());

    // Group messages and output flags by source batch index.
    std::vector<std::vector<TMessage>> candidateOutputMessages(sourceMessageBatches.size());
    std::vector<std::vector<bool>> isOutputByBatch(sourceMessageBatches.size());
    for (auto&& [output, outputParents, isOutput] : Zip(result.OutputMessages, result.OutputMessagesParentMessageIds, flatDistribute)) {
        if (outputParents->ParentMessages.size() != 1 || outputParents->ParentTimers.size() != 0) {
            THROW_ERROR_EXCEPTION("Expected exactly one ParentMessageId (ParentsCount: %v)",
                outputParents->ParentMessages.size());
        }
        int index = GetOrCrash(sourceMessageBatchIndices, outputParents->ParentMessages[0].Get());
        // SystemTimestamp will be set later in CheckDelayedMessages.
        candidateOutputMessages[index].push_back(std::move(output));
        isOutputByBatch[index].push_back(isOutput);
    }

    std::vector<TWatermarkGeneratorCookie> watermarkGeneratorCookies;
    watermarkGeneratorCookies.reserve(candidateOutputMessages.size());
    for (const auto& batch : candidateOutputMessages) {
        watermarkGeneratorCookies.push_back(WatermarkGenerator_->RegisterRead(batch));
    }

    {
        i64 outputCount = 0;
        for (const auto& isOutput : isOutputByBatch) {
            for (const bool flag : isOutput) {
                if (flag) {
                    outputCount += 1;
                }
            }
        }

        YT_TLOG_INFO("Process completed")
            .With("SourceBatches", std::ssize(sourceMessageBatches))
            .With("SourceMessages", std::ssize(allSourceMessages))
            .With("CandidateOutputMessages", std::ssize(result.OutputMessages))
            .With("OutputMessages", outputCount);
    }

    for (i64 i = 0; i < std::ssize(sourceMessageBatches); ++i) {
        YT_VERIFY(!sourceMessageBatches[i].Messages.empty());
        auto timestamp = GetTriggerTimestamp(candidateOutputMessages[i]);
        DelayedMessages_.push_back(
            {
                .FirstMessageId = sourceMessageBatches[i].Messages[0]->MessageId,
                .BatchCookie = std::move(sourceMessageBatches[i].Cookie),
                .CandidateOutputMessages = std::move(candidateOutputMessages[i]),
                .IsOutput = std::move(isOutputByBatch[i]),
                .Timestamp = timestamp,
                .WatermarkGeneratorCookie = watermarkGeneratorCookies[i],
            });
    }
}

bool TSwiftOrderedSourceComputation::CheckDelayedMessages(
    IComputationRunContextPtr context,
    NTracing::TTraceContextPtr epochTraceContext,
    TSystemTimestamp now,
    TIntrusivePtr<TOrderedMemory<TMessageId, TSystemTimestamp>> timestampMemory,
    TDynamicComputationSpecPtr dynamicSpec)
{
    // This function starts distributing some of delayed messages. Use common for all computations part name here.
    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.Start"));

    const TSystemTimestamp thresholdTimestamp = [&] () {
        auto thresholdTimestamp = InfinitySystemTimestamp;
        if (GetSpec()->WatermarkStrategy->WatermarkAlignment && GetSpec()->WatermarkStrategy->WatermarkAlignment->ReadDelays) {
            for (const auto& [streamId, delay] : *GetSpec()->WatermarkStrategy->WatermarkAlignment->ReadDelays) {
                auto watermark = GetEpochEventWatermark(streamId);
                thresholdTimestamp = std::min(thresholdTimestamp, TSystemTimestamp(std::max(watermark.Underlying(), delay.Seconds()) - delay.Seconds()));
            }
        }
        return thresholdTimestamp;
    }();

    const auto canBePublished = [&] (const TProcessedBatch& processed) {
        if (dynamicSpec->Draining) {
            // Can process only if message was registered in timestamp memory.
            return timestampMemory->IsRegistered(processed.FirstMessageId);
        }
        return processed.Timestamp <= thresholdTimestamp;
    };

    i64 publishedInputBatches = 0;
    i64 publishedParsed = 0;
    i64 publishedOutputs = 0;

    // Accumulate all output messages across all processed batches for a single RegisterOutputMessages call.
    std::vector<TOutputMessageConstPtr> allOutputMessages;
    size_t publishableBatchCount = 0;
    {
        size_t estimate = 0;
        for (const auto& delayed : DelayedMessages_) {
            if (!canBePublished(delayed)) {
                break;
            }
            ++publishableBatchCount;
            estimate += delayed.CandidateOutputMessages.size();
        }
        allOutputMessages.reserve(estimate);
    }

    for (size_t i = 0; i < publishableBatchCount; ++i) {
        auto processed = std::move(DelayedMessages_.front());
        DelayedMessages_.pop_front();
        publishedParsed += std::ssize(processed.CandidateOutputMessages);

        timestampMemory->Register(processed.FirstMessageId, now);
        auto extractedTimestamp = timestampMemory->Extract(processed.FirstMessageId);
        for (auto& message : processed.CandidateOutputMessages) {
            message.SystemTimestamp = extractedTimestamp;
        }

        YT_VERIFY(processed.CandidateOutputMessages.size() == processed.IsOutput.size());

        publishedInputBatches += 1;
        OrderedSource_->MarkPublished(processed.BatchCookie);

        auto batchTracker = TDistributingTracker([
            weakThis = MakeWeak(this),
            batchCookie = std::move(processed.BatchCookie),
            watermarkGeneratorCookie = std::move(processed.WatermarkGeneratorCookie)
        ] {
            if (auto strongThis = weakThis.Lock()) {
                YT_VERIFY(GetCurrentInvoker() == strongThis->GetContext()->SerializedInvoker,
                    "Callback must be called only from DrainDistributedOutputs() or in Activate() synchronously");
                strongThis->WatermarkGenerator_->MarkPersisted(watermarkGeneratorCookie);
                strongThis->OrderedSource_->MarkPersisted(batchCookie);
            }
        });
        for (int i = 0; i < std::ssize(processed.CandidateOutputMessages); ++i) {
            auto& message = processed.CandidateOutputMessages[i];
            if (!processed.IsOutput[i] || OutputStore_->Contains(message)) {
                continue;
            }
            publishedOutputs += 1;

            auto outputMessage = New<TSwiftOrderedSourceComputationOutputMessage>(std::move(message), GetContext()->StreamSpecStorage);
            outputMessage->OnDistributedCallback = batchTracker.AddDestination();
            allOutputMessages.push_back(std::move(outputMessage));
        }

        batchTracker.Activate();
    }
    // Register all accumulated output messages in one batch.
    OutputStore_->TryRegisterKeyedBatch(allOutputMessages, *GetContext()->Partition->SourceKey, /*persist*/ false);
    RegisterOutputMessages(context, allOutputMessages, *GetContext()->Partition->SourceKey, dynamicSpec);

    YT_TLOG_INFO("Publishing batch")
        .With("SourceBatches", publishedInputBatches)
        .With("Parsed", publishedParsed)
        .With("Outputs", publishedOutputs);

    epochTraceContext->AddTag("ytflow.epoch.published_input_batches", publishedInputBatches);
    epochTraceContext->AddTag("ytflow.epoch.published_parsed", publishedParsed);
    epochTraceContext->AddTag("ytflow.epoch.published_outputs", publishedOutputs);

    return publishedInputBatches == 0;
}

void TSwiftOrderedSourceComputation::ProcessDistributedMessages(const IComputationRunContextPtr& /*context*/, std::deque<TOutputMessageConstPtr>&& messages)
{
    OutputStore_->TryUnregisterBatch(messages);
    MakePrefetcher()
        .Add([] (const TOutputMessageConstPtr& message) {
            Y_PREFETCH_READ(message.Get(), 3);
        })
        .ForEach(messages, [] (const TOutputMessageConstPtr& message) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            static_cast<TSwiftOrderedSourceComputationOutputMessage*>(const_cast<TOutputMessage*>(message.Get()))->OnDistributedCallback();
        });
}

THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> TSwiftOrderedSourceComputation::GetExtraInputLimits()
{
    if (!OrderedSource_ || !ActiveSourceStreamId_) {
        return {};
    }
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> result;
    auto& entityLimitStatus = result["max_read_window_seconds"][*ActiveSourceStreamId_];
    entityLimitStatus.Limit = GetDynamicParameters()->MaxReadWindow.Seconds();
    entityLimitStatus.Used = OrderedSource_->GetAlignmentTimestampWindow().Seconds();
    return result;
}

TComputationOrchidStatePtr TSwiftOrderedSourceComputation::GetOrchidState()
{
    auto state = TUniversalComputationBase::GetOrchidState();
    auto universalState = DynamicPointerCast<TUniversalComputationOrchidState>(state);
    YT_VERIFY(universalState);
    universalState->PartitionDescription = Format("SourceKey: %v", *GetContext()->Partition->SourceKey);
    return universalState;
}

void TSwiftOrderedSourceComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    DoInit();
}

void TSwiftOrderedSourceComputation::DoInit()
{ }

void TSwiftOrderedSourceComputation::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    YT_VERIFY(input->GetTimers().empty());
    for (const auto& message : input->GetMessages()) {
        DoProcessMessage(message, output->SetParents({message}, {}, {}));
    }
}

void TSwiftOrderedSourceComputation::DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output)
{
    DoProcessMessage(*message, std::move(output));
}

void TSwiftOrderedSourceComputation::DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/)
{ }

void TSwiftOrderedSourceComputation::DoSync(IRetryableTransactionPtr /*transaction*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
