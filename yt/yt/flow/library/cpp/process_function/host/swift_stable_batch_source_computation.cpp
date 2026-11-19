#include "swift_stable_batch_source_computation.h"

#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>

#include <yt/yt/flow/library/cpp/computation/stores/input_store.h>
#include <yt/yt/flow/library/cpp/computation/stores/output_store.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>

#include <yt/yt/flow/library/cpp/computation/computation_tracer.h>
#include <yt/yt/flow/library/cpp/computation/meta_setter.h>
#include <yt/yt/flow/library/cpp/computation/watermark_aligner.h>
#include <yt/yt/flow/library/cpp/computation/watermark_generator.h>

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>

#include <yt/yt/flow/library/cpp/misc/prefetch.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <contrib/libs/pdqsort/pdqsort.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TStableBatchSourceOutputMessage
    : public TOutputMessage
{
    using TOutputMessage::TOutputMessage;

    TOnDistributedCallback OnDistributedCallback;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TSwiftStableBatchSourceState::Register(TRegistrar registrar)
{
    registrar.Parameter("skip_if_memory", &TThis::SkipIfMemory)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TSkipIfExpressionSelection SelectSkipIfExpression(
    TSkipIfMemory* memory,
    const TMessageId& firstMessageId,
    const TMessageId& lastBatchFirstMessageId,
    const std::optional<std::string>& dynamicExpression)
{
    if (memory->IsRegistered(firstMessageId)) {
        return {.Expression = memory->Extract(firstMessageId)};
    }

    const bool changed = !memory->empty() && memory->back().second != dynamicExpression;
    memory->Register(lastBatchFirstMessageId, dynamicExpression);
    return {.Expression = dynamicExpression, .Changed = changed};
}

void ValidateSwiftStableBatchSourceSpecSpecifics(const TComputationSpec& spec)
{
    static constexpr TStringBuf ClassName = "TSwiftStableBatchSourceComputation";
    const auto& alignment = spec.WatermarkStrategy ? spec.WatermarkStrategy->WatermarkAlignment : nullptr;
    if (alignment && alignment->ReadDelays && !alignment->ReadDelays->empty()) {
        THROW_ERROR_EXCEPTION("%v does not support watermark_strategy/watermark_alignment/read_delays",
            ClassName);
    }

    for (const auto& [streamId, sourceSpec] : spec.SourceStreams) {
        if (!TRegistry::Get()->IsSourceReplayStable(sourceSpec->SourceClassName)) {
            THROW_ERROR_EXCEPTION("%v supports only sources with replay-stable reads, but source %Qv of stream %Qv is not one",
                ClassName,
                sourceSpec->SourceClassName,
                streamId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSwiftStableBatchSourceComputation::ValidateSpec(const TComputationSpec& spec)
{
    ValidateOrderedSourceSpec(spec, "TSwiftStableBatchSourceComputation");
    ValidateSwiftStableBatchSourceSpecSpecifics(spec);
}

void TSwiftStableBatchSourceComputation::TExtendedParameters::Register(TRegistrar /*registrar*/)
{ }

void TSwiftStableBatchSourceComputation::TExtendedDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_read_window", &TThis::MaxReadWindow)
        .Default(TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

TSwiftStableBatchSourceComputation::TSwiftStableBatchSourceComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TProcessFunctionComputationBase(std::move(context), std::move(dynamicContext))
{
    const auto profiler = GetContext()->Profiler.WithPrefix("/stable_batch");
    ReplayedGroupsCounter_ = profiler.Counter("/replayed_groups");
    SkipIfMemorySizeGauge_ = profiler.Gauge("/skip_if_memory_size");
}

void TSwiftStableBatchSourceComputation::DoExecute(const IComputationRunContextPtr& context, TTraceContextGuard&& initTraceContextGuard)
{
    YT_TLOG_INFO("Started DoExecute");
    YT_VERIFY(TimerStore_);
    YT_VERIFY(InputStore_);
    YT_VERIFY(OrderedSource_);

    WaitFor(InputStore_->Init())
        .ThrowOnError();
    WaitFor(TimerStore_->Init())
        .ThrowOnError();
    if (!TimerStore_->IsEmpty()) {
        THROW_ERROR_EXCEPTION("Persisted timer store expected to be empty");
    }

    const auto initContext = StateManager_->CreateContext()->AsKey(*GetContext()->Partition->SourceKey);
    State_ = WaitFor(initContext->CreateMutableStateClient<TSwiftStableBatchSourceState>(StableBatchSourceStateName))
        .ValueOrThrow();

    WatermarkGenerator_ = CreateWatermarkGenerator(
        GetSpec()->WatermarkStrategy->WatermarkGenerator,
        GetContext()->Profiler.WithPrefix("/watermark_generator"),
        Logger);
    WatermarkGenerator_->Init(initContext->WithPrefix(WatermarkStateName));

    auto* orderedSourceBase = dynamic_cast<TOrderedSourceBase*>(OrderedSource_.Get());
    THROW_ERROR_EXCEPTION_UNLESS(orderedSourceBase,
        "Source %Qv does not support stable read groups",
        GetOrCrash(GetSpec()->SourceStreams, *ActiveSourceStreamId_)->SourceClassName);
    orderedSourceBase->EnableStableReadGroups();
    OrderedSource_->Init(initContext->WithPrefix(ActiveSourceStateName));

    const auto watermarkAligner = CreateWatermarkAligner(
        GetSpec()->WatermarkStrategy->WatermarkAlignment,
        Logger);

    auto prepareInflights = [&] (auto&& inflights) {
        if (!State_->empty()) {
            auto minTimestamp = State_->front().second;
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
        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, prepareInflights(WatermarkGenerator_->Apply(BuildInflights(context, now), {*ActiveSourceStreamId_})));
        FinishRunIteration();
    }
    initTraceContextGuard.Release();
    YT_TLOG_INFO("Init completed");

    while (!isFinished) {
        auto iterGuard = StartRunIteration(context);
        auto dynamicSpec = GetDynamicSpec();

        const auto outputLimitsCheckResult = CheckOutputLimits(dynamicSpec, GetDynamicPartitionSpec());
        const auto partitionReadWatermark = WatermarkGenerator_->GetPartitionReadWatermark(OrderedSource_->GetReadEventWatermark());
        const bool alignmentCheck = dynamicSpec->Draining ||
            watermarkAligner->IsAllowToRead(partitionReadWatermark, GetEpochWatermarkState());
        const bool windowCheck = OrderedSource_->GetAlignmentTimestampWindow() < GetDynamicParameters()->MaxReadWindow;
        const bool allowRead = outputLimitsCheckResult.AllowedInputStreams.contains(*ActiveSourceStreamId_) && alignmentCheck && windowCheck;

        std::vector<ISource::TMessageBatch> sourceMessageBatches;
        if (allowRead) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Fetch"));
            sourceMessageBatches = WaitFor(OrderedSource_->GetNextBatch(TMessageBatcherSettingsPtr(dynamicSpec)))
                .ValueOrThrow();
            i64 sourceMessageCount = 0;
            for (const auto& sourceBatch : sourceMessageBatches) {
                sourceMessageCount += std::ssize(sourceBatch.Messages);
            }

            context->RegisterSourceMessages(sourceMessageCount);

            YT_TLOG_INFO("Got batch")
                .With("SourceBatches", sourceMessageBatches.size())
                .With("SourceMessages", sourceMessageCount);
        }

        auto futureUniqueSeqNo = GetTimeProvider()->GenerateGlobalUniqueSeqNo();

        auto inputWatermarks = THashMap<TStreamId, TSystemTimestamp>{{*ActiveSourceStreamId_, partitionReadWatermark}};
        for (const auto& messageBatch : sourceMessageBatches) {
            RegisterInputBeforeProcessing(messageBatch.Messages, {}, {}, inputWatermarks);
        }

        const bool published = !sourceMessageBatches.empty();
        if (published) {
            ProcessReadGroup(context, iterGuard.TraceContext, &sourceMessageBatches, futureUniqueSeqNo);
        }

        const auto [now, uniqueSeqNo] = [&] {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("GenerateGlobalUniqueSeqNo"));
            return WaitFor(futureUniqueSeqNo)
                .ValueOrThrow();
        }();

        auto tx = PrepareTransaction(context);

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Sync"));
            DoSync(tx);
            const auto persistedMessageIdExclusive = OrderedSource_->GetMaxPersistedMessageIdExclusive();
            State_->AdvanceExclusive(persistedMessageIdExclusive);
            State_->SkipIfMemory->AdvanceExclusive(persistedMessageIdExclusive);
            SkipIfMemorySizeGauge_.Update(State_->SkipIfMemory->size());
        }

        Commit(context, tx);

        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, prepareInflights(WatermarkGenerator_->Apply(BuildInflights(context, now), {*ActiveSourceStreamId_})));
        if (published) {
            NoteNonEmptyRunIteration();
        }

        FinishRunIteration();

        if (!published) {
            if (!alignmentCheck) {
                TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.WatermarkAlignment", EEpochPartKind::Waiting));
                YT_TLOG_INFO("Watermark unaligned epoch")
                    .With("PartitionReadWatermark", partitionReadWatermark);
                TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
            } else if (!windowCheck) {
                TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.ReadWindow", EEpochPartKind::Waiting));
                YT_TLOG_INFO("Read window by alignment timestamp is too long")
                    .With("Window", OrderedSource_->GetAlignmentTimestampWindow())
                    .With("MaxWindow", GetDynamicParameters()->MaxReadWindow);
                TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
            } else {
                WaitForBackoff(dynamicSpec, outputLimitsCheckResult, /*emptyInput*/ true);
            }
        }

        ClearAsynchronously(std::move(sourceMessageBatches));
    }
}

void TSwiftStableBatchSourceComputation::ProcessReadGroup(
    const IComputationRunContextPtr& context,
    const TTraceContextPtr& epochTraceContext,
    std::vector<ISource::TMessageBatch>* batches,
    TFuture<ITimeProvider::TGlobalUniqueSeqNo> futureNow)
{
    YT_VERIFY(!batches->empty());

    // Pre-filter ids: the boundaries of a group do not depend on the filter.
    const auto firstMessageId = batches->front().Messages.front()->MessageId;
    const auto lastBatchFirstMessageId = batches->back().Messages.front()->MessageId;

    // Must precede #Register below. A replayed group's entry is kept until the group is persisted, and a
    // first-sight group has a fresh seq no, so its ids exceed every entry.
    auto& publicationTimes = static_cast<TOrderedMemory<TMessageId, TSystemTimestamp>&>(*State_);
    const bool replayed = publicationTimes.IsRegistered(firstMessageId);
    if (replayed) {
        ReplayedGroupsCounter_.Increment();
    }

    // A replay must feed the function the same messages, so the filter is pinned per group.
    auto selection = SelectSkipIfExpression(State_->SkipIfMemory.Get(), firstMessageId, lastBatchFirstMessageId, GetDynamicSpec()->SkipIfExpression);
    if (selection.Changed) {
        YT_TLOG_INFO("Changed skip_if_expression of read groups")
            .With("FirstMessageId", firstMessageId)
            .With("SkipIfExpression", selection.Expression);
    }

    const auto& filter = GetFilter(selection.Expression);

    THashMap<TStreamId, TBatchStatistics> skippedStatistics;
    std::vector<TInputMessageConstPtr> allSourceMessages;
    i64 skippedCount = 0;
    for (auto& batch : *batches) {
        if (filter->IsEnabled()) {
            auto [kept, skipped, statistics] = filter->Partition(std::move(batch.Messages));
            skippedCount += std::ssize(skipped);
            for (const auto& [streamId, totals] : statistics) {
                skippedStatistics[streamId] += totals;
            }
            batch.Messages = std::move(kept);
        }

        allSourceMessages.insert(allSourceMessages.end(), batch.Messages.begin(), batch.Messages.end());
    }

    if (skippedCount > 0) {
        SkippedByExpressionCounter_.Increment(skippedCount);
        YT_TLOG_INFO("Skipped source messages by expression")
            .With("Skipped", skippedCount);
    }

    ThrottleInputBatch(allSourceMessages, {}, {});

    TRootOutputCollector::TTransformResult result;
    auto inputContext = New<TInputContext>(allSourceMessages, std::vector<TInputTimerConstPtr>{});
    {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Process"));
        if (!allSourceMessages.empty()) {
            auto metaSetter = CreateSwiftStableBatchMetaSetter(GetSpec(), EventTimestampAssigner_);
            auto outputCollector = New<TRootOutputCollector>(GetSpec(), metaSetter, /*supportsDistribute*/ true);
            DoProcess(inputContext, outputCollector->SetParents(inputContext->GetMessages(), inputContext->GetTimers(), {}));
            result = outputCollector->CollectResult();
        }
    }

    THROW_ERROR_EXCEPTION_UNLESS(result.OutputTimers.empty(), "TSwiftStableBatchSourceComputation does not support timers");

    for (const auto& parents : result.OutputMessagesParentMessageIds) {
        if (parents->ParentMessages.empty() || !parents->ParentTimers.empty()) {
            THROW_ERROR_EXCEPTION("Expected at least one parent message and no parent timers, got %v messages and %v timers",
                parents->ParentMessages.size(),
                parents->ParentTimers.size());
        }
    }

    RegisterResults(inputContext, std::move(result.LineageDelta), std::move(skippedStatistics));

    auto& candidateOutputMessages = result.OutputMessages;
    const auto& isOutput = result.OutputMessagesDistribute;
    YT_VERIFY(isOutput.size() == candidateOutputMessages.size());

    // One cookie per group: it is released only when the whole group is delivered.
    auto watermarkGeneratorCookie = WatermarkGenerator_->RegisterRead(candidateOutputMessages);

    const auto [now, uniqueSeqNo] = [&] {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("GenerateGlobalUniqueSeqNo"));
        return WaitFor(futureNow)
            .ValueOrThrow();
    }();

    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.Start"));

    // Registered under the last batch, so the lower bound of any id of the group finds its entry,
    // also when a trim has cut the beginning of the group.
    publicationTimes.Register(lastBatchFirstMessageId, now);
    const auto systemTimestamp = publicationTimes.Extract(firstMessageId);
    for (auto& message : candidateOutputMessages) {
        message.SystemTimestamp = systemTimestamp;
    }

    // Holding the first batch keeps the cursor before the group until the whole group is delivered.
    auto firstCookie = std::move(batches->front().Cookie);
    for (int index = 0; index < std::ssize(*batches); ++index) {
        OrderedSource_->MarkPublished(index == 0 ? firstCookie : (*batches)[index].Cookie);
    }

    for (int index = 1; index < std::ssize(*batches); ++index) {
        OrderedSource_->MarkPersisted((*batches)[index].Cookie);
    }

    auto groupTracker = TDistributingTracker([
        weakThis = MakeWeak(this),
        firstCookie = std::move(firstCookie),
        watermarkGeneratorCookie = std::move(watermarkGeneratorCookie)
    ] {
        if (auto strongThis = weakThis.Lock()) {
            YT_VERIFY(GetCurrentInvoker() == strongThis->GetContext()->SerializedInvoker,
                "Callback must be called only from DrainDistributedOutputs() or in Activate() synchronously");
            strongThis->WatermarkGenerator_->MarkPersisted(watermarkGeneratorCookie);
            strongThis->OrderedSource_->MarkPersisted(firstCookie);
        }
    });

    std::vector<TOutputMessageConstPtr> outputMessages;
    outputMessages.reserve(candidateOutputMessages.size());
    bool outputMessagesSorted = true;
    for (int index = 0; index < std::ssize(candidateOutputMessages); ++index) {
        auto& message = candidateOutputMessages[index];
        if (!isOutput[index] || OutputStore_->Contains(message)) {
            continue;
        }

        auto outputMessage = New<TStableBatchSourceOutputMessage>(std::move(message), GetContext()->StreamSpecStorage);
        outputMessage->OnDistributedCallback = groupTracker.AddDestination();
        if (outputMessagesSorted && !outputMessages.empty()) {
            outputMessagesSorted = outputMessages.back()->MessageId <= outputMessage->MessageId;
        }

        outputMessages.push_back(std::move(outputMessage));
    }

    // Ids order by the max parent, which need not follow emission order.
    if (!outputMessagesSorted) {
        pdqsort(outputMessages.begin(), outputMessages.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs->MessageId < rhs->MessageId;
        });
    }

    OutputStore_->TryRegisterKeyedBatch(outputMessages, *GetContext()->Partition->SourceKey, /*persist*/ false);
    RegisterOutputMessages(context, outputMessages, *GetContext()->Partition->SourceKey);
    groupTracker.Activate();

    YT_TLOG_INFO("Publishing read group")
        .With("SourceBatches", std::ssize(*batches))
        .With("SourceMessages", std::ssize(allSourceMessages))
        .With("CandidateOutputMessages", std::ssize(candidateOutputMessages))
        .With("Outputs", std::ssize(outputMessages))
        .With("Replayed", replayed)
        .With("FirstMessageId", firstMessageId);

    epochTraceContext->AddTag("ytflow.epoch.published_input_batches", std::ssize(*batches));
    epochTraceContext->AddTag("ytflow.epoch.published_parsed", std::ssize(candidateOutputMessages));
    epochTraceContext->AddTag("ytflow.epoch.published_outputs", std::ssize(outputMessages));
}

const IMessageFilterPtr& TSwiftStableBatchSourceComputation::GetFilter(const std::optional<std::string>& expression)
{
    auto it = Filters_.find(expression);
    if (it == Filters_.end()) {
        it = Filters_.emplace(expression, CreateMessageFilter(expression)).first;
    }

    return it->second;
}

void TSwiftStableBatchSourceComputation::ProcessDistributedMessages(const IComputationRunContextPtr& /*context*/, std::deque<TOutputMessageConstPtr>&& messages)
{
    OutputStore_->TryUnregisterBatch(messages);
    MakePrefetcher()
        .Add([] (const TOutputMessageConstPtr& message) {
            Y_PREFETCH_READ(message.Get(), 3);
        })
        .ForEach(messages, [] (const TOutputMessageConstPtr& message) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            static_cast<TStableBatchSourceOutputMessage*>(const_cast<TOutputMessage*>(message.Get()))->OnDistributedCallback();
        });
}

THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> TSwiftStableBatchSourceComputation::GetExtraInputLimits()
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
