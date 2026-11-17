#include "transform_computation.h"

#include "computation_tracer.h"
#include "key_visitor.h"
#include "meta_setter.h"
#include "stores/input_store.h"
#include "stores/output_store.h"
#include "stores/timer_store.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key_error.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/failed_messages.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NTracing;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TTransformComputation::TParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("processing_mode", &TThis::ProcessingMode)
        .Default(EProcessingMode::ExactlyOnce);
}

////////////////////////////////////////////////////////////////////////////////

void TTransformComputation::TDynamicParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TTransformComputation::ValidateSpec(const TComputationSpec& spec)
{
    if (spec.InputStreamIds.empty() && spec.KeyVisitorStreams.empty()) {
        THROW_ERROR_EXCEPTION("TTransformComputation requires at least one input stream or a key_visitor_streams entry");
    }

    if (!spec.SourceStreams.empty()) {
        THROW_ERROR_EXCEPTION("TTransformComputation does not support source streams");
    }

    if (spec.WatermarkStrategy && spec.WatermarkStrategy->WatermarkGenerator) {
        THROW_ERROR_EXCEPTION("TTransformComputation does not support watermark generator");
    }
    if (spec.WatermarkStrategy && spec.WatermarkStrategy->WatermarkAlignment) {
        THROW_ERROR_EXCEPTION("TTransformComputation does not support watermark alignment");
    }
}

TTransformComputation::TTransformComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TUniversalComputationBase(std::move(context), std::move(dynamicContext))
    , FailedMessages_(CreateFailedMessages())
{ }

NTables::TFailedMessagesPtr TTransformComputation::CreateFailedMessages()
{
    if (GetPartitionState() != EPartitionState::Executing || !GetContext()->FailureHandlerEnabled) {
        return nullptr;
    }

    auto tableContext = New<NTables::TContext>();
    tableContext->Client = GetRetryableClient()->WithErrorComponent(std::string(FailedMessagesTableName));
    tableContext->PipelinePath = GetContext()->PipelinePath;
    tableContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
    tableContext->Logger = Logger;
    tableContext->Profiler = GetContext()->Profiler.WithPrefix("/tables");
    tableContext->Tag = Format("%s:%s", GetContext()->Partition->ComputationId, FailedMessagesTableName);
    return New<NTables::TFailedMessages>(
        std::move(tableContext),
        GetContext()->StreamSpecStorage->GetStreamSpecs(),
        GetContext()->Job->WorkerAddress,
        GetContext()->FlowCoreTarget,
        GetThrottlerOrThrow(FailedMessagesThrottlerId),
        GetContext()->StatusProfiler->ErrorState("/failed_messages/write_quota"));
}

void TTransformComputation::DoPrepare(const IComputationRunContextPtr& context)
{
    InitOutputStoreDistribution(context);
}

namespace {

//! Returns the input timers and the root timers of |completedTasks| that are not in |heldRootTimerIds|.
std::vector<TInputTimerConstPtr> GetAcknowledgedTimers(
    const std::vector<TInputTimerConstPtr>& inputTimers,
    const std::vector<TUntypedCompletedAsyncTaskPtr>& completedTasks,
    const THashSet<TMessageId>& heldRootTimerIds)
{
    std::vector<TInputTimerConstPtr> acknowledged;
    i64 heldCount = 0;
    auto addTimer = [&] (const TInputTimerConstPtr& timer) {
        if (heldRootTimerIds.contains(timer->MessageId)) {
            ++heldCount;
        } else {
            acknowledged.push_back(timer);
        }
    };
    for (const auto& timer : inputTimers) {
        addTimer(timer);
    }
    for (const auto& task : completedTasks) {
        // A step that adds no task is the last one of the chain.
        addTimer(task->RootTimer);
    }
    THROW_ERROR_EXCEPTION_UNLESS(heldCount == std::ssize(heldRootTimerIds),
        "A task of an async processor descends from a timer that is not an input of the epoch");
    return acknowledged;
}

//! A restart replays the steps of a held root timer, so only the step that acknowledges it may emit outputs.
void ValidateHeldRootTimersHaveNoOutputs(
    const TRootOutputCollector::TTransformResult& result,
    const THashSet<TMessageId>& heldRootTimerIds)
{
    if (heldRootTimerIds.empty()) {
        return;
    }
    // The outputs of one input share its parent set.
    THashSet<const TMessageParents*> validated;
    auto validate = [&] (const std::vector<TMessageParentsConstPtr>& parentsList, TStringBuf output) {
        for (const auto& parents : parentsList) {
            if (!validated.insert(parents.Get()).second) {
                continue;
            }
            for (const auto& timer : parents->ParentTimers) {
                THROW_ERROR_EXCEPTION_IF(heldRootTimerIds.contains(timer->MessageId),
                    "Timer %v adds a task and %v in one epoch; outputs are allowed only at the step that acknowledges the timer",
                    timer->MessageId,
                    output);
            }
        }
    };
    validate(result.OutputMessagesParentMessageIds, "an output message");
    validate(result.OutputTimersParentMessageIds, "a timer");
}

//! The clock skew between the job and the timestamp provider that a swift timer may be set ahead by.
constexpr auto SwiftTimerTriggerTolerance = TDuration::Minutes(1);

//! A swift timer is due at once: it must not be set beyond the current timestamp |now| of the epoch.
void ValidateSwiftTimerTriggers(const TComputationSpec& spec, const std::vector<TTimer>& timers, TSystemTimestamp now)
{
    const TSystemTimestamp limit(now.Underlying() + SwiftTimerTriggerTolerance.Seconds());
    for (const auto& timer : timers) {
        if (!GetOrCrash(spec.TimerStreams, timer.StreamId)->Swift) {
            continue;
        }
        THROW_ERROR_EXCEPTION_IF(timer.TriggerTimestamp > limit,
            "Swift timer %v of stream %Qv has trigger timestamp %v beyond the current timestamp %v",
            timer.MessageId,
            timer.StreamId,
            timer.TriggerTimestamp,
            now);
    }
}

} // namespace

void TTransformComputation::DoExecute(const IComputationRunContextPtr& context, TTraceContextGuard&& initTraceContextGuard)
{
    YT_TLOG_INFO("Started DoExecute");
    YT_VERIFY(TimerStore_);
    YT_VERIFY(InputStore_);
    WaitFor(TimerStore_->Init()).ThrowOnError();
    WaitFor(InputStore_->Init()).ThrowOnError();
    for (const auto& [streamId, visitor] : KeyVisitors_) {
        const bool upstreamCompletedAtInit = ComputeKeyVisitorUpstreamStreams(*GetSpec(), streamId).empty();
        WaitFor(visitor->Init(upstreamCompletedAtInit)).ThrowOnError();
    }

    bool isFinished = true;
    {
        auto iterGuard = StartRunIteration(context);
        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();
        DoInit(StateManager_->CreateContext(/*prefix*/ {}, AsyncProcessorManager_));
        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, BuildInflights(context, now));
        FinishRunIteration();
    }

    initTraceContextGuard.Release();
    YT_TLOG_INFO("Init completed");
    while (!isFinished) {
        auto iterGuard = StartRunIteration(context);
        auto dynamicSpec = GetDynamicSpec();
        const auto deduplicateInput = [&] () -> bool {
            switch (GetParameters()->ProcessingMode) {
                case EProcessingMode::ExactlyOnce:
                    return true;
                case EProcessingMode::AtLeastOnceConsistent:
                    return false;
            };
        }();

        const auto outputLimitsCheckResult = CheckOutputLimits(dynamicSpec, GetDynamicPartitionSpec());
        const auto& allowedInputStreams = outputLimitsCheckResult.AllowedInputStreams;

        // The results have a batch of their own and, like the timers, wait while an output of their stream
        // is blocked or the job drains.
        std::vector<TUntypedCompletedAsyncTaskPtr> completedTasks;
        if (!dynamicSpec->Draining) {
            completedTasks = AsyncProcessorManager_->TakeReady(
                outputLimitsCheckResult.AllowedRootTimerStreams,
                dynamicSpec->MaxRowsPerBatch,
                dynamicSpec->MaxBytesPerBatch);
        }
        TBatchStatistics completedTaskStatistics{.Count = std::ssize(completedTasks)};
        for (const auto& task : completedTasks) {
            completedTaskStatistics.ByteSize += task->ByteSize;
        }

        std::vector<TInputMessageConstPtr> inputs;
        std::vector<TInputTimerConstPtr> inputTimers;
        std::vector<TInputVisitConstPtr> inputVisits;
        if (!allowedInputStreams.empty()) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Fetch"));
            auto inputsFuture = context->GetNextBatch(allowedInputStreams);
            inputTimers = TimerStore_->GetNextBatch(allowedInputStreams, dynamicSpec->MaxRowsPerBatch, dynamicSpec->MaxBytesPerBatch);
            {
                TTraceContextGuard waitGuard(Tracer_->CreateEpochPartTraceContext("Input.WaitForBatch", EEpochPartKind::WaitingForInput));
                inputs = WaitFor(inputsFuture).ValueOrThrow();
            }

            std::vector<TKeyVisitorPtr> allowedVisitors;
            for (const auto& [streamId, visitor] : KeyVisitors_) {
                if (allowedInputStreams.contains(streamId)) {
                    allowedVisitors.push_back(visitor);
                }
            }
            if (!allowedVisitors.empty()) {
                const i64 perVisitorBudget = dynamicSpec->MaxRowsPerBatch / std::ssize(allowedVisitors);
                for (const auto& visitor : allowedVisitors) {
                    auto visits = visitor->GetNextBatch(perVisitorBudget);
                    for (auto& visit : visits) {
                        inputVisits.push_back(New<TInputVisit>(std::move(visit)));
                    }
                }
            }
        }
        ObserveKeyCountInput(inputs, inputTimers, inputVisits);
        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();
        YT_TLOG_INFO("Got batch")
            .With("Inputs", inputs.size())
            .With("Timers", inputTimers.size())
            .With("Visits", inputVisits.size())
            .With("CompletedTasks", completedTasks.size());

        auto emptyInput = inputs.empty() && inputTimers.empty() && inputVisits.empty() && completedTasks.empty();
        auto filteredInputs = FilterInputBatch(context, std::move(inputs));

        auto unprocessedInputs = [&] () {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Deduplicate"));
            auto [processedInput, unprocessedInputs] = InputStore_->Filter(filteredInputs.Messages, deduplicateInput);
            YT_TLOG_INFO("Filtered already processed")
                .With("Inputs", processedInput.size());
            context->MarkDeduplicated(processedInput);
            return unprocessedInputs;
        }();

        // The results are inputs of the epoch too: they count toward the input quotas.
        ThrottleInputBatch(unprocessedInputs, inputTimers, inputVisits, completedTaskStatistics);

        TRootOutputCollector::TTransformResult processResult;
        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Process"));
            RegisterInputBeforeProcessing(unprocessedInputs, inputTimers, inputVisits);
            auto inputContext = New<TInputContext>(unprocessedInputs, inputTimers, inputVisits);
            auto metaSetter = CreateUniqueMetaSetter(GetSpec(), uniqueSeqNo, now, EventTimestampAssigner_);
            auto failureHandlerMode = GetContext()->FailureHandlerEnabled
                ? TRootOutputCollector::EFailureHandlerMode::Enabled
                : TRootOutputCollector::EFailureHandlerMode::Disabled;
            auto outputCollector = New<TRootOutputCollector>(GetSpec(), metaSetter, /*supportsDistribute*/ false, failureHandlerMode);
            IInputContextPtr preloadContext = inputContext;
            if (!completedTasks.empty()) {
                // A result is processed with the state of the key of its root timer.
                auto timersWithRoots = inputTimers;
                for (const auto& task : completedTasks) {
                    timersWithRoots.push_back(task->RootTimer);
                }
                preloadContext = New<TInputContext>(unprocessedInputs, timersWithRoots, inputVisits);
            }
            PreloadKeyStates(preloadContext);
            // The results go first: they end the work that the timers started. The root timer is the parent
            // of what the handler adds.
            for (const auto& task : completedTasks) {
                TagErrorWithKey("key", task->RootTimer->Key, [&] {
                    task->Processor->ResultHandler(task, outputCollector->SetParents({}, {task->RootTimer}, {}));
                });
            }
            DoProcess(inputContext, outputCollector->SetParents(inputContext->GetMessages(), inputContext->GetTimers(), inputContext->GetVisits()));
            processResult = outputCollector->CollectResult();
            // A result is work of the epoch but not a lineage input: its outputs count against the firing of its root timer.
            RegisterResults(inputContext, std::move(processResult.LineageDelta), std::move(filteredInputs.SkippedStatistics), completedTaskStatistics);
        }

        YT_TLOG_INFO("Process completed")
            .With("OutputTimers", processResult.OutputTimers.size())
            .With("OutputMessages", processResult.OutputMessages.size())
            .With("Tasks", processResult.Tasks.size());

        auto tasks = std::move(processResult.Tasks);
        auto acknowledgedTimers = GetAcknowledgedTimers(inputTimers, completedTasks, processResult.TaskRootTimerIds);
        ValidateHeldRootTimersHaveNoOutputs(processResult, processResult.TaskRootTimerIds);
        ValidateSwiftTimerTriggers(*GetSpec(), processResult.OutputTimers, now);

        if (deduplicateInput) {
            InputStore_->Register(unprocessedInputs);
        }

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.Start"));

            // Build a flat batch of output messages and register them all at once.
            std::vector<TOutputMessageConstPtr> outputMessagePtrs;
            outputMessagePtrs.reserve(processResult.OutputMessages.size());
            const auto& specStorage = GetContext()->StreamSpecStorage;
            for (auto& outputMessage : processResult.OutputMessages) {
                outputMessagePtrs.push_back(New<TOutputMessage>(std::move(outputMessage), specStorage));
            }
            OutputStore_->RegisterBatch(outputMessagePtrs);
            RegisterOutputMessages(context, outputMessagePtrs, std::nullopt);
        }
        {
            // A swift timer with a task stays registered until a step processes the result and adds no task.
            TimerStore_->Unregister(acknowledgedTimers);
            TimerStore_->Register(std::move(processResult.OutputTimers));
        }

        ValidateTimerStoreLimits(dynamicSpec);

        auto tx = PrepareTransaction(context);

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Sync"));
            DoSync(tx);
            if (!processResult.FailedMessages.empty()) {
                YT_VERIFY(FailedMessages_);

                FailedMessages_->Write(
                    tx,
                    GetContext()->Partition->ComputationId,
                    GetContext()->Partition->PartitionId,
                    now,
                    processResult.FailedMessages);
            }
            YT_TLOG_INFO("Transaction prepared");
        }
        Commit(context, tx);

        context->MarkPersisted(unprocessedInputs);
        AsyncProcessorManager_->Start(std::move(tasks));

        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, BuildInflights(context, now));

        if (!emptyInput) {
            NoteNonEmptyRunIteration();
        }
        FinishRunIteration();

        WaitForBackoff(dynamicSpec, outputLimitsCheckResult, emptyInput);

        ClearAsynchronously(
            std::move(inputTimers),
            std::move(inputVisits),
            std::move(completedTasks),
            std::move(unprocessedInputs),
            std::move(filteredInputs),
            std::move(processResult));
    }
    YT_TLOG_INFO("Completed DoExecute");
}

void TTransformComputation::ProcessDistributedMessages(const IComputationRunContextPtr& /*context*/, std::deque<TOutputMessageConstPtr>&& messages)
{
    OutputStore_->TryUnregisterBatch(messages);
}

void TTransformComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    DoInit();
}

void TTransformComputation::DoInit()
{ }

void TTransformComputation::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    struct TKeyGroup
    {
        std::vector<TInputMessageConstPtr> Messages;
        std::vector<TInputTimerConstPtr> Timers;
        std::vector<TInputVisitConstPtr> Visits;
    };

    THashMap<TKey, TKeyGroup> groups;
    for (const auto& message : input->GetMessages()) {
        groups[message->Key].Messages.push_back(message);
    }
    for (const auto& timer : input->GetTimers()) {
        groups[timer->Key].Timers.push_back(timer);
    }
    for (const auto& visit : input->GetVisits()) {
        groups[visit->Key].Visits.push_back(visit);
    }
    for (const auto& [key, data] : groups) {
        TagErrorWithKey("key", key, [&] {
            DoProcessKey(
                New<TInputContext>(data.Messages, data.Timers, data.Visits),
                output->SetParents(data.Messages, data.Timers, data.Visits));
        });
    }
}

void TTransformComputation::DoProcessKey(IInputContextPtr input, IOutputCollectorPtr output)
{
    for (const auto& timer : input->GetTimers()) {
        DoProcessTimer(timer, output->SetParents({}, {timer}, {}));
    }
    for (const auto& message : input->GetMessages()) {
        DoProcessMessage(message, output->SetParents({message}, {}, {}));
    }
    for (const auto& visit : input->GetVisits()) {
        DoProcessVisit(visit, output->SetParents({}, {}, {visit}));
    }
}

void TTransformComputation::DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output)
{
    DoProcessMessage(*message, std::move(output));
}

void TTransformComputation::DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/)
{ }

void TTransformComputation::DoProcessTimer(const TInputTimerConstPtr& timer, IOutputCollectorPtr output)
{
    DoProcessTimer(*timer, std::move(output));
}

void TTransformComputation::DoProcessTimer(const TTimer& /*timer*/, IOutputCollectorPtr /*output*/)
{ }

void TTransformComputation::DoProcessVisit(const TInputVisitConstPtr& visit, IOutputCollectorPtr output)
{
    DoProcessVisit(*visit, std::move(output));
}

void TTransformComputation::DoProcessVisit(const TVisit& /*visit*/, IOutputCollectorPtr /*output*/)
{ }

void TTransformComputation::DoSync(IRetryableTransactionPtr /*transaction*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
