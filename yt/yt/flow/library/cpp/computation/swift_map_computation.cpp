#include "swift_map_computation.h"

#include "computation_tracer.h"
#include "key_visitor.h"
#include "meta_setter.h"
#include "stores/input_store.h"
#include "stores/output_store.h"
#include "stores/timer_store.h"

#include <yt/yt/flow/library/cpp/common/key_error.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/flow/library/cpp/misc/prefetch.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NFlow {

using namespace NTracing;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TSwiftMapComputationOutputMessage
    : public TOutputMessage
{
    using TOutputMessage::TOutputMessage;

    TOnDistributedCallback OnDistributedCallback;
};

using TSwiftMapComputationOutputMessagePtr = TIntrusivePtr<TSwiftMapComputationOutputMessage>;

////////////////////////////////////////////////////////////////////////////////

void TSwiftMapComputation::TExtendedParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("allow_batching_with_relaxed_guarantees", &TThis::AllowBatchingWithRelaxedGuarantees)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSwiftMapComputation::TExtendedDynamicParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TSwiftMapComputation::ValidateSpec(const TComputationSpec& spec)
{
    if (spec.InputStreamIds.empty()) {
        THROW_ERROR_EXCEPTION("TSwiftMapComputation requires input streams");
    }

    for (const auto& outputStreamId : spec.OutputStreamIds) {
        auto parentStreamIds = GetOrDefault(spec.StreamsDependency, outputStreamId);
        auto validateNotParent = [&] (const TStreamId& streamId, TStringBuf streamKind) {
            THROW_ERROR_EXCEPTION_IF(parentStreamIds.contains(streamId),
                "Output stream %Qv of TSwiftMapComputation must not depend on %v stream %Qv; "
                "exclude the stream from the output's \"streams_dependency\" (note that by "
                "default every timer stream is auto-added to each output's dependencies, so a "
                "swift spec with both timers and outputs must set \"streams_dependency\" "
                "explicitly)",
                outputStreamId,
                streamKind,
                streamId);
        };
        for (const auto& timerStreamId : GetKeys(spec.TimerStreams)) {
            validateNotParent(timerStreamId, "timer");
        }
        for (const auto& visitStreamId : GetKeys(spec.KeyVisitorStreams)) {
            validateNotParent(visitStreamId, "key-visitor");
        }
    }

    if (!spec.SourceStreams.empty()) {
        THROW_ERROR_EXCEPTION("TSwiftMapComputation does not support source streams");
    }
    if (!spec.Sinks.empty()) {
        THROW_ERROR_EXCEPTION("TSwiftMapComputation does not support sinks");
    }
    if (spec.WatermarkStrategy && spec.WatermarkStrategy->WatermarkGenerator) {
        THROW_ERROR_EXCEPTION("TSwiftMapComputation does not support watermark generator");
    }
    if (spec.WatermarkStrategy && spec.WatermarkStrategy->WatermarkAlignment) {
        THROW_ERROR_EXCEPTION("TSwiftMapComputation does not support watermark alignment");
    }
}

////////////////////////////////////////////////////////////////////////////////

TSwiftMapComputation::TSwiftMapComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TUniversalComputationBase(std::move(context), std::move(dynamicContext))
{ }

void TSwiftMapComputation::DoPrepare(const IComputationRunContextPtr& context)
{
    if (GetParameters()->AllowBatchingWithRelaxedGuarantees) {
        YT_LOG_WARNING("AllowBatchingWithRelaxedGuarantees is enabled: outputs may merge several parents, per-key MessageId order "
            "is not preserved and downstream must tolerate at-least-once delivery");
    }
    InitOutputStoreDistribution(context, /*allowOutputDuplicates*/ true);
}

void TSwiftMapComputation::DoExecute(const IComputationRunContextPtr& context, TTraceContextGuard&& initTraceContextGuard)
{
    YT_LOG_INFO("Started DoExecute");
    YT_VERIFY(InputStore_);
    YT_VERIFY(TimerStore_);
    WaitFor(InputStore_->Init()).ThrowOnError();
    WaitFor(TimerStore_->Init()).ThrowOnError();
    for (const auto& [_, visitor] : KeyVisitors_) {
        WaitFor(visitor->Init()).ThrowOnError();
    }

    bool isFinished = true;
    {
        auto iterGuard = StartRunIteration(context);
        auto generateReportTimeFuture = GetTimeProvider()->GetTimestamp(/*barrier*/ true);
        DoInit(StateManager_->CreateContext());
        const auto now = WaitFor(generateReportTimeFuture).ValueOrThrow();
        isFinished = UpdateStatus(/*reportTime*/ now, GetInputSystemWatermark(), BuildInflights());
        FinishRunIteration();
    }

    initTraceContextGuard.Release();
    YT_LOG_INFO("Init completed");

    while (!isFinished) {
        auto iterGuard = StartRunIteration(context);
        auto dynamicSpec = GetDynamicSpec();
        const auto allowBatchingWithRelaxedGuarantees = GetParameters()->AllowBatchingWithRelaxedGuarantees;
        // Generated timestamp can be less than timestamps of input messages of this epoch. This is OK.
        auto generateReportTimeFuture = GetTimeProvider()->GenerateGlobalUniqueSeqNo();

        const auto outputLimitsCheckResult = CheckOutputLimits(dynamicSpec, GetDynamicPartitionSpec());

        std::vector<TInputMessageConstPtr> inputs;
        std::vector<TInputTimerConstPtr> inputTimers;
        std::vector<TInputVisitConstPtr> inputVisits;
        if (!outputLimitsCheckResult.AllowedInputStreams.empty()) {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Fetch"));
            auto inputsFuture = context->GetNextBatch(outputLimitsCheckResult.AllowedInputStreams);
            inputTimers = TimerStore_->GetNextBatch(outputLimitsCheckResult.AllowedInputStreams, dynamicSpec->MaxRowsPerBatch, dynamicSpec->MaxBytesPerBatch);
            inputs = WaitFor(inputsFuture).ValueOrThrow();

            std::vector<TKeyVisitorPtr> allowedVisitors;
            for (const auto& [streamId, visitor] : KeyVisitors_) {
                if (outputLimitsCheckResult.AllowedInputStreams.contains(streamId)) {
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
        YT_LOG_INFO("Got batch (Inputs: %v, Timers: %v, Visits: %v)",
            inputs.size(),
            inputTimers.size(),
            inputVisits.size());

        auto unprocessedInputs = [&] () {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Deduplicate"));
            auto [processedInput, unprocessedInputs] = InputStore_->Filter(inputs, /*checkState*/ false);
            YT_LOG_INFO("Filtered already processed (Inputs: %v)",
                processedInput.size());
            context->MarkPersisted(processedInput);
            return unprocessedInputs;
        }();

        ThrottleInputBatch(unprocessedInputs, inputTimers, inputVisits);

        // For batching we need uniqueSeqNo before Process to seed the merge meta setter; wait outside the
        // Process trace guard so the wait isn't billed to "Process". Non-batching keeps the original overlap.
        if (allowBatchingWithRelaxedGuarantees) {
            WaitUntilSet(generateReportTimeFuture.AsVoid());
        }

        std::vector<TSwiftMapComputationOutputMessagePtr> outputMessages;
        std::vector<TMessageParentsConstPtr> outputParents;
        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Process"));
            RegisterInputBeforeProcessing(unprocessedInputs, inputTimers, inputVisits);
            auto inputContext = New<TInputContext>(unprocessedInputs, inputTimers, inputVisits);
            auto metaSetter = allowBatchingWithRelaxedGuarantees
                ? CreateSwiftMergeMetaSetter(
                    GetSpec(),
                    WaitForFast(generateReportTimeFuture).ValueOrThrow().UniqueSeqNo,
                    EventTimestampAssigner_)
                : CreateDeterministicMetaSetter(GetSpec(), EventTimestampAssigner_);
            auto outputCollector = New<TRootOutputCollector>(GetSpec(), metaSetter);
            PreloadKeyStates(inputContext);
            DoProcess(inputContext, outputCollector->SetParents(inputContext->GetMessages(), inputContext->GetTimers(), inputContext->GetVisits()));
            auto result = outputCollector->CollectResult();
            TimerStore_->Unregister(inputTimers);
            TimerStore_->Register(std::move(result.OutputTimers));
            const auto& streamSpecStorage = GetContext()->StreamSpecStorage;
            outputMessages.reserve(result.OutputMessages.size());
            outputParents.reserve(result.OutputMessages.size());
            for (auto&& [outputMessage, parents] : Zip(result.OutputMessages, result.OutputMessagesParentMessageIds)) {
                if (parents->ParentMessages.empty() || !parents->ParentTimers.empty()) {
                    THROW_ERROR_EXCEPTION("Output message must have at least one parent message and no parent timers (ParentMessages: %v, ParentTimers: %v)",
                        parents->ParentMessages.size(),
                        parents->ParentTimers.size());
                }
                if (!allowBatchingWithRelaxedGuarantees && parents->ParentMessages.size() != 1) {
                    THROW_ERROR_EXCEPTION("Output message has %v parents; merging requires allow_batching_with_relaxed_guarantees=true",
                        parents->ParentMessages.size());
                }
                outputMessages.push_back(New<TSwiftMapComputationOutputMessage>(std::move(outputMessage), streamSpecStorage));
                outputParents.push_back(std::move(parents));
            }
        }

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.Start"));

            // unprocessedInputs owns the keys for the rest of this iteration, so raw pointers are fine.
            absl::flat_hash_map<const TInputMessage*, int> inputMessageIndices;
            inputMessageIndices.reserve(unprocessedInputs.size());
            for (int i = 0; i < std::ssize(unprocessedInputs); ++i) {
                inputMessageIndices[unprocessedInputs[i].Get()] = i;
            }

            // One tracker per input parent. A parent is marked persisted only after all its children have been distributed.
            std::vector<TDistributingTracker> parentTrackers;
            parentTrackers.reserve(unprocessedInputs.size());
            for (const auto& input : unprocessedInputs) {
                parentTrackers.emplace_back([this, messageId = input->MessageId] {
                    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker,
                        "Callback must be called only from ProcessDistributedMessages() or in Activate() synchronously");
                    PersistedInputMessageIds_.push_back(messageId);
                });
            }

            // Batching: one merge tracker per unique TMessageParents pointer with multiple parents. The same
            // parents pointer is shared by all outputs emitted from one SetParents() call, so dedup by raw
            // pointer keeps parent-tracker fan-in linear in the number of unique parents-sets, not outputs.
            // With batching disabled every output has a single parent and this map stays empty.
            absl::flat_hash_map<const TMessageParents*, TDistributingTracker> mergeTrackers;
            for (const auto& parents : outputParents) {
                if (parents->ParentMessages.size() <= 1) {
                    continue;
                }
                auto [it, inserted] = mergeTrackers.try_emplace(parents.Get());
                if (!inserted) {
                    continue;
                }
                std::vector<TOnDistributedCallback> parentCallbacks;
                parentCallbacks.reserve(parents->ParentMessages.size());
                for (const auto& parent : parents->ParentMessages) {
                    const int parentIdx = GetOrCrash(inputMessageIndices, parent.Get());
                    parentCallbacks.push_back(parentTrackers[parentIdx].AddDestination());
                }
                it->second = TDistributingTracker([callbacks = std::move(parentCallbacks)] () mutable {
                    for (auto& callback : callbacks) {
                        callback();
                    }
                });
            }

            // Wire every output to its single parent's tracker, or to its merge tracker when it has many.
            for (auto&& [outputMessage, parents] : Zip(outputMessages, outputParents)) {
                if (parents->ParentMessages.size() == 1) {
                    const int parentIdx = GetOrCrash(inputMessageIndices, parents->ParentMessages[0].Get());
                    outputMessage->OnDistributedCallback = parentTrackers[parentIdx].AddDestination();
                } else {
                    outputMessage->OnDistributedCallback = mergeTrackers.at(parents.Get()).AddDestination();
                }
            }

            for (auto& [_, tracker] : mergeTrackers) {
                tracker.Activate();
            }
            for (auto& tracker : parentTrackers) {
                tracker.Activate();
            }

            // Register all output messages in one batch.
            std::vector<TOutputMessageConstPtr> outputMessagesBase(outputMessages.begin(), outputMessages.end());
            OutputStore_->TryRegisterBatch(outputMessagesBase, /*persist=*/false);
            RegisterOutputMessages(context, outputMessagesBase, std::nullopt, dynamicSpec);

            YT_LOG_INFO("Process completed (OutputMessages: %v)",
                outputMessages.size());
        }

        ValidateTimerStoreLimits(dynamicSpec);

        // May be empty to enforce lease check.
        auto tx = PrepareTransaction(context);
        Commit(context, tx);

        const auto now = WaitForFast(generateReportTimeFuture).ValueOrThrow().Timestamp;
        isFinished = UpdateStatus(/*reportTime*/ now, GetInputSystemWatermark(), BuildInflights());
        FinishRunIteration();

        WaitForBackoff(dynamicSpec, outputLimitsCheckResult,
            /*emptyInput*/ inputs.empty() && inputTimers.empty() && inputVisits.empty());

        ClearAsynchronously(
            std::move(inputs),
            std::move(inputTimers),
            std::move(inputVisits),
            std::move(unprocessedInputs),
            std::move(outputMessages),
            std::move(outputParents));
    }
}

TSystemTimestamp TSwiftMapComputation::GetInputSystemWatermark()
{
    return InputStore_->GetSystemWatermark();
}

void TSwiftMapComputation::ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages)
{
    OutputStore_->TryUnregisterBatch(messages);
    MakePrefetcher()
        .Add([] (const TOutputMessageConstPtr& message) {
            Y_PREFETCH_READ(message.Get(), 3);
        })
        .ForEach(messages, [] (const TOutputMessageConstPtr& message) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            static_cast<TSwiftMapComputationOutputMessage*>(const_cast<TOutputMessage*>(message.Get()))->OnDistributedCallback();
        });
    context->MarkPersisted(std::exchange(PersistedInputMessageIds_, {}));
}

void TSwiftMapComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    DoInit();
}

void TSwiftMapComputation::DoInit()
{ }

void TSwiftMapComputation::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    struct TKeyGroup
    {
        std::vector<TInputMessageConstPtr> Messages;
        std::vector<TInputTimerConstPtr> Timers;
        std::vector<TInputVisitConstPtr> Visits;
    };

    THashMap<TKey, TKeyGroup> groups;
    MakePrefetcher()
        .Add([] (const TInputMessageConstPtr& message) {
            Y_PREFETCH_READ(message.Get(), 3);
        })
        .Add([] (const TInputMessageConstPtr& message) {
            message->Key.Underlying().Prefetch();
        })
        .ForEach(input->GetMessages(), [&] (const TInputMessageConstPtr& message) {
            groups[message->Key].Messages.push_back(message);
        });
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

void TSwiftMapComputation::DoProcessKey(IInputContextPtr input, IOutputCollectorPtr output)
{
    for (const auto& timer : input->GetTimers()) {
        DoProcessTimer(timer, output->SetParents({}, {timer}, {}));
    }
    MakePrefetcher()
        .Add([] (const TInputMessageConstPtr& message) {
            Y_PREFETCH_READ(message.Get(), 3);
        })
        .Add([] (const TInputMessageConstPtr& message) {
            message->Payload.Underlying().Prefetch();
        })
        .ForEach(input->GetMessages(), [&] (const TInputMessageConstPtr& message) {
            DoProcessMessage(message, output->SetParents({message}, {}, {}));
        });
    for (const auto& visit : input->GetVisits()) {
        DoProcessVisit(visit, output->SetParents({}, {}, {visit}));
    }
}

void TSwiftMapComputation::DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output)
{
    DoProcessMessage(*message, std::move(output));
}

void TSwiftMapComputation::DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/)
{ }

void TSwiftMapComputation::DoProcessTimer(const TInputTimerConstPtr& timer, IOutputCollectorPtr output)
{
    DoProcessTimer(*timer, std::move(output));
}

void TSwiftMapComputation::DoProcessTimer(const TTimer& /*timer*/, IOutputCollectorPtr /*output*/)
{ }

void TSwiftMapComputation::DoProcessVisit(const TInputVisitConstPtr& visit, IOutputCollectorPtr output)
{
    DoProcessVisit(*visit, std::move(output));
}

void TSwiftMapComputation::DoProcessVisit(const TVisit& /*visit*/, IOutputCollectorPtr /*output*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
