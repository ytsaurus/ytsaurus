#include "transform_computation.h"

#include "computation_tracer.h"
#include "key_visitor.h"
#include "meta_setter.h"
#include "stores/input_store.h"
#include "stores/output_store.h"
#include "stores/timer_store.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

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
{ }

void TTransformComputation::DoPrepare(const IComputationRunContextPtr& context)
{
    InitOutputStoreDistribution(context, /*allowOutputDuplicates*/ false);
}

void TTransformComputation::DoExecute(const IComputationRunContextPtr& context, TTraceContextGuard&& initTraceContextGuard)
{
    YT_LOG_INFO("Started DoExecute");
    YT_VERIFY(TimerStore_);
    YT_VERIFY(InputStore_);
    WaitFor(TimerStore_->Init()).ThrowOnError();
    WaitFor(InputStore_->Init()).ThrowOnError();
    for (const auto& [_, visitor] : KeyVisitors_) {
        WaitFor(visitor->Init()).ThrowOnError();
    }

    bool isFinished = true;
    {
        auto iterGuard = StartRunIteration(context);
        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();
        DoInit(StateManager_->CreateContext());
        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, BuildInflights());
        FinishRunIteration();
    }

    initTraceContextGuard.Release();
    YT_LOG_INFO("Init completed");
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
        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();
        YT_LOG_INFO("Got batch (Inputs: %v, Timers: %v, Visits: %v)",
            inputs.size(),
            inputTimers.size(),
            inputVisits.size());

        auto unprocessedInputs = [&] () {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Deduplicate"));
            auto [processedInput, unprocessedInputs] = InputStore_->Filter(inputs, deduplicateInput);
            YT_LOG_INFO("Filtered already processed (Inputs: %v)",
                processedInput.size());
            context->MarkPersisted(processedInput);
            return unprocessedInputs;
        }();

        ThrottleInputBatch(unprocessedInputs, inputTimers, inputVisits);

        TRootOutputCollector::TTransformResult processResult;
        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Process"));
            RegisterInputBeforeProcessing(unprocessedInputs, inputTimers, inputVisits);
            auto inputContext = New<TInputContext>(unprocessedInputs, inputTimers, inputVisits);
            auto metaSetter = CreateUniqueMetaSetter(GetSpec(), uniqueSeqNo, now, EventTimestampAssigner_);
            auto outputCollector = New<TRootOutputCollector>(GetSpec(), metaSetter);
            PreloadKeyStates(inputContext);
            DoProcess(inputContext, outputCollector->SetParents(inputContext->GetMessages(), inputContext->GetTimers(), inputContext->GetVisits()));
            processResult = outputCollector->CollectResult();
        }

        YT_LOG_INFO("Process completed (OutputTimers: %v, OutputMessages: %v)",
            processResult.OutputTimers.size(),
            processResult.OutputMessages.size());

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
            RegisterOutputMessages(context, outputMessagePtrs, std::nullopt, dynamicSpec);
        }
        {
            TimerStore_->Unregister(inputTimers);
            TimerStore_->Register(std::move(processResult.OutputTimers));
        }

        ValidateTimerStoreLimits(dynamicSpec);

        auto tx = PrepareTransaction(context);

        {
            TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Sync"));
            DoSync(tx);
            YT_LOG_INFO("Transaction prepared");
        }
        Commit(context, tx);

        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, BuildInflights());

        context->MarkPersisted(unprocessedInputs);

        FinishRunIteration();

        WaitForBackoff(dynamicSpec, outputLimitsCheckResult,
            /*emptyInput*/ inputs.empty() && inputTimers.empty() && inputVisits.empty());

        ClearAsynchronously(
            std::move(inputs),
            std::move(inputTimers),
            std::move(inputVisits),
            std::move(unprocessedInputs),
            std::move(processResult));
    }
    YT_LOG_INFO("Completed DoExecute");
}

void TTransformComputation::ProcessDistributedMessages(const IComputationRunContextPtr& /*context*/, std::deque<TOutputMessageConstPtr>&& messages)
{
    OutputStore_->AsyncUnregisterBatch(messages);
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
    for (const auto& data : GetValues(groups)) {
        DoProcessKey(
            New<TInputContext>(data.Messages, data.Timers, data.Visits),
            output->SetParents(data.Messages, data.Timers, data.Visits));
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
