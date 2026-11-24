#include "delivery_validation.h"

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/ytree/convert.h>

#include <algorithm>
#include <limits>

namespace NYT::NFlow::NYdbTest {

////////////////////////////////////////////////////////////////////////////////

void TDeliveryProbeEnvelope::Register(TRegistrar registrar)
{
    registrar.Parameter("run_id", &TThis::RunId);
    registrar.Parameter("lane", &TThis::Lane);
    registrar.Parameter("producer_id", &TThis::ProducerId);
    registrar.Parameter("sequence", &TThis::Sequence);
    registrar.Parameter("probe_time", &TThis::ProbeTime);
    registrar.Parameter("bucket_start", &TThis::BucketStart);
    registrar.Parameter("data", &TThis::Data);
}

void TDeliveryProbeGeneratorState::Register(TRegistrar registrar)
{
    registrar.Parameter("next_sequence", &TThis::NextSequence)
        .Default(0);
    registrar.Parameter("last_probe_time", &TThis::LastProbeTime)
        .Default(0);
}

void TDeliveryProbeGeneratorParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("run_id", &TThis::RunId);
    registrar.Parameter("lane", &TThis::Lane);
    registrar.Parameter("window_size", &TThis::WindowSize).GreaterThan(TDuration::Zero());
    registrar.Parameter("generated_stream_id", &TThis::GeneratedStreamId).Default("generated");
    registrar.Parameter("expected_stream_id", &TThis::ExpectedStreamId).Default("expected");
}

void TDeliveryProbeGenerator::Init(const IRuntimeInitContextPtr& initContext)
{
    auto parameters = initContext->GetParameters<TDeliveryProbeGeneratorParameters>();
    RunId_ = parameters->RunId;
    Lane_ = parameters->Lane;
    WindowSize_ = parameters->WindowSize;
    GeneratedStreamId_ = parameters->GeneratedStreamId;
    ExpectedStreamId_ = parameters->ExpectedStreamId;
    initContext->InitClient<TDeliveryProbeGeneratorState>(GeneratorStateClient_, "generator");
}

void TDeliveryProbeGenerator::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = GeneratorStateClient_.GetState(message);
    const auto sequence = state->NextSequence++;
    THROW_ERROR_EXCEPTION_IF(sequence == std::numeric_limits<ui64>::max(), "Delivery probe sequence exhausted");
    const auto probeTime = std::max(
        state->LastProbeTime,
        context->GetCurrentTimestamp().Underlying());
    state->LastProbeTime = probeTime;
    const auto windowSeconds = WindowSize_.Seconds();
    const auto bucketStart = probeTime / windowSeconds * windowSeconds;
    const auto producerId = ToString(message->Key);

    auto envelope = New<TDeliveryProbeEnvelope>();
    envelope->RunId = RunId_;
    envelope->Lane = Lane_;
    envelope->ProducerId = producerId;
    envelope->Sequence = sequence;
    envelope->ProbeTime = probeTime;
    envelope->BucketStart = bucketStart;
    envelope->Data = Format("%v/%v/%v", RunId_, producerId, sequence);

    auto generated = context->MakeOutputMessageBuilder(GeneratedStreamId_);
    generated.SetEventTimestamp(TSystemTimestamp(probeTime));
    generated.Payload().Set(producerId, "key");
    generated.Payload().Set(std::string(NYson::ConvertToYsonString(envelope).AsStringBuf()), "data");
    output->AddMessage(generated.Finish());

    auto expected = context->MakeOutputMessageBuilder(ExpectedStreamId_);
    expected.SetEventTimestamp(TSystemTimestamp(probeTime));
    expected.Payload().Set(RunId_, "run_id");
    expected.Payload().Set(Lane_, "lane");
    expected.Payload().Set(producerId, "producer_id");
    expected.Payload().Set(bucketStart, "bucket_start");
    expected.Payload().Set(sequence, "first_sequence");
    expected.Payload().Set(sequence + 1, "issued_through");
    output->AddMessage(expected.Finish());
}

void TDeliveryProbeOutputReaderParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("run_id", &TThis::RunId);
    registrar.Parameter("lane", &TThis::Lane);
    registrar.Parameter("observed_stream_id", &TThis::ObservedStreamId).Default("observed");
}

void TDeliveryProbeOutputReader::Init(const IRuntimeInitContextPtr& initContext)
{
    auto parameters = initContext->GetParameters<TDeliveryProbeOutputReaderParameters>();
    RunId_ = parameters->RunId;
    Lane_ = parameters->Lane;
    ObservedStreamId_ = parameters->ObservedStreamId;
}

void TDeliveryProbeOutputReader::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto observed = context->MakeOutputMessageBuilder(ObservedStreamId_);
    observed.SetEventTimestamp(message->GetMeta().EventTimestamp);
    observed.Payload().Set(RunId_, "run_id");
    observed.Payload().Set(Lane_, "lane");
    observed.Payload().SetValue(GetColumn(message, "data"), "data");
    if (message->PayloadSchema->FindColumn("unparsed")) {
        observed.Payload().SetValue(GetColumn(message, "unparsed"), "unparsed");
    }
    output->AddMessage(observed.Finish());
}

void TDeliveryProbeExpectedReader::Init(const IRuntimeInitContextPtr& initContext)
{
    Parameters_ = initContext->GetParameters<TDeliveryProbeGeneratorParameters>();
}

void TDeliveryProbeExpectedReader::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto envelope = NYTree::ConvertTo<TDeliveryProbeEnvelopePtr>(
        NYson::TYsonString(GetColumnValue<std::string>(message, "data")));
    THROW_ERROR_EXCEPTION_UNLESS(envelope->RunId == Parameters_->RunId && envelope->Lane == Parameters_->Lane,
        "Unexpected reference fixture identity");
    THROW_ERROR_EXCEPTION_IF(envelope->Sequence == std::numeric_limits<ui64>::max(), "Reference sequence exhausted");
    auto expected = context->MakeOutputMessageBuilder(Parameters_->ExpectedStreamId);
    expected.SetEventTimestamp(message->GetMeta().EventTimestamp);
    expected.Payload().Set(Parameters_->RunId, "run_id");
    expected.Payload().Set(Parameters_->Lane, "lane");
    expected.Payload().Set(envelope->ProducerId, "producer_id");
    expected.Payload().Set(envelope->BucketStart, "bucket_start");
    expected.Payload().Set(envelope->Sequence, "first_sequence");
    expected.Payload().Set(envelope->Sequence + 1, "issued_through");
    output->AddMessage(expected.Finish());
}

void TDeliveryValidationWindow::Register(TRegistrar registrar)
{
    registrar.Parameter("bucket_start", &TThis::BucketStart).Default(0);
    registrar.Parameter("first_sequence", &TThis::FirstSequence).Default();
    registrar.Parameter("issued_through", &TThis::IssuedThrough).Default();
    registrar.Parameter("observed_sequences", &TThis::ObservedSequences).Default();
}

void TDeliveryValidationProducer::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_id", &TThis::ProducerId);
    registrar.Parameter("initial_sequence", &TThis::InitialSequence).Default();
    registrar.Parameter("closed_through", &TThis::ClosedThrough).Default(0);
    registrar.Parameter("windows", &TThis::Windows).Default();
}

void TDeliveryValidationState::Register(TRegistrar registrar)
{
    registrar.Parameter("first_error_kind", &TThis::FirstErrorKind).Default();
    registrar.Parameter("first_error_detail", &TThis::FirstErrorDetail).Default();
    registrar.Parameter("checked_probe_count", &TThis::CheckedProbeCount).Default(0);
    registrar.Parameter("allowed_duplicate_count", &TThis::AllowedDuplicateCount).Default(0);
    registrar.Parameter("producers", &TThis::Producers).Default();
}

void TDeliveryProbeCombinedValidatorParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("run_id", &TThis::RunId);
    registrar.Parameter("lane", &TThis::Lane);
    registrar.Parameter("exactly_once", &TThis::ExactlyOnce).Default(false);
    registrar.Parameter("window_size", &TThis::WindowSize).GreaterThan(TDuration::Zero());
    registrar.Parameter("expected_stream_id", &TThis::ExpectedStreamId).Default("expected");
    registrar.Parameter("observed_stream_id", &TThis::ObservedStreamId).Default("observed");
    registrar.Parameter("close_timer_stream_id", &TThis::CloseTimerStreamId).Default("close");
    registrar.Parameter("status_timer_stream_id", &TThis::StatusTimerStreamId).Default("status");
}

TDeliveryProbeValidator::TDeliveryProbeValidator(const TProcessFunctionContextPtr& functionContext)
    : ErrorState_(functionContext->StatusProfiler->ErrorState("delivery_validation"))
{ }

void TDeliveryProbeValidator::Init(const IRuntimeInitContextPtr& initContext)
{
    Parameters_ = initContext->GetParameters<TDeliveryProbeCombinedValidatorParameters>();
    THROW_ERROR_EXCEPTION_UNLESS(Parameters_->WindowSize.Seconds() > 0, "Probe windows require whole seconds");
    initContext->InitClient<TDeliveryValidationState>(StateClient_, "validation");
}

void TDeliveryProbeValidator::ProcessKey(
    const IInputContextPtr& input,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    constexpr size_t MaxProducers = 4;
    constexpr size_t MaxOpenWindows = 30;
    constexpr size_t MaxObservedSequences = 20'000;
    constexpr size_t MaxProducerIdLength = 512;
    constexpr size_t MaxErrorDetailLength = 1024;

    const auto& messages = input->GetMessages();
    const auto& timers = input->GetTimers();
    if (messages.empty() && timers.empty()) {
        return;
    }
    auto state = messages.empty()
        ? StateClient_.GetState(timers.front())
        : StateClient_.GetState(messages.front());
    const auto& parameters = *Parameters_;
    const auto now = context->GetCurrentTimestamp();
    const auto scheduleStatus = [&] {
        output->AddTimer(parameters.StatusTimerStreamId, TSystemTimestamp(now.Underlying() + 1), now);
    };
    // Only the state loaded at the epoch boundary is eligible for external reporting.
    if (!state->FirstErrorKind.empty()) {
        ErrorState_->SetError(TError("YDB delivery validation failed: %v (%v)",
            state->FirstErrorKind,
            state->FirstErrorDetail));
        if (std::any_of(timers.begin(), timers.end(), [&] (const auto& timer) {
                return timer->StreamId == parameters.StatusTimerStreamId;
            })) {
            scheduleStatus();
        }
        return;
    }
    const auto fail = [&] (std::string kind, std::string detail) {
        if (state->FirstErrorKind.empty()) {
            state->FirstErrorKind = std::move(kind);
            state->FirstErrorDetail = detail.substr(0, MaxErrorDetailLength);
            for (const auto& producer : state->Producers) {
                producer->Windows.clear();
            }
            scheduleStatus();
        }
    };
    const auto duplicate = [&] (const std::string& producerId, ui64 bucketStart, ui64 sequence) {
        if (parameters.ExactlyOnce) {
            fail("duplicate_exactly_once_probe", Format("sequence was observed more than once (ProducerId: %v, BucketStart: %v, Sequence: %v)", producerId, bucketStart, sequence));
        } else if (state->AllowedDuplicateCount == std::numeric_limits<ui64>::max()) {
            fail("validation_state_limit", "duplicate counter exhausted");
        } else {
            ++state->AllowedDuplicateCount;
        }
    };
    size_t openWindows = 0;
    size_t observedSequences = 0;
    for (const auto& producer : state->Producers) {
        openWindows += producer->Windows.size();
        for (const auto& window : producer->Windows) {
            observedSequences += window->ObservedSequences.size();
        }
    }

    for (const auto& message : messages) {
        if (!state->FirstErrorKind.empty()) {
            break;
        }
        std::string producerId;
        ui64 bucketStart = 0;
        ui64 first = 0;
        ui64 last = 0;
        ui64 sequence = 0;
        const bool expected = message->StreamId == parameters.ExpectedStreamId;
        try {
            if (GetColumnValue<std::string>(message, "run_id") != parameters.RunId ||
                GetColumnValue<std::string>(message, "lane") != parameters.Lane)
            {
                fail("corrupt_probe", "trusted tags do not match the configured run/lane");
                continue;
            }
            if (expected) {
                auto range = context->ConvertToYsonMessage<TExpectedProbeRange>(message);
                producerId = range->ProducerId;
                bucketStart = range->BucketStart;
                first = range->FirstSequence;
                last = range->IssuedThrough;
                if (first >= last) {
                    fail("invalid_expected_range", "expected range is empty or reversed");
                    continue;
                }
                if (last - first > MaxObservedSequences) {
                    fail("validation_state_limit", "expected range exceeds the sequence cap");
                    continue;
                }
            } else if (message->StreamId == parameters.ObservedStreamId) {
                if (message->PayloadSchema->FindColumn("unparsed") && GetColumn(message, "unparsed").Type != NTableClient::EValueType::Null) {
                    fail("malformed_probe", "output record contains unparsed framing data");
                    continue;
                }
                auto envelope = NYTree::ConvertTo<TDeliveryProbeEnvelopePtr>(
                    NYson::TYsonString(GetColumnValue<std::string>(message, "data")));
                producerId = envelope->ProducerId;
                bucketStart = envelope->BucketStart;
                sequence = envelope->Sequence;
                if (envelope->RunId != parameters.RunId || envelope->Lane != parameters.Lane ||
                    bucketStart != envelope->ProbeTime / parameters.WindowSize.Seconds() * parameters.WindowSize.Seconds() ||
                    envelope->Data != Format("%v/%v/%v", envelope->RunId, producerId, sequence))
                {
                    fail("corrupt_probe", "probe identity, bucket or payload does not match the active run");
                    continue;
                }
            } else {
                THROW_ERROR_EXCEPTION("Unknown delivery validation stream %Qv", message->StreamId);
            }
        } catch (const std::exception& ex) {
            fail("malformed_probe", ex.what());
            continue;
        }
        if (producerId.empty() || producerId.size() > MaxProducerIdLength ||
            bucketStart % parameters.WindowSize.Seconds() != 0 ||
            bucketStart > MaxAdequateTimestamp.Underlying() - parameters.WindowSize.Seconds())
        {
            fail("corrupt_probe", "invalid producer identity or window timestamp");
            continue;
        }
        auto producerIt = std::find_if(state->Producers.begin(), state->Producers.end(), [&] (const auto& producer) {
            return producer->ProducerId == producerId;
        });
        if (producerIt == state->Producers.end()) {
            if (state->Producers.size() == MaxProducers) {
                fail("validation_state_limit", "producer limit exceeded");
                continue;
            }
            auto producer = New<TDeliveryValidationProducer>();
            producer->ProducerId = producerId;
            state->Producers.push_back(producer);
            producerIt = state->Producers.end() - 1;
        }
        const auto& producer = *producerIt;
        if (producer->InitialSequence) {
            if (expected && first >= *producer->InitialSequence && last <= producer->ClosedThrough) {
                continue;
            }
            if (!expected && sequence < producer->ClosedThrough) {
                if (sequence < *producer->InitialSequence) {
                    fail("corrupt_probe", "sequence precedes this run's trusted initial sequence");
                } else {
                    duplicate(producerId, bucketStart, sequence);
                }
                continue;
            }
        }
        if (!expected && std::any_of(producer->Windows.begin(), producer->Windows.end(), [&] (const auto& window) {
                return std::binary_search(window->ObservedSequences.begin(), window->ObservedSequences.end(), sequence);
            })) {
            duplicate(producerId, bucketStart, sequence);
            continue;
        }
        auto windowIt = std::lower_bound(producer->Windows.begin(), producer->Windows.end(), bucketStart, [] (const auto& window, ui64 bucket) {
            return window->BucketStart < bucket;
        });
        if (windowIt == producer->Windows.end() || (*windowIt)->BucketStart != bucketStart) {
            if (openWindows == MaxOpenWindows) {
                fail("validation_state_limit", "open window limit exceeded");
                continue;
            }
            auto window = New<TDeliveryValidationWindow>();
            window->BucketStart = bucketStart;
            windowIt = producer->Windows.insert(windowIt, window);
            ++openWindows;
            output->AddTimer(parameters.CloseTimerStreamId,
                TSystemTimestamp(bucketStart + parameters.WindowSize.Seconds()),
                TSystemTimestamp(bucketStart));
        }
        const auto& window = *windowIt;
        if (expected) {
            window->FirstSequence = window->FirstSequence ? std::min(*window->FirstSequence, first) : first;
            window->IssuedThrough = window->IssuedThrough ? std::max(*window->IssuedThrough, last) : last;
            if (*window->IssuedThrough - *window->FirstSequence > MaxObservedSequences) {
                fail("validation_state_limit", "merged expected range exceeds the sequence cap");
            }
        } else {
            auto it = std::lower_bound(window->ObservedSequences.begin(), window->ObservedSequences.end(), sequence);
            if (it != window->ObservedSequences.end() && *it == sequence) {
                duplicate(producerId, bucketStart, sequence);
            } else if (observedSequences == MaxObservedSequences) {
                fail("validation_state_limit", "observed sequence limit exceeded");
            } else {
                window->ObservedSequences.insert(it, sequence);
                ++observedSequences;
            }
        }
    }
    if (!state->FirstErrorKind.empty() || !std::any_of(timers.begin(), timers.end(), [&] (const auto& timer) {
            return timer->StreamId == parameters.CloseTimerStreamId;
        })) {
        return;
    }
    const auto watermark = std::min(context->GetWatermark(parameters.ExpectedStreamId), context->GetWatermark(parameters.ObservedStreamId));
    for (const auto& producer : state->Producers) {
        while (!producer->Windows.empty() &&
            producer->Windows.front()->BucketStart + parameters.WindowSize.Seconds() <= watermark.Underlying())
        {
            const auto& window = producer->Windows.front();
            if (!window->FirstSequence || !window->IssuedThrough) {
                fail("unexpected_observation", "closed window has no trusted expected range");
                return;
            }
            const auto first = *window->FirstSequence;
            const auto last = *window->IssuedThrough;
            if (producer->InitialSequence && first != producer->ClosedThrough) {
                fail("delivery_loss", "expected coverage does not continue the closed sequence frontier");
                return;
            }
            const auto count = last - first;
            const auto& observed = window->ObservedSequences;
            if (observed.size() != count || observed.empty() || observed.front() != first || observed.back() != last - 1) {
                fail("delivery_loss", Format("observed set does not cover expected range (ProducerId: %v, BucketStart: %v, FirstSequence: %v, IssuedThrough: %v)", producer->ProducerId, window->BucketStart, first, last));
                return;
            }
            if (state->CheckedProbeCount > std::numeric_limits<ui64>::max() - count) {
                fail("validation_state_limit", "checked probe counter exhausted");
                return;
            }
            if (!producer->InitialSequence) {
                producer->InitialSequence = first;
            }
            producer->ClosedThrough = last;
            state->CheckedProbeCount += count;
            producer->Windows.erase(producer->Windows.begin());
        }
    }
}

void TExpectedProbeRange::Register(TRegistrar registrar)
{
    registrar.Parameter("run_id", &TThis::RunId).Default();
    registrar.Parameter("lane", &TThis::Lane).Default();
    registrar.Parameter("producer_id", &TThis::ProducerId).Default();
    registrar.Parameter("bucket_start", &TThis::BucketStart).Default(0);
    registrar.Parameter("first_sequence", &TThis::FirstSequence).Default(0);
    registrar.Parameter("issued_through", &TThis::IssuedThrough).Default(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTest
