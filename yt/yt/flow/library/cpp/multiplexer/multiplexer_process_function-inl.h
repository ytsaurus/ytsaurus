#pragma once

#ifndef MULTIPLEXER_PROCESS_FUNCTION_INL_H_
    #error "Direct inclusion of this file is not allowed, include multiplexer_process_function.h"
    #include "multiplexer_process_function.h"
#endif

#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TUserState>
TMultiplexerProcessFunction<TUserState>::TMultiplexerProcessFunction(
    const TProcessFunctionContextPtr& context)
    : Logger_(context->Logger)
{ }

template <class TUserState>
void TMultiplexerProcessFunction<TUserState>::Init(
    const IRuntimeInitContextPtr& context)
{
    auto multiplexerContext = context->WithPrefix("multiplexer");
    multiplexerContext->InitClient(InternalStateClient_, "internal");
    multiplexerContext->InitClient(UserStateClient_, "user");
}

template <class TUserState>
void TMultiplexerProcessFunction<TUserState>::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = InternalStateClient_.GetState(message->Key);
    auto userState = UserStateClient_.GetState(message->Key);

    OnInputMessage(message->Key, message, userState, context);

    if (!state->IsActive) {
        state->IsActive = true;
        state->Offset = std::nullopt;
        state->InitialStartOffset = std::nullopt;
        state->InSecondPhase = false;
        output->AddTimer(GetNextTimerFiring(message->Key, context));
    } else {
        state->InitialStartOffset = state->Offset;
        state->InSecondPhase = false;
    }
}

template <class TUserState>
void TMultiplexerProcessFunction<TUserState>::ProcessTimer(
    const TInputTimerConstPtr& timer,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = InternalStateClient_.GetState(timer->Key);
    auto userState = UserStateClient_.GetState(timer->Key);
    if (!state->IsActive) {
        state.Clear();
        userState.Clear();
        return;
    }

    auto currentOffsetSchema = GetCurrentOffsetSchema(context);
    if (currentOffsetSchema &&
        state->OffsetSchema &&
        *currentOffsetSchema != *state->OffsetSchema)
    {
        YT_LOG_EVENT(Logger_, NLogging::ELogLevel::Info,
            "Offset schema changed, restarting iteration from scratch "
            "(Key: %v, OldSchema: %v, NewSchema: %v)",
            timer->Key,
            *state->OffsetSchema,
            *currentOffsetSchema);
        state->Offset = std::nullopt;
        state->InitialStartOffset = std::nullopt;
        state->InSecondPhase = false;
    }
    state->OffsetSchema = currentOffsetSchema;

    auto parameters = context->GetDynamicParameters<TDynamicMultiplexerParameters>();
    std::optional<TKey> endOffset;
    if (state->InSecondPhase) {
        endOffset = state->InitialStartOffset;
    }

    auto nextOffset = FetchBatch(
        timer->Key,
        state->Offset,
        endOffset,
        parameters->BatchSize,
        userState,
        output,
        context);

    if (nextOffset) {
        if (state->Offset && !(*nextOffset > *state->Offset)) {
            THROW_ERROR_EXCEPTION("FetchBatch returned non-monotonic offset for key %v", timer->Key)
                .With("current_offset", state->Offset)
                .With("next_offset", *nextOffset);
        }
        if (endOffset && !(*nextOffset <= *endOffset)) {
            THROW_ERROR_EXCEPTION("FetchBatch overshot endOffsetInclusive for key %v", timer->Key)
                .With("next_offset", *nextOffset)
                .With("end_offset", *endOffset);
        }

        state->Offset = std::move(nextOffset);
        output->AddTimer(GetNextTimerFiring(timer->Key, context));
    } else if (!state->InSecondPhase && state->InitialStartOffset) {
        state->Offset = std::nullopt;
        state->InSecondPhase = true;
        output->AddTimer(GetNextTimerFiring(timer->Key, context));
    } else {
        state.Clear();
        userState.Clear();
    }
}

template <class TUserState>
void TMultiplexerProcessFunction<TUserState>::OnInputMessage(
    const TKey& /*key*/,
    const TInputMessageConstPtr& /*message*/,
    TStateAccessor<TUserState>& /*userState*/,
    const IRuntimeContextPtr& /*context*/)
{ }

template <class TUserState>
NTableClient::TTableSchemaPtr TMultiplexerProcessFunction<TUserState>::GetCurrentOffsetSchema(
    const IRuntimeContextPtr& /*context*/)
{
    return nullptr;
}

template <class TUserState>
TSystemTimestamp TMultiplexerProcessFunction<TUserState>::GetNextTimerFiring(
    const TKey& key,
    const IRuntimeContextPtr& context) const
{
    auto parameters = context->GetDynamicParameters<TDynamicMultiplexerParameters>();
    ui64 period = parameters->TimerPeriod.Seconds();
    ui64 shift = THash<TKey>()(key) % period;
    ui64 timestamp = context->GetCurrentTimestamp().Underlying();
    return TSystemTimestamp(timestamp - timestamp % period + shift + period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
