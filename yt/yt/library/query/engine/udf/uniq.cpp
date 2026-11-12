#include "uniq_state.h"

using namespace NYT::NQueryClient::NUdf;

extern "C" ui64 UniqGetFingerprint(TUnversionedValue* valueBegin, int valueCount);

// uniq

extern "C" void uniq_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    TUniqState::AllocateState(context, result, TUniqState::DefaultBufferSizePower);
}

extern "C" void uniq_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValues,
    int valueCount)
{
    auto* uniqState = reinterpret_cast<TUniqState*>(state->Data.String);

    auto hash = static_cast<ui32>(UniqGetFingerprint(newValues, valueCount));

    if (!uniqState->IsGood(hash)) {
        *result = *state;
        return;
    }

    uniqState->InsertImpl(hash);

    TUniqState::Adapt(context, state);

    *result = *state;
}

extern "C" void uniq_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    auto* leftUniqState = reinterpret_cast<TUniqState*>(leftState->Data.String);
    auto* rightUniqState = reinterpret_cast<TUniqState*>(rightState->Data.String);

    if (rightUniqState->SamplingDegree > leftUniqState->SamplingDegree) {
        leftUniqState->SamplingDegree = rightUniqState->SamplingDegree;
        leftUniqState->Rehash();
    }

    if (rightUniqState->HasSentinel && !leftUniqState->HasSentinel) {
        leftUniqState->HasSentinel = true;
        leftUniqState->Size++;

        leftUniqState = TUniqState::Adapt(context, leftState);
    }

    auto rightBufferSize = rightUniqState->GetBufferSize();
    for (ui32 index = 0; index < rightBufferSize; ++index) {
        auto hash = rightUniqState->Buffer[index];
        if (hash != TUniqState::SentinelHash && leftUniqState->IsGood(hash)) {
            leftUniqState->InsertImpl(hash);
            leftUniqState = TUniqState::Adapt(context, leftState);
        }
    }

    *result = *leftState;
}

extern "C" void uniq_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = EValueType::Uint64;
    auto* uniqState = reinterpret_cast<TUniqState*>(state->Data.String);

    result->Data.Uint64 = uniqState->Estimate();
}

// uniq_state

extern "C" void uniq_state_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    uniq_init(context, result);
}

extern "C" void uniq_state_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValues,
    int valueCount)
{
    uniq_update(context, result, state, newValues, valueCount);
}

extern "C" void uniq_state_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_state_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    *result = *state;
}

// uniq_merge

extern "C" void uniq_merge_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    uniq_init(context, result);
}

extern "C" void uniq_merge_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    uniq_finalize(context, result, state);
}

// uniq_merge_state

extern "C" void uniq_merge_state_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    uniq_init(context, result);
}

extern "C" void uniq_merge_state_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_state_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_state_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    *result = *state;
}
