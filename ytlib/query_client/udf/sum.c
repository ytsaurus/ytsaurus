#include "yt_udf.h"

void sum_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;
    result->Type = Null;
}

static void sum_iteration(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;
    if (newValue->Type == Null) {
        result->Type = state->Type;
        result->Data = state->Data;
    } else if (state->Type == Null) {
        result->Type = newValue->Type;
        result->Data = newValue->Data;
    } else if (newValue->Type == Int64) {
        result->Type = state->Type;
        result->Data.Int64 = state->Data.Int64 + newValue->Data.Int64;
    } else if (newValue->Type == Uint64) {
        result->Type = state->Type;
        result->Data.Uint64 = state->Data.Uint64 + newValue->Data.Uint64;
    } else if (newValue->Type == Double) {
        result->Type = state->Type;
        result->Data.Double = state->Data.Double + newValue->Data.Double;
    }
}

void sum_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    sum_iteration(context, result, state, newValue);
}

void sum_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    sum_iteration(context, result, dstState, state);
}

void sum_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;
    result->Type = state->Type;
    result->Data = state->Data;
}
