#include "yt_udf.h"

void first_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;
    result->Type = Null;
}

static void first_iteration(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;
    if (state->Type == Null) {
        result->Type = newValue->Type;
        result->Length = newValue->Length;
        result->Data = newValue->Data;
    } else {
        result->Type = state->Type;
        result->Length = state->Length;
        result->Data = state->Data;
    }
}

void first_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    first_iteration(context, result, state, newValue);
}

void first_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    first_iteration(context, result, dstState, state);
}

void first_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;
    result->Type = state->Type;
    result->Length = state->Length;
    result->Data = state->Data;
}
