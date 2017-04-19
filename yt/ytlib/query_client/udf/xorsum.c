#include "yt_udf.h"

/*
uint64_t xorsum_init(TExecutionContext* context)
{
    return 0;
}

uint64_t xorsum_update(TExecutionContext* context, uint64_t state, uint64_t value)
{
    return state ^ value;
}

uint64_t xorsum_merge(TExecutionContext* context, uint64_t lhs, uint64_t rhs)
{
    return lhs ^ rhs;
}

uint64_t xorsum_finalize(TExecutionContext* context, uint64_t state)
{
    return state;
}
*/

void xorsum_init(
    TExecutionContext* context,
    TUnversionedValue* result)
{
    result->Type = Null;
}

static void xorsum_iteration(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    if (newValue->Type == Null) {
        result->Type = state->Type;
        result->Data = state->Data;
    } else if (state->Type == Null) {
        result->Type = newValue->Type;
        result->Data = newValue->Data;
    } else {
        result->Type = state->Type;
        result->Data.Uint64 = state->Data.Uint64 ^ newValue->Data.Uint64;
    }
}

void xorsum_update(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    xorsum_iteration(context, result, state, newValue);
}

void xorsum_merge(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    xorsum_iteration(context, result, dstState, state);
}

void xorsum_finalize(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = state->Type;
    result->Data = state->Data;
}

