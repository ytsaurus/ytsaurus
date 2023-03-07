#include "yt_udf.h"
#include <string.h>

static int string_less_than(
    TUnversionedValue* string1,
    TUnversionedValue* string2)
{
    int length1IsLess = string1->Length < string2->Length;
    int min_length = length1IsLess ? string1->Length : string2->Length;

    int cmp_result = memcmp(
        string1->Data.String,
        string2->Data.String,
        min_length);

    return (cmp_result < 0) || (cmp_result == 0 && length1IsLess);
}

void max_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;
    result->Type = Null;
}

static void max_iteration(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;
    if (newValue->Type == Null) {
        result->Type = state->Type;
        result->Length = state->Length;
        result->Data = state->Data;
    } else if (state->Type == Null
        || (newValue->Type == Int64 && state->Data.Int64 < newValue->Data.Int64)
        || (newValue->Type == Uint64 && state->Data.Uint64 < newValue->Data.Uint64)
        || (newValue->Type == Double && state->Data.Double < newValue->Data.Double)
        || (newValue->Type == String && string_less_than(state, newValue)))
    {
        result->Type = newValue->Type;
        result->Length = newValue->Length;
        if (newValue->Type == String) {
            char* permanentData = AllocateBytes(context, newValue->Length);
            memcpy(permanentData, newValue->Data.String, newValue->Length);
            result->Data.String = permanentData;
        } else {
            result->Data = newValue->Data;
        }
    } else {
        result->Type = state->Type;
        result->Length = state->Length;
        result->Data = state->Data;
    }
}

void max_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    max_iteration(context, result, state, newValue);
}

void max_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    max_iteration(context, result, dstState, state);
}

void max_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;
    result->Type = state->Type;
    result->Length = state->Length;
    result->Data = state->Data;
}
