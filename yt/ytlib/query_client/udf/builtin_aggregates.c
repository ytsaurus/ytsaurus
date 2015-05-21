#include <yt_udf.h>
#include <string.h>

void avg_init(
    TExecutionContext* context,
    TUnversionedValue* result)
{
    int stateSize = 2 * sizeof(int64_t);
    char* statePtr = AllocateBytes(context, stateSize);
    int64_t* intStatePtr = (int64_t*)statePtr;
    intStatePtr[0] = 0;
    intStatePtr[1] = 0;

    result->Length = stateSize;
    result->Type = String;
    result->Data.String = statePtr;
}

void avg_update(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    int64_t* intStatePtr = (int64_t*)state->Data.String;
    if (newValue->Type != Null) {
        intStatePtr[0] += 1;
        intStatePtr[1] += newValue->Data.Int64;
    }

    result->Length = 2 * sizeof(int64_t);
    result->Type = String;
    result->Data.String = (char*)intStatePtr;
}

void avg_merge(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    int64_t* dstStatePtr = (int64_t*)dstState->Data.String;
    int64_t* intStatePtr = (int64_t*)state->Data.String;

    dstStatePtr[0] += intStatePtr[0];
    dstStatePtr[1] += intStatePtr[1];

    result->Length = 2 * sizeof(int64_t);
    result->Type = String;
    result->Data.String = (char*)dstStatePtr;
}

void avg_finalize(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    int64_t* intStatePtr = (int64_t*)state->Data.String;
    if (intStatePtr[0] == 0) {
        result->Type = Null;
    } else {
        double resultData = (double)intStatePtr[1] / (double)intStatePtr[0];
        result->Type = Double;
        result->Data.Double = resultData;
    }
}

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
    TExecutionContext* context,
    TUnversionedValue* result)
{
    result->Type = Null;
}

static void max_iteration(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
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
            char* permanentData = AllocatePermanentBytes(context, newValue->Length);
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
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    max_iteration(context, result, state, newValue);
}

void max_merge(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    max_iteration(context, result, dstState, state);
}

void max_finalize(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = state->Type;
    result->Length = state->Length;
    result->Data = state->Data;
}

void min_init(
    TExecutionContext* context,
    TUnversionedValue* result)
{
    result->Type = Null;
}

static void min_iteration(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    if (newValue->Type == Null) {
        result->Type = state->Type;
        result->Length = state->Length;
        result->Data = state->Data;
    } else if (state->Type == Null
        || (newValue->Type == Int64 && state->Data.Int64 >= newValue->Data.Int64)
        || (newValue->Type == Uint64 && state->Data.Uint64 >= newValue->Data.Uint64)
        || (newValue->Type == Double && state->Data.Double >= newValue->Data.Double)
        || (newValue->Type == String && !string_less_than(state, newValue)))
    {
        result->Type = newValue->Type;
        result->Length = newValue->Length;
        if (newValue->Type == String) {
            char* permanentData = AllocatePermanentBytes(context, newValue->Length);
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

void min_update(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    min_iteration(context, result, state, newValue);
}

void min_merge(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    min_iteration(context, result, dstState, state);
}

void min_finalize(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = state->Type;
    result->Length = state->Length;
    result->Data = state->Data;
}

void sum_init(
    TExecutionContext* context,
    TUnversionedValue* result)
{
    result->Type = Null;
}

static void sum_iteration(
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
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    sum_iteration(context, result, state, newValue);
}

void sum_merge(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    sum_iteration(context, result, dstState, state);
}

void sum_finalize(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = state->Type;
    result->Data = state->Data;
}
