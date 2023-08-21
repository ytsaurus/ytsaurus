#include <yt/yt/library/query/misc/udf_c_abi.h>
#include <string.h>

void first_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;

    result->Type = VT_Null;
}

static void first_iteration(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    if (state->Type == VT_Null) {
        result->Type = newValue->Type;
        if (newValue->Type == VT_String || newValue->Type == VT_Any) {
            char* permanentData = AllocateBytes(context, newValue->Length);
            memcpy(permanentData, newValue->Data.String, newValue->Length);
            result->Length = newValue->Length;
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
