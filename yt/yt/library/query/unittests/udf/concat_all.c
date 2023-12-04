#include <yt/yt/library/query/misc/udf_c_abi.h>

void concat_all_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;
    result->Type = VT_String;
    result->Length = 0;
    result->Data.String = NULL;
}

void concat_all_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    result->Type = VT_String;
    result->Length = state->Length + newValue->Length;

    result->Data.String = AllocateBytes(context, result->Length);

    memcpy(result->Data.String, state->Data.String, state->Length);
    memcpy(result->Data.String + state->Length, newValue->Data.String, newValue->Length);
}

void concat_all_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    result->Type = VT_String;
    result->Length = dstState->Length + state->Length;

    result->Data.String = AllocateBytes(context, result->Length);

    memcpy(result->Data.String, dstState->Data.String, dstState->Length);
    memcpy(result->Data.String + dstState->Length, state->Data.String, state->Length);
}

void concat_all_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = VT_String;
    result->Length = state->Length;

    result->Data.String = AllocateBytes(context, result->Length);

    memcpy(result->Data.String, state->Data.String, state->Length);
}
