#include <yt/yt/library/query/misc/udf_c_abi.h>

#include <stdio.h>

void xor_aggregate_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;

    result->Type = VT_Int64;
    result->Flags = VF_Aggregate;
    result->Data.Int64 = 0;
}

void xor_aggregate_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;

    result->Type = VT_Int64;
    if ((state->Flags & VF_Aggregate) ^ (newValue->Flags & VF_Aggregate)) {
        result->Flags = VF_Aggregate;
    }
    result->Data.Int64 = 0;
}

void xor_aggregate_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    (void)context;

    result->Type = VT_Int64;
    if ((dstState->Flags & VF_Aggregate) ^ (state->Flags & VF_Aggregate)) {
        result->Flags = VF_Aggregate;
    }
    result->Data.Int64 = 0;
}

void xor_aggregate_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;

    result->Type = VT_Int64;
    result->Flags = state->Flags;
    result->Data.Int64 = 0;
}
