#include <yt/ytlib/query_client/udf/udf_c_abi.h>

#include <stdio.h>

void xor_aggregate_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;

    ClearValue(result);
    result->Type = VT_Int64;
    result->Aggregate = 1;
    result->Data.Int64 = 0;
}

void xor_aggregate_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;

    ClearValue(result);
    result->Type = VT_Int64;
    result->Aggregate = state->Aggregate ^ newValue->Aggregate;
    result->Data.Int64 = 0;
}

void xor_aggregate_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    (void)context;

    ClearValue(result);
    result->Type = VT_Int64;
    result->Aggregate = dstState->Aggregate ^ state->Aggregate;
    result->Data.Int64 = 0;
}

void xor_aggregate_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;

    ClearValue(result);
    result->Type = VT_Int64;
    result->Aggregate = state->Aggregate;
    result->Data.Int64 = 0;
}

