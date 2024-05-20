#include <yt/yt/library/query/misc/udf_c_abi.h>

void DictSumIteration(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* delta);

void dict_sum_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;
    result->Type = VT_Null;
}

void dict_sum_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* delta)
{
    DictSumIteration(context, result, state, delta);
}

void dict_sum_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* delta)
{
    DictSumIteration(context, result, state, delta);
}

void dict_sum_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;
    result->Type = state->Type;
    if (result->Type != VT_Null) {
        result->Data = state->Data;
        result->Length = state->Length;
    }
}
