#include <yt/yt/library/query/misc/udf_c_abi.h>

void FirstInit(
    TExpressionContext* context,
    TUnversionedValue* result);

void FirstUpdate(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue);

void FirstMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state);

void FirstFinalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state);

void first_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    FirstInit(context, result);
}

void first_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    FirstUpdate(context, result, state, newValue);
}

void first_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    FirstMerge(context, result, dstState, state);
}

void first_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    FirstFinalize(context, result, state);
}
