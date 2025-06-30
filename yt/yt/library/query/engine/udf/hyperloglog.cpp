#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void HyperLogLogAllocate(TExpressionContext* context, TUnversionedValue* result);
extern "C" void HyperLogLogAdd(void* hll1, uint64_t val);
extern "C" void HyperLogLogMerge(void* hll1, void* hll2);
extern "C" void HyperLogLogMergeWithValidation(void* hll1, void* hll2, uint64_t incomingStateLength);
extern "C" uint64_t HyperLogLogEstimateCardinality(void* hll1);
extern "C" uint64_t HyperLogLogGetFingerprint(TUnversionedValue* value);

extern "C" void cardinality_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    HyperLogLogAllocate(context, result);
}

extern "C" void cardinality_update(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    result->Type = EValueType::String;
    result->Length = state->Length;
    result->Data.String = state->Data.String;

    HyperLogLogAdd(result->Data.String, HyperLogLogGetFingerprint(newValue));
}

extern "C" void cardinality_merge(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    result->Type = EValueType::String;
    result->Length = state1->Length;
    result->Data.String = state1->Data.String;

    HyperLogLogMerge(state1->Data.String, state2->Data.String);
}

extern "C" void cardinality_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = EValueType::Uint64;
    result->Data.Uint64 = HyperLogLogEstimateCardinality(state->Data.String);
}

extern "C" void cardinality_state_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    HyperLogLogAllocate(context, result);
}

extern "C" void cardinality_state_update(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    result->Type = EValueType::String;
    result->Length = state->Length;
    result->Data.String = state->Data.String;

    HyperLogLogAdd(result->Data.String, HyperLogLogGetFingerprint(newValue));
}

extern "C" void cardinality_state_merge(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    result->Type = EValueType::String;
    result->Length = state1->Length;
    result->Data.String = state1->Data.String;

    HyperLogLogMerge(state1->Data.String, state2->Data.String);
}

extern "C" void cardinality_state_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    *result = *state;
}

extern "C" void cardinality_merge_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    HyperLogLogAllocate(context, result);
}

extern "C" void cardinality_merge_update(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    result->Type = EValueType::String;
    result->Length = state->Length;
    result->Data.String = state->Data.String;

    HyperLogLogMergeWithValidation(state->Data.String, newValue->Data.String, newValue->Length);
}

extern "C" void cardinality_merge_merge(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    result->Type = EValueType::String;
    result->Length = state1->Length;
    result->Data.String = state1->Data.String;

    HyperLogLogMerge(state1->Data.String, state2->Data.String);
}

extern "C" void cardinality_merge_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = EValueType::Uint64;
    result->Data.Uint64 = HyperLogLogEstimateCardinality(state->Data.String);
}
