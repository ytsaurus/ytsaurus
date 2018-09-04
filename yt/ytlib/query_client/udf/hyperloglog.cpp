#include <yt/client/table_client/unversioned_value.h>

#include "yt_udf_cpp.h"

static uint64_t Hash(TUnversionedValue* v)
{
    auto value = (NYT::NTableClient::TUnversionedValue*)v;
    return NYT::NTableClient::GetFarmFingerprint(*value);
}

extern "C" void HyperLogLogAllocate(TExpressionContext* context, TUnversionedValue* result);
extern "C" void HyperLogLogAdd(void* hll1, uint64_t val);
extern "C" void HyperLogLogMerge(void* hll1, void* hll2);
extern "C" ui64 HyperLogLogEstimateCardinality(void* hll1);

extern "C" void cardinality_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    HyperLogLogAllocate(context, result);
}

extern "C" void cardinality_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    result->Type = EValueType::String;
    result->Length = state->Length;
    result->Data.String = state->Data.String;

    HyperLogLogAdd(result->Data.String, Hash(newValue));
}

extern "C" void cardinality_merge(
    TExpressionContext* context,
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
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = EValueType::Uint64;
    result->Data.Uint64 = HyperLogLogEstimateCardinality(state->Data.String);
}
