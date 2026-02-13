#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" uint64_t HyperLogLogGetFingerprint(TUnversionedValue* value);

#define DEFINE_HLL(PRECISION) \
    extern "C" void HyperLogLogAllocate ## PRECISION(TExpressionContext* context, TUnversionedValue* result); \
    extern "C" void HyperLogLogAdd ## PRECISION(void* hll1, uint64_t val); \
    extern "C" void HyperLogLogMerge ## PRECISION(void* hll1, void* hll2); \
    extern "C" void HyperLogLogMergeWithValidation ## PRECISION(void* hll1, void* hll2, uint64_t incomingStateLength); \
    extern "C" uint64_t HyperLogLogEstimateCardinality ## PRECISION(void* hll1); \
\
    extern "C" void hll_ ## PRECISION ## _init( \
        TExpressionContext* context, \
        TUnversionedValue* result) \
    { \
        HyperLogLogAllocate ## PRECISION(context, result); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _update( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state, \
        TUnversionedValue* newValue) \
    { \
        result->Type = EValueType::String; \
        result->Length = state->Length; \
        result->Data.String = state->Data.String; \
\
        HyperLogLogAdd ## PRECISION(result->Data.String, HyperLogLogGetFingerprint(newValue)); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state1, \
        TUnversionedValue* state2) \
    { \
        result->Type = EValueType::String; \
        result->Length = state1->Length; \
        result->Data.String = state1->Data.String; \
\
        HyperLogLogMerge ## PRECISION(state1->Data.String, state2->Data.String); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _finalize( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state) \
    { \
        result->Type = EValueType::Uint64; \
        result->Data.Uint64 = HyperLogLogEstimateCardinality ## PRECISION(state->Data.String); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _state_init( \
        TExpressionContext* context, \
        TUnversionedValue* result) \
    { \
        HyperLogLogAllocate ## PRECISION(context, result); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _state_update( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state, \
        TUnversionedValue* newValue) \
    { \
        result->Type = EValueType::String; \
        result->Length = state->Length; \
        result->Data.String = state->Data.String; \
\
        HyperLogLogAdd ## PRECISION(result->Data.String, HyperLogLogGetFingerprint(newValue)); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _state_merge( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state1, \
        TUnversionedValue* state2) \
    { \
        result->Type = EValueType::String; \
        result->Length = state1->Length; \
        result->Data.String = state1->Data.String; \
\
        HyperLogLogMerge ## PRECISION(state1->Data.String, state2->Data.String); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _state_finalize( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state) \
    { \
        *result = *state; \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_init( \
        TExpressionContext* context, \
        TUnversionedValue* result) \
    { \
        HyperLogLogAllocate ## PRECISION(context, result); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_update( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state, \
        TUnversionedValue* newValue) \
    { \
        result->Type = EValueType::String; \
        result->Length = state->Length; \
        result->Data.String = state->Data.String; \
\
        HyperLogLogMergeWithValidation ## PRECISION(state->Data.String, newValue->Data.String, newValue->Length); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_merge( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state1, \
        TUnversionedValue* state2) \
    { \
        result->Type = EValueType::String; \
        result->Length = state1->Length; \
        result->Data.String = state1->Data.String; \
\
        HyperLogLogMerge ## PRECISION(state1->Data.String, state2->Data.String); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_finalize( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state) \
    { \
        result->Type = EValueType::Uint64; \
        result->Data.Uint64 = HyperLogLogEstimateCardinality ## PRECISION(state->Data.String); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_state_init( \
        TExpressionContext* context, \
        TUnversionedValue* result) \
    { \
        HyperLogLogAllocate ## PRECISION(context, result); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_state_update( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state, \
        TUnversionedValue* newValue) \
    { \
        result->Type = EValueType::String; \
        result->Length = state->Length; \
        result->Data.String = state->Data.String; \
\
        HyperLogLogMergeWithValidation ## PRECISION(state->Data.String, newValue->Data.String, newValue->Length); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_state_merge( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state1, \
        TUnversionedValue* state2) \
    { \
        result->Type = EValueType::String; \
        result->Length = state1->Length; \
        result->Data.String = state1->Data.String; \
\
        HyperLogLogMerge ## PRECISION(state1->Data.String, state2->Data.String); \
    } \
\
    extern "C" void hll_ ## PRECISION ## _merge_state_finalize( \
        TExpressionContext* /*context*/, \
        TUnversionedValue* result, \
        TUnversionedValue* state) \
    { \
        *result = *state; \
    }

DEFINE_HLL(7)
DEFINE_HLL(8)
DEFINE_HLL(9)
DEFINE_HLL(10)
DEFINE_HLL(11)
DEFINE_HLL(12)
DEFINE_HLL(13)
DEFINE_HLL(14)

// COMPAT(dtorilov): Remove after 25.4.
// COMPAT BEGIN {

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

extern "C" void cardinality_merge_state_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    HyperLogLogAllocate(context, result);
}

extern "C" void cardinality_merge_state_update(
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

extern "C" void cardinality_merge_state_merge(
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

extern "C" void cardinality_merge_state_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    *result = *state;
}

// } COMPAT END
