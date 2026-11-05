#include <yt/yt/library/query/misc/udf_c_abi.h>

#include <library/cpp/yt/assert/assert.h>

extern "C" void InferrumKVCacheReplicaSetMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2);
extern "C" void InferrumKVCacheReplicaSetFinalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state);


extern "C" void _inferrum_kv_cache_replica_set_init(
    TExpressionContext* /*context*/,
    TUnversionedValue* result)
{
    result->Type = VT_Null;
}

extern "C" void _inferrum_kv_cache_replica_set_update(
    TExpressionContext* /*context*/,
    TUnversionedValue* /*result*/,
    TUnversionedValue* /*state*/,
    TUnversionedValue* /*delta*/)
{
    // Update is not used anyway.
    YT_ABORT();
}

extern "C" void _inferrum_kv_cache_replica_set_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    InferrumKVCacheReplicaSetMerge(context, result, state1, state2);
}

extern "C" void _inferrum_kv_cache_replica_set_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    InferrumKVCacheReplicaSetFinalize(context, result, state);
}
