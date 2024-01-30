#include <yt/yt/library/query/misc/udf_c_abi.h>

#include <yt/yt/client/table_client/unversioned_value.h>

extern "C" void StoredReplicaSetMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2);
extern "C" void StoredReplicaSetFinalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state);


extern "C" void _yt_stored_replica_set_init(
    TExpressionContext* /*context*/,
    TUnversionedValue* result)
{
    result->Type = VT_Null;
}

extern "C" void _yt_stored_replica_set_update(
    TExpressionContext* /*context*/,
    TUnversionedValue* /*result*/,
    TUnversionedValue* /*state*/,
    TUnversionedValue* /*delta*/)
{
    // Update is not used anyway.
    YT_ABORT();
}

extern "C" void _yt_stored_replica_set_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    StoredReplicaSetMerge(context, result, state1, state2);
}

extern "C" void _yt_stored_replica_set_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    StoredReplicaSetFinalize(context, result, state);
}

