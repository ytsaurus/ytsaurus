#pragma once

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NQueryClient::NRoutines {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TInferrumKVCacheReplica, ui32);

void InferrumKVCacheReplicaSetMerge(
    TExpressionContext* context,
    NTableClient::TUnversionedValue* result,
    NTableClient::TUnversionedValue* state1,
    NTableClient::TUnversionedValue* state2);

void InferrumKVCacheReplicaSetFinalize(
    TExpressionContext* context,
    NTableClient::TUnversionedValue* result,
    NTableClient::TUnversionedValue* state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NRoutines
