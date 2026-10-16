#pragma once

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NQueryClient::NRoutines {

////////////////////////////////////////////////////////////////////////////////

// TODO(dtorilov): Rename to YTStored* and YTLastSeen*
void StoredReplicaSetMerge(
    TExpressionContext* context,
    NTableClient::TUnversionedValue* result,
    NTableClient::TUnversionedValue* state1,
    NTableClient::TUnversionedValue* state2);

void StoredReplicaSetFinalize(
    TExpressionContext* context,
    NTableClient::TUnversionedValue* result,
    NTableClient::TUnversionedValue* state);

void LastSeenReplicaSetMerge(
    TExpressionContext* context,
    NTableClient::TUnversionedValue* result,
    NTableClient::TUnversionedValue* state1,
    NTableClient::TUnversionedValue* state2);

void LastSeenReplicaSetFinalize(
    TExpressionContext* context,
    NTableClient::TUnversionedValue* result,
    NTableClient::TUnversionedValue* state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NRoutines
